"""The checker must accept a correct run and catch every kind of anomaly it claims to detect.

Each test takes a real faulty run, damages the table or the history the way a specific bug
would, and asserts the checker reports exactly that class of violation.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq

from ledger.checker import check
from ledger.tablelog import TableLog


def violations(run, **kw):
    wl, hist, table = run
    return check(wl, [hist], table, **kw).violations


def kinds(run, **kw):
    return {v.split("]")[0] + "]" for v in violations(run, **kw)}


def rewrite_history(hist, edit):
    entries = [json.loads(line) for line in hist.read_text().splitlines()]
    entries = [e for e in map(edit, entries) if e is not None]
    hist.write_text("".join(json.dumps(e) + "\n" for e in entries))


def append_commit(table, **fields):
    log = TableLog.load(table)
    last = log.commits[-1]
    v = log.latest + 1
    commit = {"version": v, "txnId": "tampered", "timestampMillis": last.timestamp_ms + 1, "operation": "APPEND",
              "readVersion": v - 1, "adds": [], "removes": []}
    commit.update(fields)
    (table / "_log" / f"{v:020d}.json").write_text(json.dumps(commit))


def test_correct_run_passes(clean_run):
    report = check(clean_run[0], [clean_run[1]], clean_run[2])
    assert report.ok, report.violations
    assert report.stats["history.crash"] > 0, "the run should have exercised crashes"
    assert report.stats["checkpoints"] > 0
    assert report.stats["compactions"] > 0


def test_detects_double_counted_batch(run_copy):
    wl, hist, table = run_copy
    log = TableLog.load(table)
    path = next(iter(log.files_at[log.latest]))
    stats = dict(log.add_stats[path])
    data = (table / path).read_bytes()
    stats["path"] = "data/duplicate.parquet"
    (table / stats["path"]).write_bytes(data)
    append_commit(table, adds=[stats], producer={"producerId": "p0", "seq": 99})
    found = kinds(run_copy)
    assert "[final]" in found and "[log]" in found and "[batches]" in found


def test_detects_lost_acknowledged_batch(run_copy):
    wl, hist, table = run_copy
    first = {}

    def drop_one_commit_from_ack(e):
        if e["type"] == "ok" and e["op"] == "append" and not e["duplicate"] and not first:
            first.update(e)
            e = dict(e, version=e["version"] + 1)
        return e

    rewrite_history(hist, drop_one_commit_from_ack)
    assert "[acks]" in kinds(run_copy)


def test_detects_compaction_that_drops_a_row(run_copy):
    wl, hist, table = run_copy
    log = TableLog.load(table)
    compaction = next(c for c in log.commits if c.operation == "COMPACT")
    path = table / compaction.adds[0]["path"]
    t = pq.read_table(path)
    pq.write_table(t.slice(1), path)
    found = kinds(run_copy)
    assert "[batches]" in found and "[files]" in found


def test_detects_missing_events_in_final_state(run_copy):
    wl, hist, table = run_copy
    with wl.open("a") as f:
        f.write(json.dumps({"producer": "ghost", "seq": 1, "events": [["deadbeef", "acct-000", "r", 5, 0]]}) + "\n")
    assert "[final]" in kinds(run_copy)
    assert "[final]" not in kinds(run_copy, require_complete=False)


def test_detects_stale_or_torn_reads(run_copy):
    wl, hist, table = run_copy

    def tear(e):
        if e["type"] == "read" and e["mode"] == "version" and len(e["events"]) > 2:
            e = dict(e, events=e["events"][1:])
        return e

    rewrite_history(hist, tear)
    assert any("partially visible" in v or "missing" in v for v in violations(run_copy))


def test_detects_stale_latest_read(run_copy):
    wl, hist, table = run_copy
    done = []

    def stale(e):
        if e["type"] == "read" and e["mode"] == "latest" and e["version"] > 1 and not done:
            done.append(e)
            return dict(e, version=0, events=[], totals={})
        return e

    rewrite_history(hist, stale)
    assert any("stale" in v for v in violations(run_copy))


def test_detects_wrong_duckdb_totals(run_copy):
    wl, hist, table = run_copy

    def inflate(e):
        if e["type"] == "read" and e["totals"]:
            account = next(iter(e["totals"]))
            e = dict(e, totals=dict(e["totals"], **{account: e["totals"][account] + 1}))
        return e

    rewrite_history(hist, inflate)
    assert any("DuckDB totals" in v for v in violations(run_copy))


def test_detects_wrong_billing_correction(run_copy):
    wl, hist, table = run_copy

    def skew(e):
        if e["type"] == "correction" and e["items"]:
            e = dict(e, items=e["items"][1:])
        return e

    rewrite_history(hist, skew)
    assert "[billing]" in kinds(run_copy)


def test_detects_corrupt_checkpoint(run_copy):
    wl, hist, table = run_copy
    cp_path = sorted((table / "_log").glob("*.checkpoint.json"))[-1]
    cp = json.loads(cp_path.read_text())
    cp["files"] = cp["files"][1:]
    cp_path.write_text(json.dumps(cp))
    assert any("checkpoint" in v for v in violations(run_copy))


def test_detects_log_gap(run_copy):
    wl, hist, table = run_copy
    log = TableLog.load(table)
    (table / "_log" / f"{log.latest + 2:020d}.json").write_text(
        (table / "_log" / f"{log.latest:020d}.json").read_text())
    assert "[log]" in kinds(run_copy)


def test_tolerates_truncated_final_history_line(run_copy):
    wl, hist, table = run_copy
    with hist.open("a") as f:
        f.write('{"type": "invoke", "op": "app')
    assert violations(run_copy) == []


def test_detects_phantom_rows(run_copy):
    wl, hist, table = run_copy
    phantom = pa.table({
        "event_id": ["phantom"], "account_id": ["acct-000"], "resource_id": ["r"],
        "compute_millis": pa.array([1], pa.int64()),
        "event_time": pa.array([0], pa.timestamp("ms", tz="UTC")),
    })
    pq.write_table(phantom, table / "data" / "phantom.parquet")
    size = (table / "data" / "phantom.parquet").stat().st_size
    append_commit(table, operation="COMPACT", adds=[{"path": "data/phantom.parquet", "rows": 1, "bytes": size,
                                                    "computeMillis": 1, "minEventTime": 0, "maxEventTime": 0}])
    found = kinds(run_copy)
    assert "[final]" in found and "[batches]" in found
