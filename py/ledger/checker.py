"""Jepsen-style checker for LedgerTable runs.

Inputs are the workload (ground truth), one or more client histories, and the table directory.
The checker trusts none of the Java code: it replays the log itself (tablelog.py), reads data
files with pyarrow, and judges every client observation against what the ground truth says
must be visible. It verifies:

  log            the log replays cleanly and every checkpoint equals the replay
  files          every data file's contents match the statistics its commit recorded
  batches        each append commit holds exactly its batch's events; compaction preserves rows exactly
  final          the final table holds every workload event exactly once, nothing else, with exact per-account totals
  acks           every acknowledged batch is committed at the version the client was told
  recovery       a restarted producer never sees fewer committed batches than it was already acknowledged
  reads          every snapshot read returns exactly the batches committed at or before its version, with
                 matching DuckDB totals; as-of reads pick the right version; latest reads are never older
                 than a commit acknowledged before the read began
  billing        invoices equal ground truth at their version, and invoice + corrections equals the final truth
"""

from __future__ import annotations

import argparse
import bisect
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from .tablelog import TableLog
from .workload import DAY_MS, load as load_workload

BatchKey = tuple[str, int]


@dataclass
class Report:
    violations: list[str] = field(default_factory=list)
    stats: Counter = field(default_factory=Counter)

    @property
    def ok(self) -> bool:
        return not self.violations

    def fail(self, check: str, message: str) -> None:
        self.violations.append(f"[{check}] {message}")


def read_history(paths: list[Path], report: Report) -> list[dict]:
    """Merges histories by timestamp. A truncated final line is what a killed process leaves, so it is skipped."""
    entries = []
    for path in paths:
        lines = path.read_text().splitlines()
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                if i != len(lines) - 1:
                    report.fail("history", f"{path.name}:{i + 1} is not valid JSON")
    entries.sort(key=lambda e: e["t"])
    return entries


class Truth:
    """Ground truth from the workload, and what it implies for each table version."""

    def __init__(self, batches: list[dict], log: TableLog):
        self.events: dict[str, tuple] = {}
        self.batch_of: dict[str, BatchKey] = {}
        self.batch_events: dict[BatchKey, list[tuple]] = {}
        for b in batches:
            key = (b["producer"], b["seq"])
            rows = [tuple(e) for e in b["events"]]
            self.batch_events[key] = rows
            for r in rows:
                self.events[r[0]] = r
                self.batch_of[r[0]] = key
        self.log = log
        # Batches visible at each version, and the exact totals they imply.
        added_at: dict[int, list[BatchKey]] = defaultdict(list)
        for key, v in log.batch_version.items():
            added_at[v].append(key)
        self.visible: list[frozenset[BatchKey]] = []
        self.account_totals: list[dict[str, int]] = []
        self.day_totals: list[dict[tuple[str, int], int]] = []
        visible: set[BatchKey] = set()
        accounts: Counter = Counter()
        days: Counter = Counter()
        for v in range(log.latest + 1):
            for key in added_at.get(v, []):
                visible.add(key)
                for _, account, _, millis, ts in self.batch_events.get(key, []):
                    accounts[account] += millis
                    days[(account, ts // DAY_MS)] += millis
            self.visible.append(frozenset(visible))
            self.account_totals.append({k: n for k, n in accounts.items() if n})
            self.day_totals.append({k: n for k, n in days.items() if n})


def check(workload: Path, histories: list[Path], table: Path, require_complete: bool = True) -> Report:
    report = Report()
    log = TableLog.load(table)
    report.stats["versions"] = log.latest + 1
    report.stats["checkpoints"] = log.check_checkpoints()
    for v in log.violations:
        report.fail("log", v)

    truth = Truth(load_workload(workload), log)
    history = read_history(histories, report)
    check_files(log, report)
    check_batches(log, truth, report)
    check_final(log, truth, report, require_complete)
    check_acks(log, history, report)
    check_reads(log, truth, history, report)
    check_billing(log, truth, history, report)
    for e in history:
        if e["type"] == "error":
            report.fail("client", f"client hit an impossible error: {e['error']}")
        report.stats[f"history.{e['type']}"] += 1
        if e["type"] == "ok" and e.get("op") == "compact" and e.get("status") == "COMPACTED":
            report.stats["compactions"] += 1
    return report


def check_files(log: TableLog, report: Report) -> None:
    for path, stats in log.add_stats.items():
        if not (log.root / path).exists():
            if any(path in files for files in log.files_at):
                report.fail("files", f"{path} is referenced by the log but missing")
            continue
        rows = log.read_file(path)
        actual = {
            "rows": len(rows),
            "computeMillis": sum(r[3] for r in rows),
            "minEventTime": min(r[4] for r in rows),
            "maxEventTime": max(r[4] for r in rows),
            "bytes": (log.root / path).stat().st_size,
        }
        for k, v in actual.items():
            if stats[k] != v:
                report.fail("files", f"{path}: {k} is {v} but its commit recorded {stats[k]}")


def check_batches(log: TableLog, truth: Truth, report: Report) -> None:
    for c in log.commits:
        if c.operation == "APPEND":
            if not c.producer or len(c.adds) != 1 or c.removes:
                report.fail("batches", f"append {c.version} must add one file for one batch")
                continue
            expected = truth.batch_events.get(c.producer)
            if expected is None:
                report.fail("batches", f"append {c.version} commits batch {c.producer} that is not in the workload")
                continue
            got = log.read_file(c.adds[0]["path"])
            if Counter(got) != Counter(expected):
                report.fail("batches", f"append {c.version}: file content differs from batch {c.producer}")
        elif c.operation == "COMPACT":
            before = Counter()
            for path in c.removes:
                before.update(log.read_file(path))
            after = Counter()
            for add in c.adds:
                after.update(log.read_file(add["path"]))
            if before != after:
                report.fail("batches", f"compaction {c.version} changed rows: "
                                       f"{sum((before - after).values())} lost, {sum((after - before).values())} added")
            if c.producer:
                report.fail("batches", f"compaction {c.version} carries a producer batch")


def check_final(log: TableLog, truth: Truth, report: Report, require_complete: bool) -> None:
    rows = log.rows_at(log.latest)
    ids = Counter(r[0] for r in rows)
    report.stats["final.rows"] = len(rows)
    dups = [i for i, n in ids.items() if n > 1]
    if dups:
        report.fail("final", f"{len(dups)} events appear more than once, e.g. {dups[:3]}")
    phantoms = [i for i in ids if i not in truth.events]
    if phantoms:
        report.fail("final", f"{len(phantoms)} events were never produced, e.g. {phantoms[:3]}")
    for r in rows:
        if r[0] in truth.events and truth.events[r[0]] != r:
            report.fail("final", f"event {r[0]} was altered: {r} != {truth.events[r[0]]}")
            break
    expected_ids = set(truth.events) if require_complete else {
        e[0] for key in truth.visible[log.latest] for e in truth.batch_events[key]}
    missing = expected_ids - set(ids)
    if missing:
        report.fail("final", f"{len(missing)} events are missing, e.g. {sorted(missing)[:3]}")
    totals: Counter = Counter()
    for r in rows:
        totals[r[1]] += r[3]
    expected: Counter = Counter()
    for i in expected_ids:
        expected[truth.events[i][1]] += truth.events[i][3]
    if dict(totals) != dict(expected):
        diff = {a: totals[a] - expected[a] for a in set(totals) | set(expected) if totals[a] != expected[a]}
        report.fail("final", f"per-account totals are off by {diff}")
    report.stats["final.compute_millis"] = sum(totals.values())


def check_acks(log: TableLog, history: list[dict], report: Report) -> None:
    acked: dict[str, int] = defaultdict(int)
    for e in history:
        if e["type"] == "ok" and e["op"] == "append":
            key = (e["producer"], e["seq"])
            report.stats["acks.duplicate" if e["duplicate"] else "acks.fresh"] += 1
            v = log.batch_version.get(key)
            if v is None:
                report.fail("acks", f"acknowledged batch {key} was never committed")
            elif e["duplicate"] and v > e["version"]:
                report.fail("acks", f"duplicate ack for {key} claims version {e['version']} but it committed at {v}")
            elif not e["duplicate"] and v != e["version"]:
                report.fail("acks", f"ack for {key} says version {e['version']} but it committed at {v}")
            acked[e["producer"]] = max(acked[e["producer"]], e["seq"])
        elif e["type"] == "recover":
            report.stats["recoveries"] += 1
            if e["seq"] < acked[e["producer"]]:
                report.fail("recovery", f"{e['process']} recovered seq {e['seq']} after seq "
                                        f"{acked[e['producer']]} was acknowledged")


def check_reads(log: TableLog, truth: Truth, history: list[dict], report: Report) -> None:
    ack_times: list[int] = []
    ack_floor: list[int] = []  # highest version acknowledged at or before ack_times[i]
    for h in history:
        if h["type"] == "ok" and h["op"] == "append":
            ack_times.append(h["t"])
            ack_floor.append(max(h["version"], ack_floor[-1] if ack_floor else -1))
    for e in history:
        if e["type"] != "read":
            continue
        report.stats[f"reads.{e['mode']}"] += 1
        v = e["version"]
        where = f"{e['process']} {e['mode']} read at v{v}"
        if v > log.latest:
            report.fail("reads", f"{where}: version does not exist")
            continue
        seen = Counter(e["events"])
        if any(n > 1 for n in seen.values()):
            report.fail("reads", f"{where}: returned duplicate events")
        per_batch = Counter(truth.batch_of.get(i, ("?", -1)) for i in seen)
        expected = truth.visible[v]
        if set(per_batch) != expected:
            extra = set(per_batch) - expected
            lost = expected - set(per_batch)
            report.fail("reads", f"{where}: {len(lost)} committed batches missing, {len(extra)} unexpected batches")
        elif any(per_batch[k] != len(truth.batch_events[k]) for k in expected):
            report.fail("reads", f"{where}: a batch is only partially visible")
        if e["totals"] != truth.account_totals[v]:
            report.fail("reads", f"{where}: DuckDB totals differ from ground truth")
        if e["mode"] == "as_of" and log.version_as_of(e["requested_ts"]) != v:
            report.fail("reads", f"{where}: as-of {e['requested_ts']} should be v{log.version_as_of(e['requested_ts'])}")
        if e["mode"] == "latest":
            i = bisect.bisect_left(ack_times, e["invoke_t"])
            floor = ack_floor[i - 1] if i else -1
            if v < floor:
                report.fail("reads", f"{where}: stale; v{floor} was acknowledged before the read began")


def check_billing(log: TableLog, truth: Truth, history: list[dict], report: Report) -> None:
    invoices = {}
    for e in history:
        if e["type"] == "invoice":
            report.stats["invoices"] += 1
            v = e["version"]
            items = {(a, d): n for a, d, n in e["items"]}
            invoices[v] = items
            if items != truth.day_totals[v]:
                report.fail("billing", f"invoice at v{v} differs from ground truth")
    final = truth.day_totals[log.latest]
    for e in history:
        if e["type"] != "correction":
            continue
        report.stats["corrections"] += 1
        report.stats["correction.line_items"] += len(e["items"])
        rebuilt = Counter(invoices.get(e["from"], {}))
        for a, d, n in e["items"]:
            rebuilt[(a, d)] += n
        if {k: n for k, n in rebuilt.items() if n} != final:
            report.fail("billing", f"invoice at v{e['from']} plus corrections does not equal the final truth")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workload", type=Path, required=True)
    ap.add_argument("--history", type=Path, nargs="+", required=True)
    ap.add_argument("--table", type=Path, required=True)
    ap.add_argument("--partial", action="store_true", help="do not require every workload batch to be committed")
    a = ap.parse_args()
    report = check(a.workload, a.history, a.table, require_complete=not a.partial)
    for k, v in sorted(report.stats.items()):
        print(f"  {k:28} {v}")
    if report.ok:
        print("OK: all checks passed")
        return
    print(f"FAILED: {len(report.violations)} violations")
    for v in report.violations[:50]:
        print("  " + v)
    sys.exit(1)


if __name__ == "__main__":
    main()
