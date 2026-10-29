"""End-to-end demo: concurrent ingestion with late events, a writer killed in the middle of a
commit, another killed with an uncommitted data file, a compactor killed mid-commit while others
keep compacting, then exact billing reconciliation and time-travel queries.
"""

from __future__ import annotations

import argparse
import shutil
import signal
import subprocess
import time
from collections import Counter
from pathlib import Path

from . import javacli, workload
from .checker import check
from .tablelog import TableLog
from .workload import DAY_MS


def banner(text: str) -> None:
    print(f"\n\033[1m== {text}\033[0m")


def wait_for(proc: subprocess.Popen, marker: str, timeout_s: float = 60) -> str:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if marker in line:
            return line.strip()
        if not line and proc.poll() is not None:
            raise RuntimeError(f"process exited before printing {marker!r}")
    raise TimeoutError(marker)


def kill(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGKILL)
    proc.wait()


def cli(*args: object, limit: int = 20) -> str:
    out = javacli.run(*args).stdout
    lines = out.rstrip().splitlines()
    for line in lines[:limit]:
        print("    " + line)
    if len(lines) > limit:
        print(f"    ... {len(lines) - limit} more rows")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--work", type=Path, default=Path("work/demo"))
    a = ap.parse_args()
    shutil.rmtree(a.work, ignore_errors=True)
    a.work.mkdir(parents=True)
    table, wl = a.work / "table", a.work / "workload.jsonl"
    producers = [f"p{i}" for i in range(4)]

    banner("Generate 4 producers x 40 batches, ~15% of events up to a week late")
    batches = workload.generate(workload.WorkloadSpec(seed=2026, producers=4, batches=40, mean_batch=25, accounts=8))
    workload.write(batches, wl)
    print(f"    {len(batches)} batches, {sum(len(b['events']) for b in batches)} events")

    def writer(p: str, *extra: object) -> subprocess.Popen:
        return javacli.spawn("ingest", "--table", table, "--workload", wl, "--producer", p,
                             "--history", a.work / f"history-{p}.jsonl", "--checkpoint-interval", 10, *extra)

    banner("Start 4 writer processes and 2 compactor processes on one table")
    victim_commit = writer("p0", "--pause-at", "AFTER_COMMIT_WRITE", "--pause-occurrence", 12)
    victim_data = writer("p1", "--pause-at", "AFTER_DATA_WRITE", "--pause-occurrence", 8)
    others = {p: writer(p) for p in producers[2:]}
    compactor_log = (a.work / "compactor.log").open("w")
    compactor = javacli.spawn("compact", "--table", table, "--small-file-rows", 500, "--max-files", 8,
                              "--duration-seconds", 120, stdout=compactor_log)
    victim_compactor = javacli.spawn("compact", "--table", table, "--small-file-rows", 500, "--max-files", 8,
                                     "--duration-seconds", 120, "--pause-at", "BEFORE_COMMIT_WRITE")

    print("    " + wait_for(victim_commit, "PAUSED"))
    kill(victim_commit)
    log = TableLog.load(table)
    landed = log.producers_at_final.get("p0", (0, 0))
    print(f"    SIGKILL p0: its commit for batch p0/{landed[0]} is durable at v{landed[1]} but was never acknowledged")

    print("    " + wait_for(victim_data, "PAUSED"))
    kill(victim_data)
    print("    SIGKILL p1: it wrote a data file but died before committing it")

    print("    " + wait_for(victim_compactor, "PAUSED"))
    kill(victim_compactor)
    print("    SIGKILL a compactor after it rewrote files, before its commit; the other compactor keeps going")

    banner("Restart the killed writers; they resume from table state alone")
    restarted = {"p0": writer("p0"), "p1": writer("p1")}
    for p, proc in {**others, **restarted}.items():
        out, _ = proc.communicate(timeout=300)
        if proc.returncode != 0:
            raise RuntimeError(f"writer {p} failed:\n{out}")
    print("    all writers finished")
    compactor.send_signal(signal.SIGTERM)
    compactor.wait()
    compactor_log.close()

    log = TableLog.load(table)
    ops = Counter(c.operation for c in log.commits)
    live = log.files_at[log.latest]
    on_disk = {str(p.relative_to(table)) for p in (table / "data").glob("*.parquet")}
    print(f"    log: v{log.latest}, {ops['APPEND']} appends, {ops['COMPACT']} compactions, "
          f"{len(live)} live files, {len(on_disk - set(log.add_stats))} orphaned files from killed processes")

    banner("Billing from DuckDB vs. ground truth from the workload")
    cli("statement", "--table", table, limit=6)
    rows = cli("query", "--table", table,
               "SELECT account_id, SUM(compute_millis)::BIGINT AS compute_millis FROM usage GROUP BY 1 ORDER BY 1")
    billed = {parts[0]: int(parts[1]) for parts in (line.split() for line in rows.splitlines()[2:]) if len(parts) == 2}
    truth = Counter()
    for b in batches:
        for _, account, _, millis, _ in b["events"]:
            truth[account] += millis
    exact = billed == dict(truth)
    print(f"    {len(billed)} accounts, {sum(billed.values())} ms billed, {sum(truth.values())} ms produced "
          f"-> {'EXACT MATCH per account' if exact else 'MISMATCH'}")

    banner("Late events: invoice an early version, then correct it")
    invoiced = log.latest // 3
    covered_through = max(c.adds[0]["maxEventTime"] // DAY_MS
                          for c in log.commits[1:invoiced + 1] if c.operation == "APPEND")
    late = sum(1 for b in batches if log.batch_version[(b["producer"], b["seq"])] > invoiced
               for e in b["events"] if e[4] // DAY_MS <= covered_through)
    print(f"    v{invoiced} was invoiced; {late} events committed after it fall on days that invoice already billed")
    out = javacli.run("corrections", "--table", table, "--from", invoiced).stdout.splitlines()
    header, items = out[1], [line for line in out[2:] if line.strip()]
    earlier = [line for line in items
               if workload.days_since_epoch(line.split()[1]) <= covered_through]
    print(f"    {len(items)} correction line items; {len(earlier)} adjust days the v{invoiced} invoice already billed:")
    print("    " + header)
    for line in earlier[:8]:
        print("    " + line)

    banner("Time travel: the same billing SQL at that version and as of a wall-clock instant")
    cli("query", "--table", table, "--version", invoiced,
        "SELECT count(*) AS events, SUM(compute_millis)::BIGINT AS compute_millis FROM usage")
    mid = log.commits[log.latest // 2].timestamp_ms
    instant = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(mid / 1000)) + f".{mid % 1000:03d}Z"
    cli("query", "--table", table, "--as-of", instant,
        "SELECT count(*) AS events, SUM(compute_millis)::BIGINT AS compute_millis FROM usage")

    banner("Independent history check")
    report = check(wl, sorted(a.work.glob("history-*.jsonl")), table)
    for k in ("versions", "checkpoints", "final.rows", "acks.fresh", "acks.duplicate", "recoveries"):
        print(f"    {k:16} {report.stats[k]}")
    print("    " + ("OK: no lost or duplicated usage, totals exact" if report.ok else "FAILED"))
    for v in report.violations[:20]:
        print("    " + v)
    if not (report.ok and exact):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
