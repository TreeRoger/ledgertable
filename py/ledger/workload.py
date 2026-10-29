"""Generates usage-event workloads: skewed accounts, bursty batch sizes, and late events.

A workload is JSONL with one batch per line:
    {"producer": "p0", "seq": 1, "events": [[event_id, account_id, resource_id, compute_millis, event_time_ms], ...]}
Every event id is unique across the whole workload, so any duplicate the checker finds was
introduced by the table, not by the input.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import random
from dataclasses import dataclass
from pathlib import Path

DAY_MS = 86_400_000
START_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z


@dataclass(frozen=True)
class WorkloadSpec:
    seed: int
    producers: int = 6
    batches: int = 30
    mean_batch: int = 20
    accounts: int = 25
    late_fraction: float = 0.15
    max_lateness_days: int = 7
    days: int = 14


def generate(spec: WorkloadSpec) -> list[dict]:
    rng = random.Random(spec.seed)
    # Zipf-like skew: a handful of accounts produce most of the usage, as real tenants do.
    weights = [1.0 / (rank + 1) ** 1.1 for rank in range(spec.accounts)]
    accounts = [f"acct-{i:03d}" for i in range(spec.accounts)]
    used_ids: set[str] = set()
    batches = []
    for p in range(spec.producers):
        producer = f"p{p}"
        for seq in range(1, spec.batches + 1):
            # Event time advances with the batch; late events are stamped up to a week earlier.
            now_ms = START_MS + int((seq / spec.batches) * spec.days * DAY_MS) + rng.randrange(3_600_000)
            size = max(1, int(rng.expovariate(1 / spec.mean_batch)))
            events = []
            for _ in range(size):
                event_id = f"{rng.getrandbits(64):016x}"
                while event_id in used_ids:
                    event_id = f"{rng.getrandbits(64):016x}"
                used_ids.add(event_id)
                event_time = now_ms - rng.randrange(60_000)
                if rng.random() < spec.late_fraction:
                    event_time -= rng.randrange(1, spec.max_lateness_days + 1) * DAY_MS
                account = rng.choices(accounts, weights)[0]
                resource = f"{account}/cluster-{rng.randrange(4)}"
                compute_millis = rng.randrange(1, 3_600_000)
                events.append([event_id, account, resource, compute_millis, event_time])
            batches.append({"producer": producer, "seq": seq, "events": events})
    return batches


def days_since_epoch(iso_day: str) -> int:
    """UTC day number of a YYYY-MM-DD date, matching event_time // DAY_MS."""
    return (dt.date.fromisoformat(iso_day) - dt.date(1970, 1, 1)).days


def write(batches: list[dict], path: Path) -> None:
    with path.open("w") as f:
        for b in batches:
            f.write(json.dumps(b, separators=(",", ":")) + "\n")


def load(path: Path) -> list[dict]:
    with path.open() as f:
        return [json.loads(line) for line in f if line.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--producers", type=int, default=WorkloadSpec.producers)
    ap.add_argument("--batches", type=int, default=WorkloadSpec.batches)
    ap.add_argument("--mean-batch", type=int, default=WorkloadSpec.mean_batch)
    ap.add_argument("--accounts", type=int, default=WorkloadSpec.accounts)
    ap.add_argument("--late-fraction", type=float, default=WorkloadSpec.late_fraction)
    a = ap.parse_args()
    spec = WorkloadSpec(seed=a.seed, producers=a.producers, batches=a.batches, mean_batch=a.mean_batch,
                        accounts=a.accounts, late_fraction=a.late_fraction)
    batches = generate(spec)
    write(batches, a.out)
    print(f"wrote {len(batches)} batches, {sum(len(b['events']) for b in batches)} events to {a.out}")


if __name__ == "__main__":
    main()
