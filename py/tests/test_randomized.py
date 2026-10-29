"""Randomized fault-injection runs, checked end to end.

Seeds come from LEDGER_SEEDS (default 1-12). A seed fixes the workload and each simulated
process's fault schedule; thread interleavings still vary between runs, so a failure is
replayed by rerunning its seed, possibly a few times. Each failure prints the exact commands.
"""

import json
import os
import re
import uuid
from collections import Counter

import pytest

from conftest import run_harness, seeds
from ledger import javacli, workload
from ledger.checker import check

SPEC = dict(producers=6, batches=20, mean_batch=15)


def replay_hint(seed: int, extra: tuple) -> str:
    return (f"replay: python -m ledger.workload --seed {seed} --producers {SPEC['producers']} "
            f"--batches {SPEC['batches']} --mean-batch {SPEC['mean_batch']} --out w.jsonl && "
            f"java -jar target/ledgertable.jar harness --table t --workload w.jsonl --history h.jsonl "
            f"--seed {seed} {' '.join(map(str, extra))}")


def compaction_outcomes(hist) -> str:
    """Says what the compactors did instead of compacting, for runs that never compacted."""
    outcomes = Counter()
    for line in hist.read_text().splitlines():
        e = json.loads(line)
        if e.get("op") == "compact" and e["type"] != "invoke":
            outcomes[e.get("status") or e["type"] + ": " + re.sub(r"[0-9a-f-]{8,}", "*", e.get("error", ""))] += 1
        elif e["type"] == "crash" and e["process"].startswith("compactor"):
            outcomes["crash: " + re.sub(r"[0-9a-f-]{8,}", "*", e["error"])] += 1
    return "compaction outcomes:\n" + "\n".join(f"  {n:4} {o}" for o, n in outcomes.most_common(15))


@pytest.mark.parametrize("seed", seeds())
def test_random_faults_local(seed, tmp_path):
    extra = ("--crash-rate", 0.05, "--error-rate", 0.05, "--compactors", 2, "--readers", 2,
             "--checkpoint-interval", 1 + seed % 7)
    wl, hist, table = run_harness(tmp_path, seed, workload.WorkloadSpec(seed=seed, **SPEC), *extra)
    report = check(wl, [hist], table)
    assert report.ok, "\n".join(report.violations[:20] + [replay_hint(seed, extra)])
    assert report.stats["history.crash"] > 0 and report.stats["compactions"] > 0, compaction_outcomes(hist)


@pytest.mark.skipif(not os.environ.get("LEDGER_S3_ENDPOINT"), reason="LEDGER_S3_ENDPOINT not set")
@pytest.mark.parametrize("seed", seeds()[:3])
def test_random_faults_s3(seed, tmp_path):
    prefix = f"randomized/{uuid.uuid4()}"
    spec = workload.WorkloadSpec(seed=seed, producers=4, batches=10, mean_batch=10)
    wl, hist = tmp_path / "w.jsonl", tmp_path / "h.jsonl"
    workload.write(workload.generate(spec), wl)
    result = javacli.run("harness", "--s3-prefix", prefix, "--workload", wl, "--history", hist, "--seed", seed,
                         "--crash-rate", 0.04, "--error-rate", 0.04, "--checkpoint-interval", 4, check=False)
    assert result.returncode in (0, 2), result.stderr
    javacli.run("export", "--s3-prefix", prefix, "--to", tmp_path / "table")
    report = check(wl, [hist], tmp_path / "table")
    assert report.ok, "\n".join(report.violations[:20])
