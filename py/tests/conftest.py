import os
import shutil
from pathlib import Path

import pytest

from ledger import javacli, workload

SMALL = workload.WorkloadSpec(seed=11, producers=4, batches=12, mean_batch=10)


def run_harness(out: Path, seed: int, spec: workload.WorkloadSpec, *extra: object) -> tuple[Path, Path, Path]:
    out.mkdir(parents=True, exist_ok=True)
    wl, hist, table = out / "workload.jsonl", out / "history.jsonl", out / "table"
    workload.write(workload.generate(spec), wl)
    result = javacli.run("harness", "--table", table, "--workload", wl, "--history", hist, "--seed", seed, *extra,
                         check=False)
    assert result.returncode in (0, 2), result.stdout + result.stderr
    return wl, hist, table


@pytest.fixture(scope="session")
def clean_run(tmp_path_factory) -> tuple[Path, Path, Path]:
    """One faulty harness run shared by tests that tamper with copies of it."""
    if not javacli.JAR.exists():
        pytest.fail(f"{javacli.JAR} not built; run `make build`")
    return run_harness(tmp_path_factory.mktemp("clean"), 11, SMALL, "--crash-rate", "0.03", "--error-rate", "0.03",
                       "--checkpoint-interval", "4")


@pytest.fixture
def run_copy(clean_run, tmp_path) -> tuple[Path, Path, Path]:
    wl, hist, table = clean_run
    shutil.copy(wl, tmp_path / "workload.jsonl")
    shutil.copy(hist, tmp_path / "history.jsonl")
    shutil.copytree(table, tmp_path / "table")
    return tmp_path / "workload.jsonl", tmp_path / "history.jsonl", tmp_path / "table"


def seeds() -> list[int]:
    spec = os.environ.get("LEDGER_SEEDS", "1-12")
    lo, _, hi = spec.partition("-")
    return list(range(int(lo), int(hi or lo) + 1))
