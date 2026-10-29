"""SIGKILL real writer and compactor processes at random while they share a table, then check."""

from ledger import chaos, workload
from ledger.checker import check
from ledger.tablelog import TableLog


def test_sigkill_writers_and_compactor(tmp_path):
    spec = workload.WorkloadSpec(seed=5, producers=4, batches=80, mean_batch=10)
    wl = tmp_path / "workload.jsonl"
    workload.write(workload.generate(spec), wl)
    table = tmp_path / "table"
    result = chaos.run(table, wl, [f"p{i}" for i in range(spec.producers)], tmp_path, seed=5, kill_interval=(0.2, 0.8))
    assert result.kills >= 10
    report = check(wl, result.histories, table)
    assert report.ok, report.violations[:20]
    # Every writer start records a recovery; more than one per producer means a killed writer was restarted
    # and resumed from table state. (Kills of the compactor, or of a JVM still starting up, record nothing.)
    assert report.stats["recoveries"] > spec.producers
    assert any(c.operation == "COMPACT" for c in TableLog.load(table).commits)
