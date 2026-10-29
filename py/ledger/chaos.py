"""Real-process chaos: several writer JVMs and a compactor JVM share one table directory while
this script SIGKILLs them at random and restarts them, until every writer reports DONE.

Killed processes recover purely from table state, so this exercises the same recovery path a
production restart would, including kills that land in the middle of a commit.
"""

from __future__ import annotations

import random
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from . import javacli


@dataclass
class ChaosResult:
    kills: int = 0
    restarts: int = 0
    histories: list[Path] = field(default_factory=list)


def run(table: Path, workload: Path, producers: list[str], work: Path, seed: int,
        kill_interval: tuple[float, float] = (0.3, 1.2), timeout_s: float = 300) -> ChaosResult:
    rng = random.Random(seed)
    result = ChaosResult()
    hist = {p: work / f"history-{p}.jsonl" for p in producers}
    result.histories = list(hist.values())

    def start_writer(p: str):
        return javacli.spawn("ingest", "--table", table, "--workload", workload, "--producer", p,
                             "--history", hist[p], "--checkpoint-interval", 5)

    def start_compactor():
        return javacli.spawn("compact", "--table", table, "--small-file-rows", 400, "--max-files", 6,
                             "--duration-seconds", timeout_s, stdout=subprocess.DEVNULL)

    writers = {p: start_writer(p) for p in producers}
    compactor = start_compactor()
    done: set[str] = set()
    deadline = time.monotonic() + timeout_s
    try:
        while len(done) < len(producers):
            if time.monotonic() > deadline:
                raise TimeoutError(f"writers still running: {set(producers) - done}")
            time.sleep(rng.uniform(*kill_interval))
            for p, proc in list(writers.items()):
                if p not in done and proc.poll() is not None:
                    output = proc.stdout.read()
                    if proc.returncode == 0 and f"DONE {p}" in output:
                        done.add(p)
                    elif proc.returncode != -signal.SIGKILL:
                        raise RuntimeError(f"writer {p} exited {proc.returncode}:\n{output}")
            live = [p for p in producers if p not in done and writers[p].poll() is None]
            victims = live + (["compactor"] if compactor.poll() is None else [])
            if not victims:
                continue
            victim = rng.choice(victims)
            proc = compactor if victim == "compactor" else writers[victim]
            proc.send_signal(signal.SIGKILL)
            proc.wait()
            result.kills += 1
            if victim == "compactor":
                compactor = start_compactor()
            else:
                writers[victim] = start_writer(victim)
            result.restarts += 1
    finally:
        for proc in [*writers.values(), compactor]:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
    return result
