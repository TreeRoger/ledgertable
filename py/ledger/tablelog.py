"""An independent Python reader for LedgerTable's on-disk format.

The checker must not trust the Java code it is checking, so this module re-implements log
replay from the raw JSON commits and reads data files with pyarrow instead of the Java reader.
Every replay rule violation is collected rather than raised, so one run reports everything.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.parquet as pq

COMMIT_RE = re.compile(r"^(\d{20})\.json$")
CHECKPOINT_RE = re.compile(r"^(\d{20})\.checkpoint\.json$")


@dataclass(frozen=True)
class Commit:
    version: int
    txn_id: str
    timestamp_ms: int
    operation: str
    read_version: int
    adds: tuple[dict, ...]
    removes: tuple[str, ...]
    producer: tuple[str, int] | None


@dataclass
class TableLog:
    root: Path
    commits: list[Commit] = field(default_factory=list)
    files_at: list[frozenset[str]] = field(default_factory=list)
    """Live data file paths at each version, from an independent replay."""
    producers_at_final: dict[str, tuple[int, int]] = field(default_factory=dict)
    add_stats: dict[str, dict] = field(default_factory=dict)
    batch_version: dict[tuple[str, int], int] = field(default_factory=dict)
    """The version whose commit carried each (producer, seq) batch."""
    violations: list[str] = field(default_factory=list)
    _file_cache: dict[str, list[tuple]] = field(default_factory=dict)

    @property
    def latest(self) -> int:
        return len(self.commits) - 1

    @classmethod
    def load(cls, root: Path) -> "TableLog":
        log = cls(root=root)
        log_dir = root / "_log"
        versions = sorted(int(m.group(1)) for p in log_dir.iterdir() if (m := COMMIT_RE.match(p.name)))
        if versions != list(range(len(versions))):
            log.violations.append(f"log versions are not contiguous from 0: {versions[:5]}...{versions[-5:]}")
        for v in versions:
            raw = json.loads((log_dir / f"{v:020d}.json").read_text())
            producer = raw.get("producer")
            log.commits.append(Commit(
                version=raw["version"], txn_id=raw["txnId"], timestamp_ms=raw["timestampMillis"],
                operation=raw["operation"], read_version=raw["readVersion"], adds=tuple(raw["adds"]),
                removes=tuple(raw["removes"]),
                producer=(producer["producerId"], producer["seq"]) if producer else None))
        log._replay()
        return log

    def _replay(self) -> None:
        live: set[str] = set()
        producers: dict[str, tuple[int, int]] = {}
        prev_ts = None
        for i, c in enumerate(self.commits):
            where = f"commit {c.version}"
            if c.version != i:
                self.violations.append(f"{where}: stored at position {i}")
            if prev_ts is not None and c.timestamp_ms <= prev_ts:
                self.violations.append(f"{where}: timestamp {c.timestamp_ms} does not increase")
            prev_ts = c.timestamp_ms
            if (c.operation == "CREATE") != (c.version == 0):
                self.violations.append(f"{where}: CREATE must be exactly version 0")
            if not c.read_version < c.version:
                self.violations.append(f"{where}: readVersion {c.read_version} is not before it")
            for path in c.removes:
                if path not in live:
                    self.violations.append(f"{where}: removes file that is not live: {path}")
                live.discard(path)
            for add in c.adds:
                if add["path"] in live or add["path"] in self.add_stats:
                    self.violations.append(f"{where}: adds a file that was added before: {add['path']}")
                live.add(add["path"])
                self.add_stats[add["path"]] = add
            if c.producer:
                p, seq = c.producer
                expected = producers.get(p, (0, -1))[0] + 1
                if seq != expected:
                    self.violations.append(f"{where}: producer {p} seq {seq}, expected {expected}")
                if c.producer in self.batch_version:
                    self.violations.append(f"{where}: batch {p}/{seq} committed twice")
                self.batch_version[c.producer] = c.version
                producers[p] = (seq, c.version)
            self.files_at.append(frozenset(live))
        self.producers_at_final = producers

    def check_checkpoints(self) -> int:
        """Verifies every checkpoint against the replay; returns how many were checked."""
        checked = 0
        producers: dict[str, dict] = {}
        by_version: dict[int, dict] = {}
        for c in self.commits:
            if c.producer:
                producers[c.producer[0]] = {"seq": c.producer[1], "version": c.version}
            by_version[c.version] = {k: dict(v) for k, v in producers.items()}
        for p in sorted((self.root / "_log").iterdir()):
            m = CHECKPOINT_RE.match(p.name)
            if not m:
                continue
            v = int(m.group(1))
            checked += 1
            cp = json.loads(p.read_text())
            if v > self.latest:
                self.violations.append(f"checkpoint {v} is beyond the last commit {self.latest}")
                continue
            files = {f["path"]: f for f in cp["files"]}
            if cp["version"] != v or cp["timestampMillis"] != self.commits[v].timestamp_ms:
                self.violations.append(f"checkpoint {v}: header does not match commit {v}")
            if set(files) != self.files_at[v]:
                self.violations.append(f"checkpoint {v}: live files differ from replay")
            elif any(files[path] != self.add_stats[path] for path in files):
                self.violations.append(f"checkpoint {v}: file statistics differ from the adding commits")
            if cp["producers"] != by_version[v]:
                self.violations.append(f"checkpoint {v}: producer state differs from replay")
        return checked

    def read_file(self, path: str) -> list[tuple]:
        """Rows of one data file as (event_id, account_id, resource_id, compute_millis, event_time_ms)."""
        if path not in self._file_cache:
            t = pq.read_table(self.root / path)
            ids = t.column("event_id").to_pylist()
            accounts = t.column("account_id").to_pylist()
            resources = t.column("resource_id").to_pylist()
            millis = t.column("compute_millis").to_pylist()
            times = t.column("event_time").cast("int64").to_pylist()
            self._file_cache[path] = list(zip(ids, accounts, resources, millis, times))
        return self._file_cache[path]

    def rows_at(self, version: int) -> list[tuple]:
        rows: list[tuple] = []
        for path in sorted(self.files_at[version]):
            rows.extend(self.read_file(path))
        return rows

    def version_as_of(self, timestamp_ms: int) -> int:
        best = -1
        for c in self.commits:
            if c.timestamp_ms <= timestamp_ms:
                best = c.version
        return best
