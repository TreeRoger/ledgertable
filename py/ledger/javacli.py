"""Runs the LedgerTable CLI (the shaded jar) from Python."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
JAR = Path(os.environ.get("LEDGER_JAR", ROOT / "target" / "ledgertable.jar"))


def java() -> str:
    home = os.environ.get("JAVA_HOME")
    if home and (Path(home) / "bin" / "java").exists():
        return str(Path(home) / "bin" / "java")
    found = shutil.which("java")
    if not found:
        raise RuntimeError("java not found; set JAVA_HOME")
    return found


def command(*args: object) -> list[str]:
    return [java(), "-jar", str(JAR), *map(str, args)]


def run(*args: object, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(command(*args), check=check, capture_output=True, text=True)


def spawn(*args: object, stdout=subprocess.PIPE) -> subprocess.Popen:
    """Starts the CLI; pass a file or DEVNULL as stdout for long-running processes nobody reads from."""
    return subprocess.Popen(command(*args), stdout=stdout, stderr=subprocess.STDOUT, text=True)
