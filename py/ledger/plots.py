"""Turns benchmark CSVs into plots and a markdown summary. Everything is computed from the raw CSVs."""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Categorical slots 1-3 of the reference palette, assigned to entities in a fixed order.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
GRID = "#e4e3df"
SURFACE = "#fcfcfb"
STORE_LABEL = {"local": "Local FS", "s3": "MinIO (S3)"}

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_SECONDARY, "xtick.color": INK_SECONDARY,
    "ytick.color": INK_SECONDARY, "text.color": INK, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 10, "axes.titlesize": 11, "axes.titleweight": "bold", "legend.frameon": False,
    "lines.linewidth": 2, "lines.markersize": 6,
})


def read(results: Path, name: str) -> list[dict]:
    rows = []
    for path in sorted(results.glob(f"{name}_*.csv")):
        with path.open() as f:
            rows.extend(csv.DictReader(f))
    return rows


def pct(values: list[float], q: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, int(q * len(s)))]


def stores(rows: list[dict]) -> list[str]:
    return [s for s in STORE_LABEL if any(r["store"] == s for r in rows)]


def line(ax, xs, ys, color, label, direct_label=True):
    ax.plot(xs, ys, color=color, marker="o", markeredgecolor=SURFACE, markeredgewidth=2, label=label)
    if direct_label:
        ax.annotate(label, (xs[-1], ys[-1]), xytext=(6, 0), textcoords="offset points", va="center",
                    color=INK_SECONDARY, fontsize=9)


def commit_scaling(results: Path, plots: Path, md: list[str]) -> None:
    rows = read(results, "commit_scaling")
    if not rows:
        return
    fig, axes = plt.subplots(1, 3, figsize=(13, 3.8))
    md += ["## Commit scaling", "",
           "Each writer appends 50-event batches as its own producer, after 5 unmeasured warm-up commits. "
           "Retries = lost commit races per commit.", "",
           "| store | writers | commits/s | p50 ms | p99 ms | retries/commit |", "|---|---|---|---|---|---|"]
    for i, store in enumerate(stores(rows)):
        by_w = defaultdict(list)
        for r in rows:
            if r["store"] == store:
                by_w[int(r["writers"])].append(r)
        ws = sorted(by_w)
        tput, p50, p99 = [], [], []
        for w in ws:
            lat = [int(r["latency_us"]) / 1000 for r in by_w[w]]
            # Throughput over the measured window: first measured start to last measured finish.
            first = min(int(r["start_us"]) for r in by_w[w])
            last = max(int(r["start_us"]) + int(r["latency_us"]) for r in by_w[w])
            wall = (last - first) / 1e6
            retries = statistics.mean(int(r["attempts"]) - 1 for r in by_w[w])
            tput.append(len(lat) / wall)
            p50.append(pct(lat, 0.5))
            p99.append(pct(lat, 0.99))
            md.append(f"| {STORE_LABEL[store]} | {w} | {tput[-1]:.0f} | {p50[-1]:.1f} | {p99[-1]:.1f} | {retries:.2f} |")
        for ax, ys in zip(axes, (tput, p50, p99)):
            line(ax, ws, ys, SERIES[i], STORE_LABEL[store])
    for ax, title, ylabel in zip(axes, ("Commit throughput", "Commit latency p50", "Commit latency p99"),
                                 ("commits / s", "ms", "ms")):
        ax.set_title(title, loc="left")
        ax.set_xlabel("concurrent writers")
        ax.set_ylabel(ylabel)
        ax.set_xscale("log", base=2)
        ax.set_xticks(sorted({int(r["writers"]) for r in rows}), labels=sorted({r["writers"] for r in rows}, key=int))
        ax.set_ylim(bottom=0)
    axes[0].legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(plots / "commit_scaling.png", dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(5.5, 3.6))
    for i, store in enumerate(stores(rows)):
        by_w = defaultdict(list)
        for r in rows:
            if r["store"] == store:
                by_w[int(r["writers"])].append(int(r["attempts"]) - 1)
        ws = sorted(by_w)
        line(ax, ws, [statistics.mean(by_w[w]) for w in ws], SERIES[i], STORE_LABEL[store])
    ax.set_title("Lost commit races per commit", loc="left")
    ax.set_xlabel("concurrent writers")
    ax.set_ylabel("retries / commit")
    ax.set_xscale("log", base=2)
    ax.set_xticks(sorted({int(r["writers"]) for r in rows}), labels=sorted({r["writers"] for r in rows}, key=int))
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(plots / "conflict_rate.png", dpi=150)
    plt.close(fig)
    md.append("")


def compaction(results: Path, plots: Path, md: list[str]) -> None:
    writers = read(results, "compaction_writers")
    query = read(results, "compaction_query")
    if not writers or not query:
        return
    md += ["## Compaction", "", "| store | concurrent compactors | append p50 ms | append p99 ms | retries/commit |",
           "|---|---|---|---|---|"]
    for store in stores(writers):
        for c in ("0", "2"):
            lat = [int(r["latency_us"]) / 1000 for r in writers if r["store"] == store and r["compactors"] == c]
            retries = statistics.mean(int(r["attempts"]) - 1 for r in writers if r["store"] == store and r["compactors"] == c)
            md.append(f"| {STORE_LABEL[store]} | {c} | {pct(lat, 0.5):.1f} | {pct(lat, 0.99):.1f} | {retries:.2f} |")
    md += ["", "| store | phase | live files | billing query p50 ms |", "|---|---|---|---|"]
    fig, ax = plt.subplots(figsize=(5.5, 3.6))
    names = stores(query)
    width = 0.36
    for j, phase in enumerate(("before", "after")):
        vals = []
        for store in names:
            rs = [r for r in query if r["store"] == store and r["phase"] == phase]
            vals.append(statistics.median(float(r["query_ms"]) for r in rs))
            md.append(f"| {STORE_LABEL[store]} | {phase} compaction | {rs[0]['files']} | {vals[-1]:.1f} |")
        xs = [k + (j - 0.5) * (width + 0.02) for k in range(len(names))]
        bars = ax.bar(xs, vals, width, color=SERIES[j], label=f"{phase} compaction", edgecolor=SURFACE, linewidth=2)
        ax.bar_label(bars, fmt="%.0f ms", padding=3, color=INK_SECONDARY, fontsize=9)
    ax.set_xticks(range(len(names)), labels=[STORE_LABEL[s] for s in names])
    ax.set_title("Billing query over the same rows", loc="left")
    ax.set_ylabel("ms (median)")
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(plots / "compaction_query.png", dpi=150)
    plt.close(fig)
    md.append("")


def checkpoints(results: Path, plots: Path, md: list[str]) -> None:
    rows = read(results, "checkpoint_load")
    if not rows:
        return
    names = stores(rows)
    fig, axes = plt.subplots(1, len(names), figsize=(5.5 * len(names), 3.6), squeeze=False)
    md += ["## Checkpoints", "", "Time to load the latest snapshot (median).", "",
           "| store | checkpoint interval | log length | load ms |", "|---|---|---|---|"]
    for ax, store in zip(axes[0], names):
        for i, interval in enumerate(("0", "25", "100")):
            by_len = defaultdict(list)
            for r in rows:
                if r["store"] == store and r["interval"] == interval:
                    by_len[int(r["log_length"])].append(float(r["load_ms"]))
            if not by_len:
                continue
            ls = sorted(by_len)
            ys = [statistics.median(by_len[n]) for n in ls]
            label = "no checkpoints" if interval == "0" else f"every {interval}"
            # The two checkpointed lines nearly coincide, so only the outlier is labelled at its end.
            line(ax, ls, ys, SERIES[i], label, direct_label=interval == "0")
            for n, y in zip(ls, ys):
                md.append(f"| {STORE_LABEL[store]} | {label} | {n} | {y:.1f} |")
        ax.set_title(f"Snapshot load time - {STORE_LABEL[store]}", loc="left")
        ax.set_xlabel("commits in log")
        ax.set_ylabel("ms (median, log scale)")
        ax.set_yscale("log")
        ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(plots / "checkpoint_load.png", dpi=150)
    plt.close(fig)
    md.append("")


def recovery(results: Path, plots: Path, md: list[str]) -> None:
    rows = read(results, "recovery")
    if not rows:
        return
    md += ["## Crash recovery", "",
           "Time for a restarted writer to open the table, find its last committed batch and get the interrupted "
           "batch acknowledged (median). A crash after the data write leaves one orphaned data file; a crash after "
           "the commit write leaves none and the resubmission is acknowledged as a duplicate.", "",
           "| store | checkpoints | log length | crash point | recovery ms | duplicate | orphan files |",
           "|---|---|---|---|---|---|---|"]
    groups = defaultdict(list)
    for r in rows:
        groups[(r["store"], r["interval"], int(r["log_length"]), r["crash_point"])].append(r)
    for (store, interval, n, point), rs in sorted(groups.items()):
        ms = statistics.median(float(r["recovery_ms"]) for r in rs)
        dup = {r["duplicate"] for r in rs}
        orphans = {r["orphan_files"] for r in rs}
        label = "off" if interval == "0" else f"every {interval}"
        md.append(f"| {STORE_LABEL[store]} | {label} | {n} | {point} | {ms:.1f} | {'/'.join(sorted(dup))} | "
                  f"{'/'.join(sorted(orphans))} |")
    names = stores(rows)
    fig, axes = plt.subplots(1, len(names), figsize=(5.5 * len(names), 3.6), squeeze=False)
    for ax, store in zip(axes[0], names):
        cats = sorted({(int(r["log_length"]), r["interval"]) for r in rows if r["store"] == store})
        width = 0.36
        for j, point in enumerate(("AFTER_DATA_WRITE", "AFTER_COMMIT_WRITE")):
            vals = [statistics.median(float(r["recovery_ms"]) for r in groups[(store, iv, n, point)]) for n, iv in cats]
            xs = [k + (j - 0.5) * (width + 0.02) for k in range(len(cats))]
            label = "crash before commit" if j == 0 else "crash after commit"
            ax.bar(xs, vals, width, color=SERIES[j], label=label, edgecolor=SURFACE, linewidth=2)
        ax.set_xticks(range(len(cats)),
                      labels=[f"{n} commits\n{'no ckpt' if iv == '0' else 'ckpt/' + iv}" for n, iv in cats])
        ax.set_title(f"Writer recovery time - {STORE_LABEL[store]}", loc="left")
        ax.set_ylabel("ms (median)")
        ax.grid(axis="x", visible=False)
        ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(plots / "recovery.png", dpi=150)
    plt.close(fig)
    md.append("")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", type=Path, required=True)
    ap.add_argument("--plots", type=Path, required=True)
    a = ap.parse_args()
    a.plots.mkdir(parents=True, exist_ok=True)
    md = ["# Benchmark results", "", "Generated by `python -m ledger.plots` from the CSVs in this directory.", ""]
    for section in (commit_scaling, compaction, checkpoints, recovery):
        section(a.results, a.plots, md)
    (a.results / "summary.md").write_text("\n".join(md) + "\n")
    print(f"wrote plots to {a.plots} and {a.results / 'summary.md'}")


if __name__ == "__main__":
    main()
