#!/usr/bin/env python3
"""Regenerates the README charts.
  python3 scripts/plot.py docs/signal_run.txt
The argument is saved stdout of `itchfh --signal ...`; the imbalance chart is parsed from it.
The benchmark bars are the README table values (edit BENCH after re-measuring)."""
import re
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SURFACE, INK, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
BLUE, ORANGE = "#2a78d6", "#eb6834"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "text.color": INK,
                     "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED,
                     "ytick.color": MUTED, "svg.fonttype": "none"})

# (label, throughput M msg/s, p50 ns): midpoints of the ranges in the README table.
BENCH = [("Baseline\nstd::unordered_map + std::map", 1.5, 561),
         ("v1\nflat hash map + sorted arrays", 4.45, 285),
         ("v2\n+ lookahead prefetch, 16-byte slots", 5.3, 120)]


def bench():
    fig, axes = plt.subplots(1, 2, figsize=(9, 2.6), facecolor=SURFACE, sharey=True)
    labels = [b[0] for b in BENCH][::-1]
    for ax, col, title, fmt in ((axes[0], 1, "Throughput (million messages / s)", "{:.1f}M"),
                                (axes[1], 2, "Median latency per message (ns)", "{:.0f} ns")):
        vals = [b[col] for b in BENCH][::-1]
        bars = ax.barh(labels, vals, height=0.45, color=BLUE)
        ax.bar_label(bars, labels=[fmt.format(v) for v in vals], padding=4, color=INK)
        ax.set_title(title, loc="left", fontsize=10, color=INK)
        ax.set_facecolor(SURFACE)
        ax.set_xlim(0, max(vals) * 1.22)
        ax.xaxis.set_visible(False)
        ax.tick_params(length=0)
        for s in ax.spines.values():
            s.set_visible(False)
    fig.tight_layout()
    fig.savefig("docs/bench.svg", facecolor=SURFACE)


def signal(path):
    rows = [list(map(float, m.groups())) for m in re.finditer(
        r"^\s+(\d\.\d)-\d\.\d\s+([\d.]+)% \(\s*\d+\)\s+([\d.]+)% \(\s*\d+\)", open(path).read(), re.M)]
    if len(rows) != 10:
        sys.exit(f"expected 10 imbalance rows in {path}, found {len(rows)}")
    x = [r[0] + 0.05 for r in rows]
    fig, ax = plt.subplots(figsize=(9, 3.6), facecolor=SURFACE)
    ax.set_facecolor(SURFACE)
    ax.axhline(50, color=MUTED, lw=1, ls=(0, (3, 3)))
    ax.plot(x, [r[2] for r in rows], color=ORANGE, lw=2, marker="o", ms=5, label="measured just before the move (partly mechanical)")
    ax.plot(x, [r[1] for r in rows], color=BLUE, lw=2, marker="o", ms=5, label="measured when the quote was first posted")
    ax.annotate(f"{rows[-1][1]:.0f}%", (x[-1], rows[-1][1]), xytext=(8, -3), textcoords="offset points", color=INK)
    ax.annotate(f"{rows[0][1]:.0f}%", (x[0], rows[0][1]), xytext=(-30, -3), textcoords="offset points", color=INK)
    ax.set_xlim(-0.06, 1.06)
    ax.set_ylim(0, 100)
    ax.set_xticks([i / 10 for i in range(11)])
    ax.set_xlabel("Order-book imbalance: bid size / (bid size + ask size)")
    ax.set_title("Share of next mid-price moves that were up (%)", loc="left", fontsize=10, color=INK)
    ax.grid(axis="y", color=GRID, lw=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(length=0)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.legend(frameon=False, loc="upper left", labelcolor=MUTED)
    fig.tight_layout()
    fig.savefig("docs/imbalance.svg", facecolor=SURFACE)


if __name__ == "__main__":
    bench()
    if len(sys.argv) > 1:
        signal(sys.argv[1])
