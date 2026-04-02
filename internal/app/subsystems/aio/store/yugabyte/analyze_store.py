#!/usr/bin/env python3
"""
Store-layer fault-tolerance result analyser for YugabyteDB thesis.

Reads all store_fault_*.csv files produced by store_fault_tolerance_test.go,
then:
  - Prints a structured text report to stdout
  - Saves 8 publication-quality charts as individual PNG files
  - Saves a single combined summary figure (3×3 grid)

The key insight unique to store-layer tests vs HTTP-level tests is the error
split:
  net_error   — connection refused / timeout (EXPECTED during fault injection)
  store_error — transaction failure after all retries (MUST BE ZERO — signals
                data integrity risk)

Usage:
    pip install pandas matplotlib seaborn
    python internal/app/subsystems/aio/store/yugabyte/analyze_store.py [results_dir] [--latest]

Output files (written to <results_dir>_<timestamp>/):
    store_throughput.png       — ops/s before / during / after per test
    store_latency.png          — p50/p95/p99 latency per test per phase
    store_availability.png     — availability % per test per phase
    store_errors.png           — net_err vs store_err (stacked) during failure
    store_error_ratio.png      — store_error fraction over all tests (must be 0)
    store_integrity.png        — db_rows expected vs actual per test
    store_recovery_ratio.png   — after/before throughput ratio per test
    store_latency_spike.png    — p99 spike (during − before) = leader-election overhead
    store_summary.png          — all 8 panels in one figure
"""

import sys
import os
import re
import glob
from datetime import datetime
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import numpy as np
import seaborn as sns

# ── style ─────────────────────────────────────────────────────────────────────

sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)

# ── constants ─────────────────────────────────────────────────────────────────

# Canonical phase ordering (before → during → after) across all test variants.
PHASE_ORDER = [
    "before-failure", "before-pause",
    "during-failure", "during-pause",
    "after-recovery", "after-unpause",
]
PHASE_LABELS = {
    "before-failure": "Before",
    "before-pause":   "Before",
    "during-failure": "During",
    "during-pause":   "During",
    "after-recovery": "After",
    "after-unpause":  "After",
}
PHASE_COLORS = {
    "Before": "#4caf50",   # green
    "During": "#f44336",   # red
    "After":  "#2196f3",   # blue
}

# Canonical test ordering for consistent x-axis across all charts.
TEST_ORDER = [
    "StoreFT_FollowerNodeKill",
    "StoreFT_PrimaryNodeKill",
    "StoreFT_NetworkPartition",
    "StoreFT_ReadWriteMix",
]
# Short labels for axes (space-constrained).
TEST_SHORT = {
    "StoreFT_FollowerNodeKill":  "Follower\nKill",
    "StoreFT_PrimaryNodeKill":   "Primary\nKill",
    "StoreFT_NetworkPartition":  "Network\nPartition",
    "StoreFT_ReadWriteMix":      "Read/Write\nMix",
}

# ── data loading ──────────────────────────────────────────────────────────────

def load(results_dir: str, latest_only: bool = False) -> pd.DataFrame:
    pattern = os.path.join(results_dir, "store_fault_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        sys.exit(f"No store_fault_*.csv files found in {results_dir!r}.\n"
                 f"Run tests first:\n"
                 f"  STORE_FAULT_TEST_ENABLED=true go test -v -timeout=30m "
                 f"./internal/app/subsystems/aio/store/yugabyte/ -run TestStoreFT")
    if latest_only:
        files = [files[-1]]
        print(f"Analysing latest file only: {os.path.basename(files[0])}")
    dfs = [pd.read_csv(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(files)} file(s), {len(df)} records total.\n")

    # Normalise phase → Before / During / After.
    df["phase_label"] = df["phase"].map(PHASE_LABELS).fillna(df["phase"])

    # Total ops (any outcome).
    df["total_ops_calc"] = df["store_ok"] + df["store_err"] + df["net_err"]

    # Availability: successful / total (0–100 %).
    total = df["total_ops_calc"].replace(0, float("nan"))
    df["availability_pct"] = df["store_ok"] / total * 100

    # Total errors.
    df["total_errors"] = df["store_err"] + df["net_err"]

    # Error fraction: store_error / total (ideally 0).
    df["store_err_pct"] = df["store_err"] / total * 100

    # Workers extracted from notes, e.g. "workers=16".
    def _workers(note):
        m = re.search(r"workers=(\d+)", str(note))
        return int(m.group(1)) if m else None
    df["workers"] = df["notes"].apply(_workers)

    return df


# ── text report ───────────────────────────────────────────────────────────────

def print_report(df: pd.DataFrame) -> None:
    SEP = "=" * 80
    TESTS = [t for t in TEST_ORDER if t in df["test_name"].values]

    # 1. Throughput
    print(SEP)
    print("1. THROUGHPUT (ops/s)")
    print(SEP)
    print(f"{'Test':<30} {'Phase':<10} {'ops/s':>8}  {'vs baseline':>12}")
    for test in TESTS:
        sub = df[df["test_name"] == test].copy()
        if sub.empty:
            continue
        baseline = sub[sub["phase_label"] == "Before"]["ops_per_sec"].mean()
        for _, row in sub.iterrows():
            ratio = (row["ops_per_sec"] / baseline * 100) if baseline else float("nan")
            label = row["phase_label"]
            marker = " ←" if label == "During" else ""
            print(f"  {test:<28} {label:<10} {row['ops_per_sec']:>8.1f}  {ratio:>10.1f}%{marker}")
        print()

    # 2. Latency percentiles
    print(SEP)
    print("2. LATENCY PERCENTILES (ms)")
    print(SEP)
    print(f"{'Test':<30} {'Phase':<10} {'p50':>7} {'p95':>7} {'p99':>7} {'max':>8}")
    for test in TESTS:
        sub = df[df["test_name"] == test]
        if sub.empty:
            continue
        for _, row in sub.iterrows():
            print(f"  {test:<28} {row['phase_label']:<10} "
                  f"{row['p50_ms']:>7.1f} {row['p95_ms']:>7.1f} "
                  f"{row['p99_ms']:>7.1f} {row['max_ms']:>8.1f}")
        print()

    # 3. Availability
    print(SEP)
    print("3. AVAILABILITY DURING / AFTER FAILURE (%)")
    print(SEP)
    print(f"{'Test':<30} {'Avail during':>14} {'Avail after':>13}")
    for test in TESTS:
        sub = df[df["test_name"] == test]
        if sub.empty:
            continue
        during = sub[sub["phase_label"] == "During"]["availability_pct"].mean()
        after  = sub[sub["phase_label"] == "After"]["availability_pct"].mean()
        print(f"  {test:<30} {during:>13.2f}%  {after:>12.2f}%")
    print()

    # 4. Error breakdown — the thesis-critical section
    print(SEP)
    print("4. ERROR BREAKDOWN — during-failure phase")
    print("   store_error MUST BE 0 (transaction failure = potential data loss)")
    print("   net_error   is EXPECTED (connection refused = node is unreachable)")
    print(SEP)
    print(f"{'Test':<30} {'store_err':>12} {'net_err':>10} {'total_err':>12}")
    any_store_err = False
    for test in TESTS:
        sub = df[(df["test_name"] == test) & (df["phase_label"] == "During")]
        if sub.empty:
            continue
        for _, row in sub.iterrows():
            se = row["store_err"]
            ne = row["net_err"]
            flag = "  ← !! UNEXPECTED !!" if se > 0 else ""
            if se > 0:
                any_store_err = True
            print(f"  {test:<28} {se:>12.0f} {ne:>10.0f} {se+ne:>12.0f}{flag}")
    print()
    if any_store_err:
        print("  ⚠ WARNING: store_error > 0 — possible data integrity issue!")
    else:
        print("  ✓ store_error = 0 across all tests — data integrity preserved.")
    print()

    # 5. Data integrity
    print(SEP)
    print("5. DATA INTEGRITY — db_rows_expected vs db_rows_actual")
    print(SEP)
    print(f"  {'Test':<30} {'Expected':>10} {'Actual':>10} {'Diff':>8}")
    for test in TESTS:
        sub = df[df["test_name"] == test]
        if sub.empty:
            continue
        row = sub.iloc[-1]  # last phase has cumulative count
        exp = row.get("db_rows_expected", 0)
        act = row.get("db_rows_actual", 0)
        diff = exp - act
        ok = "✓ No loss" if diff == 0 else f"✗ LOST {diff:.0f} rows"
        print(f"  {test:<30} {exp:>10.0f} {act:>10.0f} {diff:>8.0f}  {ok}")
    print()

    # 6. Throughput recovery ratio
    print(SEP)
    print("6. THROUGHPUT RECOVERY RATIO (after / before)")
    print("   Target: ≥65% within 30 s (tablet rebalancing still in progress)")
    print(SEP)
    for test in TESTS:
        sub = df[df["test_name"] == test]
        if sub.empty:
            continue
        before = sub[sub["phase_label"] == "Before"]["ops_per_sec"].mean()
        after  = sub[sub["phase_label"] == "After"]["ops_per_sec"].mean()
        ratio  = after / before * 100 if before else float("nan")
        flag   = " ✓" if ratio >= 65 else " ✗ below 65% threshold"
        print(f"  {test:<30}  {after:>8.1f} / {before:>8.1f} ops/s = {ratio:>6.1f}%{flag}")
    print()

    # 7. Leader election overhead (latency spike)
    print(SEP)
    print("7. LEADER ELECTION OVERHEAD — p99 latency spike (during − before)")
    print(SEP)
    for test in TESTS:
        sub = df[df["test_name"] == test]
        if sub.empty:
            continue
        before_p99 = sub[sub["phase_label"] == "Before"]["p99_ms"].mean()
        during_p99 = sub[sub["phase_label"] == "During"]["p99_ms"].mean()
        spike_ms   = during_p99 - before_p99
        print(f"  {test:<30}  before_p99={before_p99:>7.1f}ms  "
              f"during_p99={during_p99:>7.1f}ms  spike=+{spike_ms:>7.1f}ms")
    print()
    print(SEP)


# ── chart helpers ─────────────────────────────────────────────────────────────

def _tests_present(df: pd.DataFrame) -> list[str]:
    return [t for t in TEST_ORDER if t in df["test_name"].values]


def _short(test: str) -> str:
    return TEST_SHORT.get(test, test)


def _phase_bar(ax, tests, df, y_col, ylabel, title, fmt="{:.0f}", ylim_pad=1.2):
    """Grouped bars (Before / During / After) for each test."""
    x = np.arange(len(tests))
    width = 0.25
    max_val = 0
    for i, label in enumerate(["Before", "During", "After"]):
        vals = []
        for test in tests:
            sub = df[(df["test_name"] == test) & (df["phase_label"] == label)]
            vals.append(float(sub[y_col].mean()) if not sub.empty else 0.0)
        max_val = max(max_val, max(vals or [0]))
        bars = ax.bar(x + i * width, vals, width,
                      label=label, color=PHASE_COLORS[label], alpha=0.85)
        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + max_val * 0.01,
                        fmt.format(v), ha="center", va="bottom", fontsize=7.5)
    ax.set_xticks(x + width)
    ax.set_xticklabels([_short(t) for t in tests], fontsize=9)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.set_title(title, fontsize=10, fontweight="bold")
    ax.legend(fontsize=8)
    if max_val > 0:
        ax.set_ylim(0, max_val * ylim_pad)


# ── individual charts ─────────────────────────────────────────────────────────

def chart_throughput(df: pd.DataFrame, out: str) -> None:
    tests = _tests_present(df)
    fig, ax = plt.subplots(figsize=(10, 5))
    _phase_bar(ax, tests, df, "ops_per_sec", "ops / second",
               "Store Throughput — Before / During / After Fault", fmt="{:.0f}")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_latency(df: pd.DataFrame, out: str) -> None:
    tests = _tests_present(df)
    n = len(tests)
    fig, axes = plt.subplots(1, n, figsize=(5 * n, 5), sharey=False)
    if n == 1:
        axes = [axes]
    for ax, test in zip(axes, tests):
        sub = df[df["test_name"] == test]
        pct_cols = ["p50_ms", "p95_ms", "p99_ms"]
        pct_labels = ["p50", "p95", "p99"]
        x = np.arange(len(pct_cols))
        width = 0.25
        for i, label in enumerate(["Before", "During", "After"]):
            row = sub[sub["phase_label"] == label]
            vals = [float(row[p].mean()) if not row.empty else 0.0 for p in pct_cols]
            ax.bar(x + i * width, vals, width,
                   label=label, color=PHASE_COLORS[label], alpha=0.85)
        ax.set_xticks(x + width)
        ax.set_xticklabels(pct_labels)
        ax.set_title(_short(test), fontsize=9)
        ax.set_ylabel("latency (ms)", fontsize=8)
        ax.legend(fontsize=7)
    fig.suptitle("Store Latency Percentiles (p50 / p95 / p99) — Before / During / After",
                 fontsize=11, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_availability(df: pd.DataFrame, out: str) -> None:
    tests = _tests_present(df)
    fig, ax = plt.subplots(figsize=(10, 5))
    _phase_bar(ax, tests, df, "availability_pct", "availability (%)",
               "Store Availability (% successful Execute() calls) — Before / During / After",
               fmt="{:.1f}", ylim_pad=1.25)
    ax.axhline(100, color="black",  linestyle="--", linewidth=0.8, label="100% baseline")
    ax.axhline(99,  color="green",  linestyle=":",  linewidth=0.8, label="99% target (RF=3)")
    ax.axhline(80,  color="orange", linestyle=":",  linewidth=0.8, label="80% warning")
    ax.set_ylim(0, 115)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_errors(df: pd.DataFrame, out: str) -> None:
    """Stacked bar: net_err (purple, expected) vs store_err (red, must be 0)."""
    tests = _tests_present(df)
    during = df[df["phase_label"] == "During"]

    rows = []
    for test in tests:
        sub = during[during["test_name"] == test]
        if sub.empty:
            continue
        rows.append({
            "test":       test,
            "net_err":    float(sub["net_err"].sum()),
            "store_err":  float(sub["store_err"].sum()),
        })
    if not rows:
        return

    edf = pd.DataFrame(rows).set_index("test")
    x = np.arange(len(edf))
    fig, ax = plt.subplots(figsize=(10, 5))

    bottom = np.zeros(len(edf))
    colors = {"net_err": "#9c27b0", "store_err": "#f44336"}
    labels = {"net_err": "net_error  (expected — node unreachable)",
              "store_err": "store_error (UNEXPECTED — transaction failure)"}
    for col in ["net_err", "store_err"]:
        vals = edf[col].values
        bars = ax.bar(x, vals, bottom=bottom, label=labels[col],
                      color=colors[col], alpha=0.85)
        # Annotate store_err bars with a warning if non-zero.
        if col == "store_err":
            for bar, v in zip(bars, vals):
                if v > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2,
                            bar.get_y() + bar.get_height() / 2,
                            f"⚠ {v:.0f}", ha="center", va="center",
                            color="white", fontsize=8, fontweight="bold")
        bottom = bottom + vals

    ax.set_xticks(x)
    ax.set_xticklabels([_short(t) for t in edf.index], fontsize=9)
    ax.set_ylabel("error count (during-failure phase)", fontsize=9)
    ax.set_title("Error Type Breakdown During Fault Injection\n"
                 "store_error = data integrity risk  |  net_error = expected connectivity loss",
                 fontsize=10, fontweight="bold")
    ax.legend(fontsize=8)

    # Horizontal zero-line emphasis for store_error.
    ax.axhline(0, color="black", linewidth=0.5)

    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_error_ratio(df: pd.DataFrame, out: str) -> None:
    """
    Donut charts per test showing success / net_error / store_error fraction.
    Unique to store-layer tests — reveals whether any unexpected failures occurred.
    """
    tests = _tests_present(df)
    n = len(tests)
    fig, axes = plt.subplots(1, n, figsize=(4.5 * n, 5))
    if n == 1:
        axes = [axes]

    for ax, test in zip(axes, tests):
        sub = df[df["test_name"] == test]
        ok  = float(sub["store_ok"].sum())
        se  = float(sub["store_err"].sum())
        ne  = float(sub["net_err"].sum())
        total = ok + se + ne
        if total == 0:
            ax.set_visible(False)
            continue

        sizes  = [ok, ne, se]
        colors = ["#4caf50", "#9c27b0", "#f44336"]
        explode = [0, 0, 0.12 if se > 0 else 0]
        labels = [
            f"success\n{ok/total*100:.1f}%",
            f"net_error\n{ne/total*100:.2f}%",
            f"store_error\n{se/total*100:.3f}%",
        ]

        wedges, texts = ax.pie(
            sizes, colors=colors, explode=explode,
            startangle=90, wedgeprops={"width": 0.55},   # donut
        )
        ax.set_title(_short(test), fontsize=9, fontweight="bold")
        ax.legend(wedges, labels, loc="lower center",
                  bbox_to_anchor=(0.5, -0.18), fontsize=7.5, frameon=False)

        # Center annotation: store_error verdict.
        verdict = "✓ 0 store\nerrors" if se == 0 else f"⚠ {se:.0f}\nstore\nerrors"
        color   = "#4caf50" if se == 0 else "#f44336"
        ax.text(0, 0, verdict, ha="center", va="center",
                fontsize=8, fontweight="bold", color=color)

    fig.suptitle("Operation Outcome Breakdown per Test\n"
                 "(store_error fraction must be 0 to prove data integrity)",
                 fontsize=11, fontweight="bold")
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: {out}")


def chart_integrity(df: pd.DataFrame, out: str) -> None:
    """DB rows expected (= successful writes) vs actual (= SELECT COUNT) per test."""
    tests = _tests_present(df)
    rows = []
    for test in tests:
        sub = df[df["test_name"] == test]
        if sub.empty:
            continue
        # Use the last row in the file (data integrity is recorded once per test).
        row = sub[sub["db_rows_expected"] > 0].iloc[-1] if not sub[sub["db_rows_expected"] > 0].empty else None
        if row is None:
            continue
        rows.append({"test": test,
                     "expected": float(row["db_rows_expected"]),
                     "actual":   float(row["db_rows_actual"])})
    if not rows:
        print("  (no db_rows data — skipping integrity chart)")
        return

    idf = pd.DataFrame(rows)
    x = np.arange(len(idf))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 5))
    bars_e = ax.bar(x - width / 2, idf["expected"], width,
                    label="Expected (successful writes)", color="#4caf50", alpha=0.85)
    bars_a = ax.bar(x + width / 2, idf["actual"],   width,
                    label="Actual (DB row count)",       color="#2196f3", alpha=0.85)

    for bar_e, bar_a, exp, act in zip(bars_e, bars_a, idf["expected"], idf["actual"]):
        diff = exp - act
        verdict = "✓ No loss" if diff == 0 else f"✗ −{diff:.0f}"
        color   = "#2e7d32" if diff == 0 else "#c62828"
        ypos    = max(exp, act) + max(idf["expected"].max(), idf["actual"].max()) * 0.04
        # Position label above the pair's midpoint
        mid_x = (bar_e.get_x() + bar_a.get_x() + bar_a.get_width()) / 2
        ax.text(mid_x, ypos, verdict, ha="center", va="bottom",
                fontsize=8.5, fontweight="bold", color=color)

    ax.set_xticks(x)
    ax.set_xticklabels([_short(t) for t in idf["test"]], fontsize=9)
    ax.set_ylabel("row count", fontsize=9)
    ax.set_title("Data Integrity — Successful Writes vs Actual DB Row Count",
                 fontsize=10, fontweight="bold")
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_recovery_ratio(df: pd.DataFrame, out: str) -> None:
    """After-recovery throughput / before-failure throughput per test."""
    tests = _tests_present(df)
    ratios, labels, colors = [], [], []
    for test in tests:
        sub = df[df["test_name"] == test]
        if sub.empty:
            continue
        before = sub[sub["phase_label"] == "Before"]["ops_per_sec"].mean()
        after  = sub[sub["phase_label"] == "After"]["ops_per_sec"].mean()
        ratio  = after / before if before else 0.0
        ratios.append(ratio * 100)
        labels.append(_short(test))
        colors.append("#4caf50" if ratio >= 0.65 else "#f44336")

    if not ratios:
        return

    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(ratios))
    bars = ax.bar(x, ratios, color=colors, alpha=0.87, width=0.55)
    for bar, v in zip(bars, ratios):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 1,
                f"{v:.1f}%", ha="center", va="bottom",
                fontsize=9, fontweight="bold")

    # Reference lines
    ax.axhline(100, color="black",  linestyle="--", linewidth=1.0, label="100% baseline")
    ax.axhline(65,  color="orange", linestyle=":",  linewidth=1.0,
               label="65% threshold (tablet rebalancing in progress)")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel("after-recovery / before-failure (%)", fontsize=9)
    ax.set_ylim(0, max(ratios or [100]) * 1.25)
    ax.set_title("Throughput Recovery Ratio — After vs Before Fault\n"
                 "YugaByte tablet leaders may still be rebalancing at 30 s",
                 fontsize=10, fontweight="bold")
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_latency_spike(df: pd.DataFrame, out: str) -> None:
    """
    p99 latency spike = p99_during − p99_before.
    This directly measures the leader-election overhead imposed on store clients.
    """
    tests = _tests_present(df)
    rows = []
    for test in tests:
        sub = df[df["test_name"] == test]
        before_p99 = sub[sub["phase_label"] == "Before"]["p99_ms"].mean()
        during_p99 = sub[sub["phase_label"] == "During"]["p99_ms"].mean()
        after_p99  = sub[sub["phase_label"] == "After"]["p99_ms"].mean()
        rows.append({
            "test": test,
            "before_p99": before_p99,
            "during_p99": during_p99,
            "after_p99":  after_p99,
            "spike_ms":   max(0.0, during_p99 - before_p99),
        })
    if not rows:
        return

    sdf = pd.DataFrame(rows)
    x = np.arange(len(sdf))
    width = 0.25

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))

    # Left panel: absolute p99 Before / During / After
    ax = axes[0]
    for i, (col, label) in enumerate([("before_p99", "Before"),
                                       ("during_p99", "During"),
                                       ("after_p99",  "After")]):
        vals = sdf[col].values
        bars = ax.bar(x + i * width, vals, width,
                      label=label, color=PHASE_COLORS[label], alpha=0.85)
        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + sdf["during_p99"].max() * 0.01,
                        f"{v:.0f}", ha="center", va="bottom", fontsize=7)
    ax.set_xticks(x + width)
    ax.set_xticklabels([_short(t) for t in sdf["test"]], fontsize=9)
    ax.set_ylabel("p99 latency (ms)", fontsize=9)
    ax.set_title("P99 Latency per Phase", fontsize=10, fontweight="bold")
    ax.legend(fontsize=8)

    # Right panel: spike only (during − before) = election overhead
    ax2 = axes[1]
    spike_colors = ["#e53935" if v > 100 else "#ff7043" if v > 20 else "#ffcc02"
                    for v in sdf["spike_ms"]]
    bars2 = ax2.bar(x, sdf["spike_ms"], color=spike_colors, alpha=0.87, width=0.55)
    for bar, v in zip(bars2, sdf["spike_ms"]):
        ax2.text(bar.get_x() + bar.get_width() / 2,
                 bar.get_height() + max(sdf["spike_ms"]) * 0.02,
                 f"+{v:.0f} ms", ha="center", va="bottom",
                 fontsize=8.5, fontweight="bold")
    ax2.set_xticks(x)
    ax2.set_xticklabels([_short(t) for t in sdf["test"]], fontsize=9)
    ax2.set_ylabel("p99 spike during fault (ms)", fontsize=9)
    ax2.set_title("Leader-Election Overhead\np99_during − p99_before",
                  fontsize=10, fontweight="bold")
    ax2.axhline(0, color="black", linewidth=0.5)

    # Color legend for spike severity
    patches = [
        mpatches.Patch(color="#e53935", label="> 100 ms  (significant)"),
        mpatches.Patch(color="#ff7043", label="20–100 ms (moderate)"),
        mpatches.Patch(color="#ffcc02", label="< 20 ms   (minimal)"),
    ]
    ax2.legend(handles=patches, fontsize=7.5)

    fig.suptitle("P99 Latency Spike = Leader-Election Overhead Visible to Store Clients",
                 fontsize=11, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_summary(df: pd.DataFrame, out: str) -> None:
    """3 × 3 grid combining all key panels into one thesis-ready figure."""
    tests  = _tests_present(df)
    labels = [_short(t) for t in tests]
    n      = len(tests)

    fig = plt.figure(figsize=(20, 17))
    gs  = fig.add_gridspec(3, 3, hspace=0.55, wspace=0.38)

    # ── Panel 1: Throughput ─────────────────────────────────────────
    ax1 = fig.add_subplot(gs[0, 0])
    _phase_bar(ax1, tests, df, "ops_per_sec", "ops/s", "Throughput")

    # ── Panel 2: P99 Latency ────────────────────────────────────────
    ax2 = fig.add_subplot(gs[0, 1])
    _phase_bar(ax2, tests, df, "p99_ms", "ms", "P99 Latency")

    # ── Panel 3: Availability ───────────────────────────────────────
    ax3 = fig.add_subplot(gs[0, 2])
    _phase_bar(ax3, tests, df, "availability_pct", "%", "Availability (%)", fmt="{:.1f}")
    ax3.axhline(99, color="green",  linestyle=":", linewidth=0.8, label="99%")
    ax3.axhline(80, color="orange", linestyle=":", linewidth=0.8, label="80%")
    ax3.set_ylim(0, 115)
    ax3.legend(fontsize=7)

    # ── Panel 4: Error breakdown ────────────────────────────────────
    ax4 = fig.add_subplot(gs[1, 0])
    during = df[df["phase_label"] == "During"]
    rows4 = []
    for test in tests:
        sub = during[during["test_name"] == test]
        if not sub.empty:
            rows4.append({"test": test,
                          "net_err":   float(sub["net_err"].sum()),
                          "store_err": float(sub["store_err"].sum())})
    if rows4:
        edf = pd.DataFrame(rows4).set_index("test")
        xi = np.arange(len(edf))
        bottom = np.zeros(len(edf))
        for col, color, label in [("net_err",   "#9c27b0", "net_error"),
                                   ("store_err", "#f44336", "store_error")]:
            vals = edf[col].values
            ax4.bar(xi, vals, bottom=bottom, color=color, alpha=0.85, label=label)
            bottom += vals
        ax4.set_xticks(xi)
        ax4.set_xticklabels([_short(t) for t in edf.index], fontsize=7.5)
        ax4.set_ylabel("errors (during failure)")
        ax4.set_title("Error Breakdown (during)")
        ax4.legend(fontsize=7.5)

    # ── Panel 5: Recovery ratio ─────────────────────────────────────
    ax5 = fig.add_subplot(gs[1, 1])
    ratios5, colors5 = [], []
    for test in tests:
        sub = df[df["test_name"] == test]
        before = sub[sub["phase_label"] == "Before"]["ops_per_sec"].mean()
        after  = sub[sub["phase_label"] == "After"]["ops_per_sec"].mean()
        ratio  = after / before * 100 if before else 0.0
        ratios5.append(ratio)
        colors5.append("#4caf50" if ratio >= 65 else "#f44336")
    x5 = np.arange(n)
    ax5.bar(x5, ratios5, color=colors5, alpha=0.85, width=0.55)
    ax5.axhline(65, color="orange", linestyle=":", linewidth=0.9)
    ax5.axhline(100, color="black", linestyle="--", linewidth=0.7)
    ax5.set_xticks(x5)
    ax5.set_xticklabels(labels, fontsize=7.5)
    ax5.set_ylabel("after / before (%)")
    ax5.set_title("Throughput Recovery %")
    for xi5, v in zip(x5, ratios5):
        ax5.text(xi5, v + 2, f"{v:.0f}%", ha="center", fontsize=7.5, fontweight="bold")

    # ── Panel 6: Latency spike ──────────────────────────────────────
    ax6 = fig.add_subplot(gs[1, 2])
    spikes = []
    for test in tests:
        sub = df[df["test_name"] == test]
        b = sub[sub["phase_label"] == "Before"]["p99_ms"].mean()
        d = sub[sub["phase_label"] == "During"]["p99_ms"].mean()
        spikes.append(max(0.0, d - b))
    x6 = np.arange(n)
    spike_colors6 = ["#e53935" if v > 100 else "#ff7043" if v > 20 else "#ffcc02"
                     for v in spikes]
    ax6.bar(x6, spikes, color=spike_colors6, alpha=0.87, width=0.55)
    ax6.set_xticks(x6)
    ax6.set_xticklabels(labels, fontsize=7.5)
    ax6.set_ylabel("ms")
    ax6.set_title("Leader Election\nOverhead (p99 spike)")
    for xi6, v in zip(x6, spikes):
        ax6.text(xi6, v + max(spikes or [1]) * 0.02,
                 f"+{v:.0f}ms", ha="center", fontsize=7.5)

    # ── Panel 7 (2,0): Data integrity ──────────────────────────────
    ax7 = fig.add_subplot(gs[2, 0])
    int_rows = []
    for test in tests:
        sub = df[(df["test_name"] == test) & (df["db_rows_expected"] > 0)]
        if not sub.empty:
            r = sub.iloc[-1]
            int_rows.append({"test": test, "exp": r["db_rows_expected"], "act": r["db_rows_actual"]})
    if int_rows:
        idf2 = pd.DataFrame(int_rows)
        xi = np.arange(len(idf2))
        w = 0.35
        ax7.bar(xi - w/2, idf2["exp"], w, label="expected", color="#4caf50", alpha=0.85)
        ax7.bar(xi + w/2, idf2["act"], w, label="actual",   color="#2196f3", alpha=0.85)
        for i, (e, a) in enumerate(zip(idf2["exp"], idf2["act"])):
            ax7.text(i, max(e, a) + max(idf2["exp"]) * 0.05,
                     "✓" if e == a else f"✗−{e-a:.0f}",
                     ha="center", fontsize=9,
                     color="green" if e == a else "red", fontweight="bold")
        ax7.set_xticks(xi)
        ax7.set_xticklabels([_short(t) for t in idf2["test"]], fontsize=7.5)
        ax7.set_ylabel("DB row count")
        ax7.set_title("Data Integrity\n(expected vs actual rows)")
        ax7.legend(fontsize=7.5)

    # ── Panel 8 (2,1): P50 latency comparison ──────────────────────
    ax8 = fig.add_subplot(gs[2, 1])
    _phase_bar(ax8, tests, df, "p50_ms", "ms", "P50 (Median) Latency")

    # ── Panel 9 (2,2): ops/s during only ───────────────────────────
    ax9 = fig.add_subplot(gs[2, 2])
    during_ops = []
    before_ops = []
    for test in tests:
        sub = df[df["test_name"] == test]
        during_ops.append(sub[sub["phase_label"] == "During"]["ops_per_sec"].mean() or 0)
        before_ops.append(sub[sub["phase_label"] == "Before"]["ops_per_sec"].mean() or 0)
    x9 = np.arange(n)
    ax9.bar(x9, during_ops, width=0.55, color="#f44336", alpha=0.85, label="During")
    ax9.step([-0.5] + list(x9) + [n - 0.5],
             [before_ops[0]] + before_ops + [before_ops[-1]],
             where="mid", color="#4caf50", linewidth=1.5, linestyle="--", label="Before (ref)")
    ax9.set_xticks(x9)
    ax9.set_xticklabels(labels, fontsize=7.5)
    ax9.set_ylabel("ops/s")
    ax9.set_title("Throughput During Fault\n(vs baseline reference)")
    ax9.legend(fontsize=7.5)

    fig.suptitle("YugabyteDB Store-Layer Fault Tolerance — Complete Analysis",
                 fontsize=14, fontweight="bold", y=1.01)
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: {out}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    latest_only = "--latest" in args
    args = [a for a in args if a != "--latest"]

    # Default results dir: sibling results/ next to this script.
    default_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    results_dir = args[0] if args else default_dir

    df = load(results_dir, latest_only=latest_only)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = results_dir.rstrip("/\\") + f"_analysis_{ts}"
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {out_dir}\n")

    print("── TEXT REPORT ──────────────────────────────────────────────────────────────\n")
    print_report(df)

    print("── CHARTS ───────────────────────────────────────────────────────────────────\n")
    chart_throughput    (df, os.path.join(out_dir, "store_throughput.png"))
    chart_latency       (df, os.path.join(out_dir, "store_latency.png"))
    chart_availability  (df, os.path.join(out_dir, "store_availability.png"))
    chart_errors        (df, os.path.join(out_dir, "store_errors.png"))
    chart_error_ratio   (df, os.path.join(out_dir, "store_error_ratio.png"))
    chart_integrity     (df, os.path.join(out_dir, "store_integrity.png"))
    chart_recovery_ratio(df, os.path.join(out_dir, "store_recovery_ratio.png"))
    chart_latency_spike (df, os.path.join(out_dir, "store_latency_spike.png"))
    chart_summary       (df, os.path.join(out_dir, "store_summary.png"))

    print(f"\nAll charts written to: {out_dir}/")


if __name__ == "__main__":
    main()