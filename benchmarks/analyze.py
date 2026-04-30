#!/usr/bin/env python3
"""
Fault-tolerance result analyser for YugabyteDB / Resonate thesis.

Reads all fault_*.csv files from benchmarks/results/ (or the directory
passed as the first CLI argument), then:
  - Prints a structured text report to stdout
  - Saves 6 publication-quality charts as individual PNG files
  - Saves a single combined summary figure

Usage:
    pip install pandas matplotlib seaborn
    python benchmarks/analyze.py [results_dir]

Output files (written to results_dir):
    analysis_throughput.png
    analysis_latency.png
    analysis_availability.png
    analysis_errors.png
    analysis_recovery.png
    analysis_integrity.png
    analysis_summary.png        ← all six panels in one figure
"""

import sys
import os
import glob
from datetime import datetime

import pandas as pd
import matplotlib
from pathlib import Path
matplotlib.use("Agg")  # no display needed
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

# ── constants ─────────────────────────────────────────────────────────────────

PHASE_ORDER = ["before-failure", "before-pause", "during-failure", "during-pause", "after-recovery", "after-unpause"]
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

WRITE_TESTS = ["NodeKill", "PrimaryNodeKill", "NodePause"]
READ_TEST   = "ReadAvailability"
INTEGRITY_TEST  = "DataIntegrity"
CONCURRENT_TEST = "ConcurrentClaim"
RECOVERY_TEST   = "RecoveryLatency"

sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)

# ── data loading ──────────────────────────────────────────────────────────────

def load(results_dir: str, latest_only: bool = False) -> pd.DataFrame:
    pattern = os.path.join(results_dir, "fault_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No fault_*.csv files found in {results_dir}")
        sys.exit(1)
    if latest_only:
        files = [files[-1]]
        print(f"Analysing latest file only: {os.path.basename(files[0])}")
    dfs = [pd.read_csv(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(files)} file(s), {len(df)} records total.\n")

    # normalise phase names to Before / During / After
    df["phase_label"] = df["phase"].map(PHASE_LABELS).fillna(df["phase"])

    # availability = 2xx / (2xx + 4xx + 5xx + net_errors)
    total = df["http_2xx"] + df["http_4xx"] + df["http_5xx"] + df["net_errors"]
    df["availability_pct"] = (df["http_2xx"] / total.replace(0, float("nan"))) * 100

    # total errors
    df["total_errors"] = df["http_4xx"] + df["http_5xx"] + df["net_errors"]

    return df


# ── text report ───────────────────────────────────────────────────────────────

def print_report(df: pd.DataFrame) -> None:
    SEP = "=" * 80

    # ── 1. Throughput ─────────────────────────────────────────────────────────
    print(SEP)
    print("1. THROUGHPUT (ops/s) — write tests")
    print(SEP)
    print(f"{'Test':<22} {'Phase':<10} {'ops/s':>8}  {'vs baseline':>12}")
    for test in WRITE_TESTS + [READ_TEST]:
        sub = df[df["test_name"] == test].copy()
        if sub.empty:
            continue
        baseline = sub[sub["phase_label"] == "Before"]["ops_per_sec"].mean()
        for _, row in sub.iterrows():
            ratio = (row["ops_per_sec"] / baseline * 100) if baseline else float("nan")
            label = row["phase_label"]
            marker = " ←" if label == "During" else ""
            print(f"  {test:<20} {label:<10} {row['ops_per_sec']:>8.1f}  {ratio:>10.1f}%{marker}")
        print()

    # ── 2. Latency ────────────────────────────────────────────────────────────
    print(SEP)
    print("2. LATENCY PERCENTILES (ms) — write tests")
    print(SEP)
    print(f"{'Test':<22} {'Phase':<10} {'p50':>8} {'p95':>8} {'p99':>8} {'max':>8}")
    for test in WRITE_TESTS:
        sub = df[df["test_name"] == test]
        if sub.empty:
            continue
        for _, row in sub.iterrows():
            print(f"  {test:<20} {row['phase_label']:<10} "
                  f"{row['p50_ms']:>8.1f} {row['p95_ms']:>8.1f} {row['p99_ms']:>8.1f} {row['max_ms']:>8.1f}")
        print()

    # ── 3. Availability ───────────────────────────────────────────────────────
    print(SEP)
    print("3. AVAILABILITY DURING FAILURE (%)")
    print(SEP)
    print(f"{'Test':<22} {'Avail during':>14} {'Avail after':>12}")
    for test in WRITE_TESTS + [READ_TEST]:
        sub = df[df["test_name"] == test]
        if sub.empty:
            continue
        during = sub[sub["phase_label"] == "During"]["availability_pct"].mean()
        after  = sub[sub["phase_label"] == "After"]["availability_pct"].mean()
        print(f"  {test:<22} {during:>13.1f}%  {after:>11.1f}%")
    print()

    # ── 4. Error breakdown ────────────────────────────────────────────────────
    print(SEP)
    print("4. ERROR BREAKDOWN — during-failure phase")
    print(SEP)
    print(f"{'Test':<22} {'http_4xx':>10} {'http_5xx':>10} {'net_err':>10} {'total_err':>10}")
    for test in WRITE_TESTS + [READ_TEST]:
        sub = df[(df["test_name"] == test) & (df["phase_label"] == "During")]
        if sub.empty:
            continue
        for _, row in sub.iterrows():
            print(f"  {test:<22} {row['http_4xx']:>10.0f} {row['http_5xx']:>10.0f} "
                  f"{row['net_errors']:>10.0f} {row['total_errors']:>10.0f}")
    print()

    # ── 5. Recovery ───────────────────────────────────────────────────────────
    print(SEP)
    print("5. RECOVERY LATENCY")
    print(SEP)
    rec = df[df["test_name"] == RECOVERY_TEST]
    if not rec.empty:
        row = rec.iloc[0]
        print(f"  Time from docker start → first successful write: {row['recovery_ms']:.0f} ms")
        print(f"  Baseline p99 write latency:                      {row['p99_ms']:.1f} ms")
        ratio = row["recovery_ms"] / row["p99_ms"] if row["p99_ms"] else float("nan")
        print(f"  Recovery / baseline p99 ratio:                   {ratio:.0f}×")
    print()

    # ── 6. Data integrity ─────────────────────────────────────────────────────
    print(SEP)
    print("6. DATA INTEGRITY — db rows expected vs actual")
    print(SEP)
    integ = df[df["test_name"] == INTEGRITY_TEST]
    if not integ.empty:
        print(f"  {'Phase':<22} {'Expected':>10} {'Actual':>10} {'Loss':>8}")
        for _, row in integ.iterrows():
            exp = row["db_rows_expected"]
            act = row["db_rows_actual"]
            loss = exp - act if exp > 0 else 0
            ok = "✓" if loss == 0 else "✗ DATA LOSS"
            print(f"  {row['phase']:<22} {exp:>10.0f} {act:>10.0f} {loss:>7.0f}  {ok}")
    print()

    # ── 7. Idempotency ────────────────────────────────────────────────────────
    print(SEP)
    print("7. IDEMPOTENCY — ConcurrentClaim DB row count")
    print(SEP)
    cc = df[df["test_name"] == CONCURRENT_TEST]
    if not cc.empty:
        row = cc.iloc[0]
        exp = int(row["db_rows_expected"])
        act = int(row["db_rows_actual"])
        status = "PASS ✓" if act == exp == 1 else "FAIL ✗"
        print(f"  Expected rows: {exp}  Actual rows: {act}  →  {status}")
        print(f"  Notes: {row['notes']}")
    print()
    print(SEP)


# ── charts ────────────────────────────────────────────────────────────────────

def _phase_bar(ax, tests, df, y_col, ylabel, title, fmt="{:.0f}"):
    """Generic helper: grouped bars (Before/During/After) per test."""
    x = range(len(tests))
    width = 0.25
    for i, label in enumerate(["Before", "During", "After"]):
        vals = []
        for test in tests:
            sub = df[(df["test_name"] == test) & (df["phase_label"] == label)]
            vals.append(sub[y_col].mean() if not sub.empty else 0)
        bars = ax.bar([xi + i * width for xi in x], vals, width,
                      label=label, color=PHASE_COLORS[label], alpha=0.85)
        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01 * max(vals or [1]),
                        fmt.format(v), ha="center", va="bottom", fontsize=8)
    ax.set_xticks([xi + width for xi in x])
    ax.set_xticklabels(tests, rotation=15, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: fmt.format(v)))


def chart_throughput(df: pd.DataFrame, out: str) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    tests = [t for t in WRITE_TESTS + [READ_TEST] if t in df["test_name"].values]
    _phase_bar(ax, tests, df, "ops_per_sec", "ops / second", "Throughput by Phase and Failure Type")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_latency(df: pd.DataFrame, out: str) -> None:
    tests = [t for t in WRITE_TESTS if t in df["test_name"].values]
    fig, axes = plt.subplots(1, len(tests), figsize=(5 * len(tests), 5), sharey=False)
    if len(tests) == 1:
        axes = [axes]
    for ax, test in zip(axes, tests):
        sub = df[df["test_name"] == test].copy()
        pcts = ["p50_ms", "p95_ms", "p99_ms"]
        labels = ["p50", "p95", "p99"]
        x = range(len(pcts))
        width = 0.25
        for i, phase_label in enumerate(["Before", "During", "After"]):
            row = sub[sub["phase_label"] == phase_label]
            vals = [row[p].mean() if not row.empty else 0 for p in pcts]
            ax.bar([xi + i * width for xi in x], vals, width,
                   label=phase_label, color=PHASE_COLORS[phase_label], alpha=0.85)
        ax.set_xticks([xi + width for xi in x])
        ax.set_xticklabels(labels)
        ax.set_title(test)
        ax.set_ylabel("latency (ms)")
        ax.legend(fontsize=8)
    fig.suptitle("Latency Percentiles (p50 / p95 / p99) Before / During / After Failure", fontsize=12)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_availability(df: pd.DataFrame, out: str) -> None:
    tests = [t for t in WRITE_TESTS + [READ_TEST] if t in df["test_name"].values]
    fig, ax = plt.subplots(figsize=(9, 5))
    _phase_bar(ax, tests, df, "availability_pct", "availability (%)",
               "Request Availability (% 2xx) by Phase and Failure Type", fmt="{:.1f}%")
    ax.axhline(100, color="black", linestyle="--", linewidth=0.8, label="100% baseline")
    ax.axhline(80,  color="orange", linestyle=":",  linewidth=0.8, label="80% threshold")
    ax.set_ylim(0, 115)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_errors(df: pd.DataFrame, out: str) -> None:
    tests = [t for t in WRITE_TESTS + [READ_TEST] if t in df["test_name"].values]
    during = df[df["phase_label"] == "During"]

    rows = []
    for test in tests:
        sub = during[during["test_name"] == test]
        if sub.empty:
            continue
        rows.append({
            "test": test,
            "http_4xx": sub["http_4xx"].sum(),
            "http_5xx": sub["http_5xx"].sum(),
            "net_errors": sub["net_errors"].sum(),
        })
    if not rows:
        return
    edf = pd.DataFrame(rows).set_index("test")

    fig, ax = plt.subplots(figsize=(9, 5))
    bottom = [0] * len(edf)
    colors = {"http_4xx": "#ff9800", "http_5xx": "#f44336", "net_errors": "#9c27b0"}
    labels = {"http_4xx": "HTTP 4xx", "http_5xx": "HTTP 5xx", "net_errors": "Net/Timeout Error"}
    x = range(len(edf))
    for col in ["http_4xx", "http_5xx", "net_errors"]:
        vals = edf[col].tolist()
        ax.bar(x, vals, bottom=bottom, label=labels[col], color=colors[col], alpha=0.85)
        bottom = [b + v for b, v in zip(bottom, vals)]
    ax.set_xticks(list(x))
    ax.set_xticklabels(edf.index, rotation=15, ha="right")
    ax.set_ylabel("error count (during-failure phase)")
    ax.set_title("Error Type Breakdown During Failure")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_recovery(df: pd.DataFrame, out: str) -> None:
    rec = df[df["test_name"] == RECOVERY_TEST]
    if rec.empty:
        print("  (no RecoveryLatency data — skipping)")
        return
    row = rec.iloc[0]
    recovery_ms = row["recovery_ms"]
    baseline_p99 = row["p99_ms"]

    fig, ax = plt.subplots(figsize=(6, 5))
    bars = ax.bar(["Baseline p99", "Recovery latency"],
                  [baseline_p99, recovery_ms],
                  color=["#4caf50", "#f44336"], alpha=0.85, width=0.4)
    for bar, v in zip(bars, [baseline_p99, recovery_ms]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
                f"{v:.0f} ms", ha="center", va="bottom", fontsize=10, fontweight="bold")
    ax.set_ylabel("milliseconds")
    ax.set_title("Recovery Latency vs Baseline Write p99\n(time from docker start → first successful write)")
    ax.set_ylim(0, recovery_ms * 1.25)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_integrity(df: pd.DataFrame, out: str) -> None:
    integ = df[df["test_name"] == INTEGRITY_TEST].copy()
    if integ.empty:
        print("  (no DataIntegrity data — skipping)")
        return

    phases = integ["phase"].tolist()
    expected = integ["db_rows_expected"].tolist()
    actual   = integ["db_rows_actual"].tolist()

    x = range(len(phases))
    width = 0.35
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar([xi - width / 2 for xi in x], expected, width, label="Expected", color="#4caf50", alpha=0.85)
    ax.bar([xi + width / 2 for xi in x], actual,   width, label="Actual",   color="#2196f3", alpha=0.85)
    ax.set_xticks(list(x))
    ax.set_xticklabels(phases, rotation=10, ha="right")
    ax.set_ylabel("DB row count")
    ax.set_title("Data Integrity: Expected vs Actual Rows in DB at Each Checkpoint")
    ax.set_ylim(0, max(expected + actual) * 1.2)
    ax.legend()

    # annotate
    for i, (e, a) in enumerate(zip(expected, actual)):
        label = "✓ No Loss" if e == a else f"✗ Lost {e-a}"
        color = "green" if e == a else "red"
        ax.text(i, max(e, a) + max(expected) * 0.03, label,
                ha="center", fontsize=9, color=color, fontweight="bold")

    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved: {out}")


def chart_summary(df: pd.DataFrame, out: str) -> None:
    """All six panels in a single 3×2 figure for thesis inclusion."""
    fig = plt.figure(figsize=(18, 14))
    gs  = fig.add_gridspec(3, 2, hspace=0.45, wspace=0.35)

    tests_w = [t for t in WRITE_TESTS + [READ_TEST] if t in df["test_name"].values]
    tests_write = [t for t in WRITE_TESTS if t in df["test_name"].values]

    # Panel 1 — throughput
    ax1 = fig.add_subplot(gs[0, 0])
    _phase_bar(ax1, tests_w, df, "ops_per_sec", "ops/s", "Throughput")

    # Panel 2 — p99 latency
    ax2 = fig.add_subplot(gs[0, 1])
    _phase_bar(ax2, tests_w, df, "p99_ms", "ms", "P99 Latency", fmt="{:.0f}")

    # Panel 3 — availability %
    ax3 = fig.add_subplot(gs[1, 0])
    _phase_bar(ax3, tests_w, df, "availability_pct", "%", "Availability (%)", fmt="{:.0f}%")
    ax3.axhline(100, color="black", linestyle="--", linewidth=0.7)
    ax3.axhline(80,  color="orange", linestyle=":", linewidth=0.7)
    ax3.set_ylim(0, 115)

    # Panel 4 — error breakdown
    ax4 = fig.add_subplot(gs[1, 1])
    during = df[df["phase_label"] == "During"]
    rows = []
    for test in tests_w:
        sub = during[during["test_name"] == test]
        if not sub.empty:
            rows.append({"test": test,
                         "http_4xx": sub["http_4xx"].sum(),
                         "http_5xx": sub["http_5xx"].sum(),
                         "net_errors": sub["net_errors"].sum()})
    if rows:
        edf = pd.DataFrame(rows).set_index("test")
        bottom = [0] * len(edf)
        colors = {"http_4xx": "#ff9800", "http_5xx": "#f44336", "net_errors": "#9c27b0"}
        labels = {"http_4xx": "4xx", "http_5xx": "5xx", "net_errors": "Net err"}
        xi = range(len(edf))
        for col in ["http_4xx", "http_5xx", "net_errors"]:
            vals = edf[col].tolist()
            ax4.bar(xi, vals, bottom=bottom, label=labels[col], color=colors[col], alpha=0.85)
            bottom = [b + v for b, v in zip(bottom, vals)]
        ax4.set_xticks(list(xi))
        ax4.set_xticklabels(edf.index, rotation=15, ha="right", fontsize=8)
        ax4.set_ylabel("error count")
        ax4.set_title("Error Breakdown (during failure)")
        ax4.legend(fontsize=8)

    # Panel 5 — recovery latency
    ax5 = fig.add_subplot(gs[2, 0])
    rec = df[df["test_name"] == RECOVERY_TEST]
    if not rec.empty:
        row = rec.iloc[0]
        vals = [row["p99_ms"], row["recovery_ms"]]
        labels = ["Baseline p99", "Recovery latency"]
        bars = ax5.bar(labels, vals, color=["#4caf50", "#f44336"], alpha=0.85, width=0.4)
        for bar, v in zip(bars, vals):
            ax5.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(vals) * 0.02,
                     f"{v:.0f}ms", ha="center", va="bottom", fontsize=9, fontweight="bold")
        ax5.set_ylabel("ms")
        ax5.set_title("Recovery Latency vs Baseline p99")

    # Panel 6 — data integrity
    ax6 = fig.add_subplot(gs[2, 1])
    integ = df[df["test_name"] == INTEGRITY_TEST]
    if not integ.empty:
        phases = integ["phase"].tolist()
        expected = integ["db_rows_expected"].tolist()
        actual   = integ["db_rows_actual"].tolist()
        xi = range(len(phases))
        width = 0.35
        ax6.bar([x - width / 2 for x in xi], expected, width, label="Expected", color="#4caf50", alpha=0.85)
        ax6.bar([x + width / 2 for x in xi], actual,   width, label="Actual",   color="#2196f3", alpha=0.85)
        ax6.set_xticks(list(xi))
        ax6.set_xticklabels(phases, rotation=10, ha="right", fontsize=8)
        ax6.set_ylabel("DB row count")
        ax6.set_title("Data Integrity (DB rows expected vs actual)")
        ax6.legend(fontsize=8)
        for i, (e, a) in enumerate(zip(expected, actual)):
            ax6.text(i, max(e, a) + max(expected) * 0.04, "✓" if e == a else "✗",
                     ha="center", fontsize=12, color="green" if e == a else "red", fontweight="bold")

    fig.suptitle("YugabyteDB Fault-Tolerance Analysis — Resonate Server", fontsize=15, fontweight="bold")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: {out}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Usage: analyze.py [results_dir] [--latest]
    args = sys.argv[1:]
    latest_only = "--latest" in args
    args = [a for a in args if a != "--latest"]
    results_dir = args[0] if args else os.path.join(os.path.dirname(__file__), "results")
    df = load(results_dir, latest_only=latest_only)
    results_dir += f"_{ts}"
    Path(results_dir).mkdir(exist_ok=True)

    print("── TEXT REPORT ──────────────────────────────────────────────────────────────\n")
    print_report(df)

    print("── CHARTS ───────────────────────────────────────────────────────────────────\n")
    chart_throughput(df,  os.path.join(results_dir, f"analysis_throughput.png"))
    chart_latency(df,     os.path.join(results_dir, f"analysis_latency.png"))
    chart_availability(df,os.path.join(results_dir, f"analysis_availability.png"))
    chart_errors(df,      os.path.join(results_dir, f"analysis_errors.png"))
    chart_recovery(df,    os.path.join(results_dir, f"analysis_recovery.png"))
    chart_integrity(df,   os.path.join(results_dir, f"analysis_integrity.png"))
    chart_summary(df,     os.path.join(results_dir, f"analysis_summary.png"))
    print("\nDone.")


if __name__ == "__main__":
    main()