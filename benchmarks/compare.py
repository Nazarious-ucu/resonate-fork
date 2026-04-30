#!/usr/bin/env python3
"""
PostgreSQL vs YugabyteDB — comprehensive benchmark comparison.

Covers three result sets:
  1. Microbenchmarks  — micro.csv + micro_yugabyte.csv
  2. Load tests       — load.csv  (both backends in one file)
  3. Fault tolerance  — fault_*.csv (YugabyteDB-only resilience tests)

Usage:
    pip install pandas matplotlib seaborn
    python benchmarks/compare.py [results_dir]

Outputs (written next to the script in benchmarks/comparison_<timestamp>/):
    micro_throughput.png
    micro_latency.png
    micro_scalability.png
    load_throughput.png
    load_latency.png
    fault_availability.png
    fault_errors.png
    fault_recovery.png
    summary.png
"""

import glob
import os
import sys
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

# ── style ─────────────────────────────────────────────────────────────────────
sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)

PG_COLOR   = "#4C72B0"
YB_COLOR   = "#DD8452"
PHASE_COLORS = {
    "Before": "#4caf50",
    "During": "#f44336",
    "After":  "#2196f3",
}
PHASE_MAP = {
    "before-failure": "Before", "before-pause": "Before",
    "during-failure": "During", "during-pause": "During",
    "after-recovery": "After",  "after-unpause": "After",
}

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


# ── data loading ──────────────────────────────────────────────────────────────

def load_micro(results_dir: str) -> pd.DataFrame:
    frames = []
    for name in ("micro.csv", "micro_yugabyte.csv"):
        p = os.path.join(results_dir, name)
        if os.path.exists(p):
            frames.append(pd.read_csv(p))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df["ms_per_op"] = df["ns_per_op"] / 1_000_000
    return df


def load_load(results_dir: str) -> pd.DataFrame:
    p = os.path.join(results_dir, "load.csv")
    if not os.path.exists(p):
        return pd.DataFrame()
    df = pd.read_csv(p)
    # When multiple runs exist for the same (backend, workers), keep highest-throughput row
    df = (df.sort_values("total_ops", ascending=False)
            .drop_duplicates(subset=["backend", "workers"], keep="first")
            .sort_values(["backend", "workers"]))
    return df


def load_fault(results_dir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(results_dir, "fault_*.csv")))
    if not files:
        return pd.DataFrame()
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    # Use latest run per test
    df["run_id"] = df["run_id"].astype(str)
    latest = df.sort_values("run_id").groupby("test_name")["run_id"].last().reset_index()
    df = df.merge(latest, on=["test_name", "run_id"])
    df["phase_label"] = df["phase"].map(PHASE_MAP).fillna(df["phase"])
    total = df["http_2xx"] + df["http_4xx"] + df["http_5xx"] + df["net_errors"]
    df["availability_pct"] = (df["http_2xx"] / total.replace(0, float("nan"))) * 100
    df["total_errors"] = df["http_4xx"] + df["http_5xx"] + df["net_errors"]
    return df


# ── helpers ───────────────────────────────────────────────────────────────────

def _bar_label(ax, bars, fmt="{:.0f}"):
    max_h = max((b.get_height() for b in bars), default=1) or 1
    for bar in bars:
        h = bar.get_height()
        if h > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, h + max_h * 0.01,
                    fmt.format(h), ha="center", va="bottom", fontsize=7.5)


def _save(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: {path}")


# ── Section 1 – Microbenchmarks ───────────────────────────────────────────────

def analyze_micro(df: pd.DataFrame, out_dir: str):
    if df.empty:
        print("[micro] No data — skipping.")
        return

    benchmarks = sorted(df["benchmark"].unique())
    backends   = ["postgres", "yugabyte"]
    x = np.arange(len(benchmarks))
    width = 0.35

    # ── 1a. Peak throughput bar chart ─────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(12, 5))
    for i, (backend, color) in enumerate(zip(backends, [PG_COLOR, YB_COLOR])):
        mask = df["backend"] == backend
        vals = [df[mask & (df["benchmark"] == b)]["ops_per_sec"].max() or 0
                for b in benchmarks]
        bars = ax.bar(x + (i - 0.5) * width, vals, width,
                      label=backend.capitalize(), color=color, alpha=0.85)
        _bar_label(ax, bars, "{:,.0f}")
    ax.set_xticks(x)
    ax.set_xticklabels(benchmarks, rotation=25, ha="right")
    ax.set_ylabel("ops / second")
    ax.set_title("Microbenchmarks — Peak Throughput: PostgreSQL vs YugabyteDB",
                 fontweight="bold")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.legend()
    _save(fig, os.path.join(out_dir, "micro_throughput.png"))

    # ── 1b. Latency at max-N bar chart ────────────────────────────────────────
    max_n_df = df.loc[df.groupby(["backend", "benchmark"])["n"].idxmax()]
    fig, ax = plt.subplots(figsize=(12, 5))
    for i, (backend, color) in enumerate(zip(backends, [PG_COLOR, YB_COLOR])):
        mask = max_n_df["backend"] == backend
        vals = [max_n_df[mask & (max_n_df["benchmark"] == b)]["ms_per_op"].values[0]
                if not max_n_df[mask & (max_n_df["benchmark"] == b)].empty else 0
                for b in benchmarks]
        bars = ax.bar(x + (i - 0.5) * width, vals, width,
                      label=backend.capitalize(), color=color, alpha=0.85)
        _bar_label(ax, bars, "{:.2f}")
    ax.set_xticks(x)
    ax.set_xticklabels(benchmarks, rotation=25, ha="right")
    ax.set_ylabel("ms / op  (lower is better)")
    ax.set_title("Microbenchmarks — Latency at Max Batch: PostgreSQL vs YugabyteDB",
                 fontweight="bold")
    ax.legend()
    _save(fig, os.path.join(out_dir, "micro_latency.png"))

    # ── 1c. Scalability curves (ops/sec vs n) ────────────────────────────────
    ncols = 3
    nrows = -(-len(benchmarks) // ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(14, 4 * nrows))
    axes = np.array(axes).flatten()
    for idx, bench in enumerate(benchmarks):
        ax = axes[idx]
        for backend, color in zip(backends, [PG_COLOR, YB_COLOR]):
            sub = df.query("backend==@backend and benchmark==@bench").sort_values("n")
            if not sub.empty:
                ax.plot(sub["n"], sub["ops_per_sec"], marker="o",
                        label=backend.capitalize(), color=color)
        ax.set_title(bench, fontsize=9)
        ax.set_xlabel("batch size (n)")
        ax.set_ylabel("ops/sec")
        ax.set_xscale("log")
        ax.legend(fontsize=7)
    for i in range(idx + 1, len(axes)):
        axes[i].set_visible(False)
    fig.suptitle("Throughput Scalability by Benchmark Operation", fontsize=13,
                 fontweight="bold")
    _save(fig, os.path.join(out_dir, "micro_scalability.png"))

    # ── Text summary ──────────────────────────────────────────────────────────
    print("\n=== MICROBENCHMARKS ===")
    print(f"{'Benchmark':<35} {'PG ops/s':>12} {'YB ops/s':>12} {'PG/YB ratio':>12}")
    print("-" * 73)
    for bench in benchmarks:
        pg = df[(df["backend"] == "postgres") & (df["benchmark"] == bench)]["ops_per_sec"].max()
        yb = df[(df["backend"] == "yugabyte") & (df["benchmark"] == bench)]["ops_per_sec"].max()
        ratio = f"{pg/yb:.1f}x" if yb > 0 else "N/A"
        print(f"  {bench:<33} {pg:>12,.0f} {yb:>12,.0f} {ratio:>12}")


# ── Section 2 – Load Tests ────────────────────────────────────────────────────

def analyze_load(df: pd.DataFrame, out_dir: str):
    print(df.head())
    if df.empty:
        print("[load] No data — skipping.")
        return

    workers = sorted(df["workers"].unique())
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Load Test — PostgreSQL vs YugabyteDB", fontsize=13, fontweight="bold")

    # ── 2a. Throughput vs workers ─────────────────────────────────────────────
    ax = axes[0]
    for backend, color in zip(["postgres", "yugabyte"], [PG_COLOR, YB_COLOR]):
        sub = df[df["backend"] == backend].sort_values("workers")
        if sub.empty:
            continue
        ax.plot(sub["workers"], sub["ops_per_sec"], marker="o",
                label=backend.capitalize(), color=color, linewidth=2)
        for _, row in sub.iterrows():
            ax.annotate(f"{row['ops_per_sec']:,.0f}",
                        xy=(row["workers"], row["ops_per_sec"]),
                        xytext=(5, 5), textcoords="offset points", fontsize=7)
    ax.set_xlabel("Concurrent workers")
    ax.set_ylabel("ops / second")
    ax.set_title("Throughput")
    ax.set_xticks(workers)
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))

    # ── 2b. P99 latency vs workers ────────────────────────────────────────────
    ax = axes[1]
    for backend, color in zip(["postgres", "yugabyte"], [PG_COLOR, YB_COLOR]):
        sub = df[df["backend"] == backend].sort_values("workers")
        if sub.empty:
            continue
        ax.plot(sub["workers"], sub["p99_us"] / 1000, marker="o",
                label=backend.capitalize(), color=color, linewidth=2)
        for _, row in sub.iterrows():
            ax.annotate(f"{row['p99_us']/1000:.1f}",
                        xy=(row["workers"], row["p99_us"] / 1000),
                        xytext=(5, 5), textcoords="offset points", fontsize=7)
    ax.set_xlabel("Concurrent workers")
    ax.set_ylabel("P99 latency (ms)")
    ax.set_title("P99 Latency")
    ax.set_xticks(workers)
    ax.legend()
    ax.grid(True, alpha=0.3)

    _save(fig, os.path.join(out_dir, "load_throughput.png"))

    # ── 2c. Full latency percentile comparison at each worker count ───────────
    pct_cols = [("mean_us", "Mean"), ("p50_us", "P50"), ("p95_us", "P95"), ("p99_us", "P99")]
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Load Test — Latency Percentiles: PostgreSQL vs YugabyteDB",
                 fontsize=13, fontweight="bold")
    for ax, (col, label) in zip(axes.flatten(), pct_cols):
        for backend, color in zip(["postgres", "yugabyte"], [PG_COLOR, YB_COLOR]):
            sub = df[df["backend"] == backend].sort_values("workers")
            if sub.empty:
                continue
            ax.plot(sub["workers"], sub[col] / 1000, marker="o",
                    label=backend.capitalize(), color=color, linewidth=2)
        ax.set_title(f"{label} Latency")
        ax.set_xlabel("Workers")
        ax.set_ylabel("ms")
        ax.set_xticks(workers)
        ax.legend()
        ax.grid(True, alpha=0.3)
    _save(fig, os.path.join(out_dir, "load_latency.png"))

    # ── Text summary ──────────────────────────────────────────────────────────
    print("\n=== LOAD TESTS ===")
    print(f"{'Workers':>8} {'Backend':>12} {'ops/s':>10} {'Mean µs':>10} "
          f"{'P50 µs':>10} {'P95 µs':>10} {'P99 µs':>10} {'PG/YB':>8}")
    print("-" * 82)
    for w in workers:
        pg_row = df.query("backend=='postgres' and workers==@w")
        yb_row = df.query("backend=='yugabyte' and workers==@w")
        for backend, row in [("postgres", pg_row), ("yugabyte", yb_row)]:
            if row.empty:
                continue
            r = row.iloc[0]
            ratio = ""
            if backend == "yugabyte" and not pg_row.empty:
                pg_ops = pg_row.iloc[0]["ops_per_sec"]
                ratio = f"{pg_ops / r['ops_per_sec']:.1f}x" if r["ops_per_sec"] else "—"
            print(f"  {w:>6} {backend:>12} {r['ops_per_sec']:>10,.0f} "
                  f"{r['mean_us']:>10,.0f} {r['p50_us']:>10,.0f} "
                  f"{r['p95_us']:>10,.0f} {r['p99_us']:>10,.0f} {ratio:>8}")


# ── Section 3 – Fault Tolerance ───────────────────────────────────────────────

WRITE_TESTS    = ["NodeKill", "PrimaryNodeKill", "NodePause"]
READ_TEST      = "ReadAvailability"
INTEGRITY_TEST = "DataIntegrity"
CONCURRENT_TEST= "ConcurrentClaim"
RECOVERY_TEST  = "RecoveryLatency"
AVAIL_TESTS    = WRITE_TESTS + [READ_TEST]


def _phase_bar(ax, tests, df, col, ylabel, title, fmt="{:.0f}"):
    x = np.arange(len(tests))
    w = 0.25
    for i, phase in enumerate(["Before", "During", "After"]):
        mask_phase = df["phase_label"] == phase
        vals = [df[mask_phase & (df["test_name"] == t)][col].mean() or 0
                for t in tests]
        bars = ax.bar(x + (i - 1) * w, vals, w,
                      label=phase, color=PHASE_COLORS[phase], alpha=0.85)
        _bar_label(ax, bars, fmt)
    ax.set_xticks(x)
    ax.set_xticklabels(tests, rotation=15, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(fontsize=8)


def analyze_fault(df: pd.DataFrame, out_dir: str):
    if df.empty:
        print("[fault] No data — skipping.")
        return

    avail_df = df[df["test_name"].isin(AVAIL_TESTS)]

    # ── 3a. Throughput & availability ─────────────────────────────────────────
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Fault Tolerance — YugabyteDB Availability", fontsize=13, fontweight="bold")
    tests = [t for t in AVAIL_TESTS if t in avail_df["test_name"].values]
    _phase_bar(axes[0], tests, avail_df, "ops_per_sec", "ops / second",
               "Throughput Before / During / After Failure")
    _phase_bar(axes[1], tests, avail_df, "availability_pct", "availability (%)",
               "Request Availability (% 2xx)", fmt="{:.0f}%")
    axes[1].axhline(100, color="black", linestyle="--", linewidth=0.8)
    axes[1].axhline(80,  color="orange", linestyle=":", linewidth=0.8)
    axes[1].set_ylim(0, 115)
    _save(fig, os.path.join(out_dir, "fault_availability.png"))

    # ── 3b. Error breakdown ───────────────────────────────────────────────────
    during = df[df["phase_label"] == "During"]
    rows = []
    for test in tests:
        sub = during[during["test_name"] == test]
        if not sub.empty:
            rows.append({"test": test,
                         "HTTP 4xx": sub["http_4xx"].sum(),
                         "HTTP 5xx": sub["http_5xx"].sum(),
                         "Net/Timeout": sub["net_errors"].sum()})
    if rows:
        edf = pd.DataFrame(rows).set_index("test")
        fig, ax = plt.subplots(figsize=(9, 5))
        bottom = np.zeros(len(edf))
        colors = {"HTTP 4xx": "#ff9800", "HTTP 5xx": "#f44336", "Net/Timeout": "#9c27b0"}
        for col, color in colors.items():
            vals = edf[col].values
            bars = ax.bar(range(len(edf)), vals, bottom=bottom,
                          label=col, color=color, alpha=0.85)
            bottom += vals
        ax.set_xticks(range(len(edf)))
        ax.set_xticklabels(edf.index, rotation=15, ha="right")
        ax.set_ylabel("Error count (during-failure phase)")
        ax.set_title("Error Type Breakdown During Failure — YugabyteDB", fontweight="bold")
        ax.legend()
        _save(fig, os.path.join(out_dir, "fault_errors.png"))

    # ── 3c. Recovery latency ──────────────────────────────────────────────────
    rec = df[df["test_name"] == RECOVERY_TEST]
    if not rec.empty:
        row = rec.iloc[0]
        rec_ms  = row["recovery_ms"]
        base_p99 = row["p99_ms"]
        fig, ax = plt.subplots(figsize=(6, 5))
        bars = ax.bar(["Baseline P99", "Recovery latency"],
                      [base_p99, rec_ms],
                      color=["#4caf50", "#f44336"], alpha=0.85, width=0.4)
        for bar, v in zip(bars, [base_p99, rec_ms]):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(rec_ms, base_p99) * 0.02,
                    f"{v:.0f} ms", ha="center", va="bottom", fontsize=10, fontweight="bold")
        ax.set_ylabel("milliseconds")
        ax.set_title("Recovery Latency vs Baseline Write P99\n"
                     "(docker start → first successful write)", fontweight="bold")
        ax.set_ylim(0, max(rec_ms, base_p99) * 1.3)
        _save(fig, os.path.join(out_dir, "fault_recovery.png"))

    # ── Text summary ──────────────────────────────────────────────────────────
    print("\n=== FAULT TOLERANCE (YugabyteDB) ===")

    print(f"\n{'Test':<22} {'Phase':<10} {'ops/s':>8} {'avail%':>8} {'p95 ms':>8} {'errors':>8}")
    print("-" * 70)
    for test in AVAIL_TESTS:
        sub = avail_df[avail_df["test_name"] == test]
        if sub.empty:
            continue
        for _, row in sub.iterrows():
            avail = f"{row['availability_pct']:.1f}%" if not np.isnan(row["availability_pct"]) else "N/A"
            print(f"  {test:<20} {row['phase_label']:<10} {row['ops_per_sec']:>8.0f} "
                  f"{avail:>8} {row['p95_ms']:>8.1f} {row['total_errors']:>8.0f}")
        print()

    integ = df[df["test_name"] == INTEGRITY_TEST]
    if not integ.empty:
        print("Data Integrity:")
        for _, row in integ.iterrows():
            e, a = int(row.get("db_rows_expected", 0)), int(row.get("db_rows_actual", 0))
            ok = "PASS" if e == a else f"FAIL (lost {e-a})"
            print(f"  {row['phase']:<22} expected={e}  actual={a}  [{ok}]")

    cc = df[df["test_name"] == CONCURRENT_TEST]
    if not cc.empty:
        row = cc.iloc[0]
        e, a = int(row.get("db_rows_expected", 0)), int(row.get("db_rows_actual", 0))
        print(f"\nConcurrentClaim (exactly-once): expected={e}  actual={a}  "
              f"[{'PASS' if e == a else 'FAIL'}]  {row['notes']}")

    if not rec.empty:
        row = rec.iloc[0]
        print(f"\nRecovery latency: {row['recovery_ms']:.0f} ms  "
              f"(baseline P99: {row['p99_ms']:.1f} ms  "
              f"ratio: {row['recovery_ms']/row['p99_ms']:.0f}×)")


# ── Section 4 – Combined summary chart ────────────────────────────────────────

def chart_summary(micro: pd.DataFrame, load: pd.DataFrame,
                  fault: pd.DataFrame, out_dir: str):
    fig = plt.figure(figsize=(18, 16))
    gs  = fig.add_gridspec(3, 3, hspace=0.55, wspace=0.4)
    fig.suptitle("PostgreSQL vs YugabyteDB — Benchmark Summary (Resonate Server)",
                 fontsize=15, fontweight="bold")

    # ── Panel A: micro peak throughput ────────────────────────────────────────
    ax = fig.add_subplot(gs[0, :2])
    if not micro.empty:
        benchmarks = sorted(micro["benchmark"].unique())
        x = np.arange(len(benchmarks))
        w = 0.35
        for i, (backend, color) in enumerate(zip(["postgres", "yugabyte"], [PG_COLOR, YB_COLOR])):
            mask = micro["backend"] == backend
            vals = [micro[mask & (micro["benchmark"] == b)]["ops_per_sec"].max() or 0
                    for b in benchmarks]
            bars = ax.bar(x + (i - 0.5) * w, vals, w,
                          label=backend.capitalize(), color=color, alpha=0.85)
            _bar_label(ax, bars, "{:,.0f}")
        ax.set_xticks(x)
        ax.set_xticklabels(benchmarks, rotation=20, ha="right", fontsize=8)
        ax.set_ylabel("ops / sec")
        ax.set_title("Microbenchmarks — Peak Throughput")
        ax.legend(fontsize=9)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))

    # ── Panel B: PG/YB ratio bar ──────────────────────────────────────────────
    ax = fig.add_subplot(gs[0, 2])
    if not micro.empty:
        ratios = []
        for bench in benchmarks:
            pg = micro[(micro["backend"] == "postgres") & (micro["benchmark"] == bench)]["ops_per_sec"].max()
            yb = micro[(micro["backend"] == "yugabyte") & (micro["benchmark"] == bench)]["ops_per_sec"].max()
            ratios.append(pg / yb if yb > 0 else 0)
        colors = ["#d62728" if r > 1 else "#2ca02c" for r in ratios]
        bars = ax.barh(benchmarks, ratios, color=colors, alpha=0.85)
        ax.axvline(1, color="black", linestyle="--", linewidth=1)
        for bar, v in zip(bars, ratios):
            ax.text(v + 0.05, bar.get_y() + bar.get_height() / 2,
                    f"{v:.1f}×", va="center", fontsize=8)
        ax.set_xlabel("PG / YB throughput ratio")
        ax.set_title("Speed Ratio\n(red = PG faster, green = YB faster)")
        ax.tick_params(axis="y", labelsize=8)

    # ── Panel C: load throughput ───────────────────────────────────────────────
    ax = fig.add_subplot(gs[1, :2])
    if not load.empty:
        workers = sorted(load["workers"].unique())
        for backend, color in zip(["postgres", "yugabyte"], [PG_COLOR, YB_COLOR]):
            sub = load[load["backend"] == backend].sort_values("workers")
            if not sub.empty:
                ax.plot(sub["workers"], sub["ops_per_sec"], marker="o",
                        label=backend.capitalize(), color=color, linewidth=2)
                for _, row in sub.iterrows():
                    ax.annotate(f"{row['ops_per_sec']:,.0f}",
                                xy=(row["workers"], row["ops_per_sec"]),
                                xytext=(4, 6), textcoords="offset points", fontsize=7)
        ax.set_xlabel("Concurrent workers")
        ax.set_ylabel("ops / second")
        ax.set_title("Load Test — Throughput Scaling")
        ax.set_xticks(workers)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))

    # ── Panel D: load P99 latency ─────────────────────────────────────────────
    ax = fig.add_subplot(gs[1, 2])
    if not load.empty:
        for backend, color in zip(["postgres", "yugabyte"], [PG_COLOR, YB_COLOR]):
            sub = load[load["backend"] == backend].sort_values("workers")
            if not sub.empty:
                ax.plot(sub["workers"], sub["p99_us"] / 1000, marker="s",
                        label=backend.capitalize(), color=color, linewidth=2)
        ax.set_xlabel("Workers")
        ax.set_ylabel("P99 latency (ms)")
        ax.set_title("Load Test — P99 Latency")
        ax.set_xticks(workers)
        ax.legend()
        ax.grid(True, alpha=0.3)

    # ── Panel E: fault throughput retention ───────────────────────────────────
    ax = fig.add_subplot(gs[2, :2])
    if not fault.empty:
        avail_df = fault[fault["test_name"].isin(AVAIL_TESTS)]
        tests = [t for t in AVAIL_TESTS if t in avail_df["test_name"].values]
        _phase_bar(ax, tests, avail_df, "ops_per_sec",
                   "ops / second", "Fault Tolerance — Throughput During Failures (YugabyteDB)")

    # ── Panel F: error breakdown ───────────────────────────────────────────────
    ax = fig.add_subplot(gs[2, 2])
    if not fault.empty:
        during = fault[fault["phase_label"] == "During"]
        tests  = [t for t in AVAIL_TESTS if t in fault["test_name"].values]
        rows = []
        for t in tests:
            sub = during[during["test_name"] == t]
            if not sub.empty:
                rows.append({"test": t,
                             "5xx": sub["http_5xx"].sum(),
                             "Net": sub["net_errors"].sum()})
        if rows:
            edf = pd.DataFrame(rows).set_index("test")
            xi = np.arange(len(edf))
            bottom = np.zeros(len(edf))
            for col, color, lbl in [("5xx", "#f44336", "HTTP 5xx"),
                                     ("Net", "#9c27b0", "Net/Timeout")]:
                ax.bar(xi, edf[col].values, bottom=bottom, label=lbl, color=color, alpha=0.85)
                bottom += edf[col].values
            ax.set_xticks(xi)
            ax.set_xticklabels(edf.index, rotation=15, ha="right", fontsize=8)
            ax.set_ylabel("Error count")
            ax.set_title("Errors During Failure")
            ax.legend(fontsize=8)

    fig.savefig(os.path.join(out_dir, "summary.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: {os.path.join(out_dir, 'summary.png')}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    results_dir = args[0] if args else RESULTS_DIR

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(os.path.dirname(__file__), f"comparison_{ts}")
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    print(f"Results dir : {results_dir}")
    print(f"Output dir  : {out_dir}\n")

    micro = load_micro(results_dir)
    load  = load_load(results_dir)
    fault = load_fault(results_dir)

    print("─── Microbenchmarks " + "─" * 60)
    analyze_micro(micro, out_dir)

    print("\n─── Load Tests " + "─" * 65)
    analyze_load(load, out_dir)

    print("\n─── Fault Tolerance " + "─" * 60)
    analyze_fault(fault, out_dir)

    print("\n─── Combined Summary Chart " + "─" * 53)
    chart_summary(micro, load, fault, out_dir)

    print(f"\nDone. All output in: {out_dir}/")


if __name__ == "__main__":
    main()