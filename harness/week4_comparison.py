import json
import os
import sys
import math
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

WRITE_RATES = [500, 1000, 2000]

# ── Load data ─────────────────────────────────────────────

def load_sweep_results():
    path = os.path.join(SCRIPT_DIR, "write-rate-sweep-results.json")
    if not os.path.exists(path):
        print(f"❌ Missing: {path}")
        print(f"   Run Task 2 first: python harness\\write_rate_sweep.py")
        sys.exit(1)
    with open(path) as f:
        return json.load(f)

def load_e2e_result():
    path = os.path.join(SCRIPT_DIR, "e2e-result-1000ops.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)

# ── Extract numbers ───────────────────────────────────────

def extract_row(sweep, rate):
    """Pull vanilla and WR-Raft p50/p99/p999 for a given rate."""
    vanilla = sweep.get(f"vanilla_{rate}", {})
    wr      = sweep.get(f"wr_{rate}",      {})

    return {
        "rate":       rate,
        "v_p50":      vanilla.get("commit_p50_ms")  or 0,
        "v_p99":      vanilla.get("commit_p99_ms")  or 0,
        "v_p999":     vanilla.get("commit_p999_ms") or 0,
        "w_p50":      wr.get("commit_p50_ms")       or 0,
        "w_p99":      wr.get("commit_p99_ms")       or 0,
        "w_p999":     wr.get("commit_p999_ms")      or 0,
    }

def improvement(v, w):
    if not v or v == 0:
        return 0.0
    return round((v - w) / v * 100, 1)

# ── Print comparison table ────────────────────────────────

def print_comparison_table(rows):
    print(f"\n{'='*78}")
    print(f"  WEEK 4 COMPARISON TABLE — WR-Raft vs Vanilla Raft")
    print(f"  Profile: MODERATE (5× spread) | 3 write rates | 60s each")
    print(f"  Workload: YCSB Workload-A (50/50 read-write)")
    print(f"{'='*78}")
    print(f"  {'Rate':<12} {'Metric':<6} "
          f"{'Vanilla (ms)':>13} {'WR-Raft (ms)':>13} {'Improvement':>12}")
    print(f"  {'-'*12} {'-'*6} {'-'*13} {'-'*13} {'-'*12}")

    for row in rows:
        rate_str = f"{row['rate']} ops/s"

        print(f"  {rate_str:<12} {'p50':<6} "
              f"{row['v_p50']:>13.3f} {row['w_p50']:>13.3f} "
              f"{improvement(row['v_p50'], row['w_p50']):>11.1f}%")
        print(f"  {'':12} {'p99':<6} "
              f"{row['v_p99']:>13.3f} {row['w_p99']:>13.3f} "
              f"{improvement(row['v_p99'], row['w_p99']):>11.1f}%")
        print(f"  {'':12} {'p999':<6} "
              f"{row['v_p999']:>13.3f} {row['w_p999']:>13.3f} "
              f"{improvement(row['v_p999'], row['w_p999']):>11.1f}%")
        print(f"  {'-'*12} {'-'*6} {'-'*13} {'-'*13} {'-'*12}")

    print(f"{'='*78}")
    print(f"  ✅ Positive % = WR-Raft faster than vanilla Raft")
    print(f"  ℹ  p99 is the key metric — tail latency improvement")
    print(f"{'='*78}\n")

# ── Save table as JSON ────────────────────────────────────

def save_table(rows):
    path = os.path.join(SCRIPT_DIR, "week4-comparison-table.json")
    out = {
        "generated":  datetime.utcnow().isoformat(),
        "profile":    "moderate",
        "workload":   "YCSB Workload-A 50/50",
        "duration_s": 60,
        "rows":       rows,
    }
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"  📄 Table saved to: {path}")

# ── Build bar chart ───────────────────────────────────────

def build_bar_chart(rows):
    """
    Grouped bar chart: x = write rate, two bars per rate
    (vanilla Raft in grey, WR-Raft in blue).
    Shows p99 only — the key metric.
    """
    rates       = [str(r["rate"]) for r in rows]
    vanilla_p99 = [r["v_p99"] for r in rows]
    wr_p99      = [r["w_p99"] for r in rows]

    x     = np.arange(len(rates))
    width = 0.35

    fig, ax = plt.subplots(figsize=(9, 6))
    fig.patch.set_facecolor("#FAFAFA")
    ax.set_facecolor("#FAFAFA")

    bars_v = ax.bar(
        x - width/2, vanilla_p99, width,
        label="Vanilla Raft",
        color="#9E9E9E",
        edgecolor="#616161",
        linewidth=0.8,
    )
    bars_w = ax.bar(
        x + width/2, wr_p99, width,
        label="WR-Raft",
        color="#1976D2",
        edgecolor="#0D47A1",
        linewidth=0.8,
    )

    # Value labels on top of each bar
    for bar in bars_v:
        h = bar.get_height()
        ax.annotate(
            f"{h:.3f}",
            xy=(bar.get_x() + bar.get_width() / 2, h),
            xytext=(0, 4), textcoords="offset points",
            ha="center", va="bottom", fontsize=9, color="#424242",
        )
    for bar in bars_w:
        h = bar.get_height()
        ax.annotate(
            f"{h:.3f}",
            xy=(bar.get_x() + bar.get_width() / 2, h),
            xytext=(0, 4), textcoords="offset points",
            ha="center", va="bottom", fontsize=9, color="#0D47A1",
        )

    # Improvement % annotations between bar pairs
    for i, row in enumerate(rows):
        imp = improvement(row["v_p99"], row["w_p99"])
        if imp > 0:
            label = f"↓{imp}%"
            color = "#2E7D32"
        elif imp < 0:
            label = f"↑{abs(imp)}%"
            color = "#C62828"
        else:
            label = "0%"
            color = "#757575"
        ax.text(
            i, max(row["v_p99"], row["w_p99"]) + 0.3,
            label,
            ha="center", va="bottom",
            fontsize=10, fontweight="bold", color=color,
        )

    ax.set_xlabel("Write Rate (ops/sec)", fontsize=12, labelpad=8)
    ax.set_ylabel("p99 Commit Latency (ms)", fontsize=12, labelpad=8)
    ax.set_title(
        "WR-Raft vs Vanilla Raft — p99 Commit Latency\n"
        "Profile: MODERATE (5× spread) | YCSB Workload-A",
        fontsize=13, pad=14,
    )
    ax.set_xticks(x)
    ax.set_xticklabels([f"{r} ops/s" for r in rates], fontsize=11)
    ax.legend(fontsize=10, framealpha=0.8)
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    ax.set_ylim(0, max(max(vanilla_p99), max(wr_p99)) * 1.35)

    plt.tight_layout()
    out_path = os.path.join(SCRIPT_DIR, "week4-p99-comparison-chart.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  📊 Bar chart saved to: {out_path}")
    return out_path

# ── Build p50/p99/p999 line chart ─────────────────────────

def build_line_chart(rows):
    """
    Line chart: x = write rate, lines for p50/p99/p999
    Solid = vanilla, dashed = WR-Raft
    """
    rates = [r["rate"] for r in rows]

    fig, axes = plt.subplots(1, 3, figsize=(14, 5), sharey=False)
    fig.patch.set_facecolor("#FAFAFA")
    fig.suptitle(
        "Commit Latency vs Write Rate — WR-Raft vs Vanilla Raft\n"
        "Profile: MODERATE | YCSB Workload-A",
        fontsize=12,
    )

    metrics = [
        ("p50",  "v_p50",  "w_p50",  "#FF9800"),
        ("p99",  "v_p99",  "w_p99",  "#1976D2"),
        ("p999", "v_p999", "w_p999", "#E91E63"),
    ]

    for ax, (label, vkey, wkey, color) in zip(axes, metrics):
        ax.set_facecolor("#FAFAFA")
        v_vals = [r[vkey] for r in rows]
        w_vals = [r[wkey] for r in rows]

        ax.plot(rates, v_vals, "o-",
                color="#9E9E9E", linewidth=2, markersize=7,
                label="Vanilla Raft")
        ax.plot(rates, w_vals, "s--",
                color=color, linewidth=2, markersize=7,
                label="WR-Raft")

        ax.set_title(f"{label} latency", fontsize=11)
        ax.set_xlabel("ops/sec", fontsize=10)
        ax.set_ylabel("ms", fontsize=10)
        ax.set_xticks(rates)
        ax.legend(fontsize=8)
        ax.grid(True, linestyle="--", alpha=0.4)

    plt.tight_layout()
    out_path = os.path.join(SCRIPT_DIR, "week4-latency-lines.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  📈 Line chart saved to: {out_path}")
    return out_path

# ── Main ──────────────────────────────────────────────────

def main():
    print(f"\n{'='*78}")
    print(f"  BUILDING WEEK 4 COMPARISON TABLE + CHARTS")
    print(f"{'='*78}")

    # Load data
    sweep = load_sweep_results()
    print(f"  ✅ Loaded write-rate-sweep-results.json")

    e2e = load_e2e_result()
    if e2e:
        print(f"  ✅ Loaded e2e-result-1000ops.json")

    # Extract rows for all 3 rates
    rows = [extract_row(sweep, rate) for rate in WRITE_RATES]

    # Print table to terminal
    print_comparison_table(rows)

    # Save table as JSON
    save_table(rows)

    # Build charts
    print(f"\n  Building charts...")
    build_bar_chart(rows)
    build_line_chart(rows)

    # Open folder so you can see the charts
    print(f"\n  ✅ All done! Open harness/ folder to view charts:")
    print(f"     explorer {SCRIPT_DIR}")
    print(f"\n  Files generated:")
    print(f"     week4-comparison-table.json")
    print(f"     week4-p99-comparison-chart.png  ← main deliverable")
    print(f"     week4-latency-lines.png          ← supporting chart")

if __name__ == "__main__":
    main()