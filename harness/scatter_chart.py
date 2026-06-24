import json
import os
import sys
import math
import matplotlib
matplotlib.use("Agg")   # non-interactive backend — saves to file
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

NODES = ["node1", "node2", "node3", "node4", "node5"]

# One color per node — consistent across all charts
NODE_COLORS = {
    "node1": "#2196F3",   # blue
    "node2": "#4CAF50",   # green
    "node3": "#FF9800",   # orange
    "node4": "#E91E63",   # pink  ← slow node
    "node5": "#9C27B0",   # purple ← slow node
}

NODE_MARKERS = {
    "node1": "o",
    "node2": "s",   # square
    "node3": "^",   # triangle
    "node4": "D",   # diamond
    "node5": "P",   # plus-filled
}

# ── Load data ─────────────────────────────────────────────

def load_results(path):
    with open(path, "r") as f:
        return json.load(f)

# ── Extract points ────────────────────────────────────────

def extract_points(records):
    """
    Returns a dict: {node: {"p99": [...], "ewa_weight": [...]}}
    One entry per epoch where p99 is not None.
    """
    points = {node: {"p99": [], "ewa_weight": []} for node in NODES}

    for rec in records:
        for node in NODES:
            p99    = rec["p99_ms"].get(node)
            weight = rec["ewa_weight"].get(node)
            if p99 is not None and weight is not None and p99 > 0:
                points[node]["p99"].append(p99)
                points[node]["ewa_weight"].append(weight)

    return points

# ── Fit a 1/x reference curve ────────────────────────────

def fit_inverse_curve(all_p99, all_weights):
    """
    Fits y = k / x to the scatter data.
    Returns (x_range, y_range) arrays for plotting.
    """
    if not all_p99:
        return None, None

    # Estimate k: k = median(weight * p99)
    products = [w * p for w, p in zip(all_weights, all_p99)]
    k = sorted(products)[len(products) // 2]

    x_min = max(0.1, min(all_p99) * 0.8)
    x_max = max(all_p99) * 1.2
    x_range = np.linspace(x_min, x_max, 200)
    y_range = k / x_range

    return x_range, y_range

# ── Main chart ────────────────────────────────────────────

def build_scatter_chart(records, out_path, profile_name="moderate"):
    points = extract_points(records)

    fig, ax = plt.subplots(figsize=(9, 6))
    fig.patch.set_facecolor("#FAFAFA")
    ax.set_facecolor("#FAFAFA")

    # Collect all points for curve fitting
    all_p99     = []
    all_weights = []

    # Plot one node at a time
    for node in NODES:
        p99_vals    = points[node]["p99"]
        weight_vals = points[node]["ewa_weight"]

        if not p99_vals:
            continue

        ax.scatter(
            p99_vals,
            weight_vals,
            color   = NODE_COLORS[node],
            marker  = NODE_MARKERS[node],
            s       = 80,
            alpha   = 0.85,
            zorder  = 3,
            label   = node,
        )

        # Annotate the node name near its centroid
        cx = sum(p99_vals)    / len(p99_vals)
        cy = sum(weight_vals) / len(weight_vals)
        ax.annotate(
            node,
            xy         = (cx, cy),
            xytext     = (8, 4),
            textcoords = "offset points",
            fontsize   = 8,
            color      = NODE_COLORS[node],
            fontweight = "bold",
        )

        all_p99.extend(p99_vals)
        all_weights.extend(weight_vals)

    # Draw reference 1/x curve
    x_curve, y_curve = fit_inverse_curve(all_p99, all_weights)
    if x_curve is not None:
        ax.plot(
            x_curve, y_curve,
            color     = "#888888",
            linewidth = 1.4,
            linestyle = "--",
            zorder    = 2,
            label     = "y = k/x reference",
        )

    # Labels and title
    ax.set_xlabel("Measured p99 fsync latency (ms)", fontsize=12, labelpad=8)
    ax.set_ylabel("Assigned EWA weight",             fontsize=12, labelpad=8)
    ax.set_title(
        f"Weight vs. p99 Latency per Node\n"
        f"Profile: {profile_name.upper()} (5× spread) — "
        f"{len(records)} epochs × 5 nodes",
        fontsize = 13,
        pad      = 14,
    )

    # Annotations explaining the shape
    ax.text(
        0.03, 0.97,
        "Fast nodes (node1/2/3)\nhigh weight, low p99",
        transform   = ax.transAxes,
        fontsize    = 8,
        color       = "#2196F3",
        va          = "top",
        ha          = "left",
        style       = "italic",
    )
    ax.text(
        0.97, 0.10,
        "Slow nodes (node4/5)\nlow weight, high p99",
        transform   = ax.transAxes,
        fontsize    = 8,
        color       = "#E91E63",
        va          = "bottom",
        ha          = "right",
        style       = "italic",
    )

    # Grid and legend
    ax.grid(True, linestyle="--", alpha=0.4, zorder=1)
    ax.legend(loc="upper right", fontsize=9, framealpha=0.8)

    # Tight layout and save
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  📊 Chart saved to: {out_path}")

# ── Second chart: weight over time per node ───────────────

def build_timeseries_chart(records, out_path):
    """
    Line chart: x = epoch number, y = EWA weight per node.
    Shows how weight stabilises over time.
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
    fig.patch.set_facecolor("#FAFAFA")

    epochs = [rec["epoch"] for rec in records]

    # Top chart: weight over time
    ax1.set_facecolor("#FAFAFA")
    for node in NODES:
        weights = [rec["ewa_weight"].get(node, None) for rec in records]
        ax1.plot(
            epochs, weights,
            color     = NODE_COLORS[node],
            marker    = NODE_MARKERS[node],
            markersize= 5,
            linewidth = 1.6,
            label     = node,
        )
    ax1.set_ylabel("EWA Weight", fontsize=11)
    ax1.set_title("EWA Weight per Node over Time (moderate profile)", fontsize=12)
    ax1.grid(True, linestyle="--", alpha=0.4)
    ax1.legend(loc="right", fontsize=8)

    # Bottom chart: p99 over time
    ax2.set_facecolor("#FAFAFA")
    for node in NODES:
        p99s = [rec["p99_ms"].get(node, None) for rec in records]
        ax2.plot(
            epochs, p99s,
            color     = NODE_COLORS[node],
            marker    = NODE_MARKERS[node],
            markersize= 5,
            linewidth = 1.6,
            label     = node,
        )
    ax2.set_xlabel("Epoch", fontsize=11)
    ax2.set_ylabel("p99 fsync latency (ms)", fontsize=11)
    ax2.set_title("Measured p99 per Node over Time", fontsize=12)
    ax2.grid(True, linestyle="--", alpha=0.4)
    ax2.legend(loc="right", fontsize=8)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  📈 Time-series chart saved to: {out_path}")

# ── Print stats table ─────────────────────────────────────

def print_stats_table(records):
    points = extract_points(records)

    print(f"\n{'='*65}")
    print(f"  SCATTER DATA SUMMARY")
    print(f"{'='*65}")
    print(f"  {'Node':<8} {'Min p99':>9} {'Max p99':>9} "
          f"{'Avg p99':>9} {'Avg weight':>12} {'Points':>7}")
    print(f"  {'-'*8} {'-'*9} {'-'*9} {'-'*9} {'-'*12} {'-'*7}")

    for node in NODES:
        p99s    = points[node]["p99"]
        weights = points[node]["ewa_weight"]
        if not p99s:
            print(f"  {node:<8} {'N/A':>9}")
            continue

        min_p99    = min(p99s)
        max_p99    = max(p99s)
        avg_p99    = round(sum(p99s)    / len(p99s),    3)
        avg_weight = round(sum(weights) / len(weights), 4)
        n_points   = len(p99s)

        print(f"  {node:<8} {min_p99:>8.3f}ms {max_p99:>8.3f}ms "
              f"{avg_p99:>8.3f}ms {avg_weight:>12.4f} {n_points:>7}")

    print(f"{'='*65}")
    print(f"  Expected: node4/5 avg weight << node1/2/3 avg weight")
    print(f"{'='*65}\n")

# ── Main ──────────────────────────────────────────────────

def main():
    # Find the results file
    results_path = os.path.join(SCRIPT_DIR, "weight-sampler-results.json")

    if not os.path.exists(results_path):
        print(f"❌ Could not find: {results_path}")
        print(f"   Run Task 2 first: python harness\\weight_sampler.py")
        sys.exit(1)

    print(f"\n📂 Loading data from: {results_path}")
    records = load_results(results_path)
    print(f"   {len(records)} epochs loaded, {len(records) * 5} total data points")

    # Print stats table
    print_stats_table(records)

    # Build scatter chart
    scatter_path = os.path.join(SCRIPT_DIR, "weight_vs_latency_scatter.png")
    build_scatter_chart(records, scatter_path, profile_name="moderate")

    # Build time-series chart
    ts_path = os.path.join(SCRIPT_DIR, "weight_timeseries.png")
    build_timeseries_chart(records, ts_path)

    print(f"\n✅ Both charts saved in harness/ folder.")
    print(f"   Open them in VS Code or Windows Explorer to view.\n")

if __name__ == "__main__":
    main()