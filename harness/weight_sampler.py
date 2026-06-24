import time
import math
import json
import os
import sys
import urllib.request
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

# ── Config ──────────────────────────────────────────────
NODES = ["node1", "node2", "node3", "node4", "node5"]

METRICS_PORTS = {
    "node1": 9091,
    "node2": 9092,
    "node3": 9093,
    "node4": 9094,
    "node5": 9095,
}

SAMPLE_INTERVAL_SEC = 10   # how often to sample
TOTAL_DURATION_SEC  = 300  # 5 minutes total
EWA_ALPHA           = 0.2  # smoothing factor (same as Intern B will use)

# ── Metrics helpers ──────────────────────────────────────

def fetch_metrics(port):
    try:
        url = f"http://localhost:{port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.read().decode("utf-8")
    except Exception:
        return ""

def parse_p99(metrics_text):
    """Parse fsync_duration_ns histogram → p99 in ms."""
    buckets     = {}
    total_count = 0.0

    for line in metrics_text.splitlines():
        if line.startswith("#"):
            continue
        if line.startswith("fsync_duration_ns_bucket{"):
            try:
                le_str = line.split('le="')[1].split('"')[0]
                count  = float(line.split("} ")[1])
                le_val = math.inf if le_str == "+Inf" else float(le_str)
                buckets[le_val] = count
            except Exception:
                pass
        if line.startswith("fsync_duration_ns_count "):
            try:
                total_count = float(line.split(" ")[1])
            except Exception:
                pass

    if total_count == 0 or not buckets:
        return None

    target = 0.99 * total_count
    for le in sorted(k for k in buckets if k != math.inf):
        if buckets[le] >= target:
            return round(le / 1_000_000, 3)   # ns → ms
    return None

# ── Weight computation ───────────────────────────────────

def compute_weights(p99_map):
    """
    Given {node: p99_ms}, compute normalized weights.
    Formula: raw_weight = 1 / p99_ms
    Then normalize so sum of weights = number of nodes.
    Returns {node: weight} rounded to 4 decimal places.
    """
    raw = {}
    for node, p99 in p99_map.items():
        if p99 is None or p99 <= 0:
            raw[node] = 0.0
        else:
            raw[node] = 1.0 / p99

    total = sum(raw.values())
    n     = len(NODES)

    if total == 0:
        # Fallback: equal weights
        return {node: 1.0 for node in NODES}

    normalized = {
        node: round((raw[node] / total) * n, 4)
        for node in NODES
    }
    return normalized

def apply_ewa(prev_weights, new_weights, alpha):
    """
    Smooth weights using Exponentially Weighted Average.
    w_new = alpha * new + (1 - alpha) * prev
    """
    if prev_weights is None:
        return new_weights
    return {
        node: round(alpha * new_weights[node] + (1 - alpha) * prev_weights[node], 4)
        for node in NODES
    }

# ── Sampling loop ────────────────────────────────────────

def run_sampler(duration_sec, interval_sec):
    """
    Samples p99 + computes weights every interval_sec for duration_sec.
    Returns list of epoch records.
    """
    epochs       = duration_sec // interval_sec
    all_records  = []
    prev_weights = None

    print(f"\n{'='*65}")
    print(f"  WEIGHT SAMPLER — moderate profile (5× spread)")
    print(f"  Sampling every {interval_sec}s for {duration_sec}s "
          f"({epochs} epochs total)")
    print(f"  EWA alpha = {EWA_ALPHA}")
    print(f"{'='*65}")

    # Print header
    header = f"  {'Ep':>3}  {'Node':<8} {'p99 (ms)':>10} "
    header += f"{'Raw weight':>11} {'EWA weight':>11}"
    print(f"\n{header}")
    print(f"  {'-'*3}  {'-'*8} {'-'*10} {'-'*11} {'-'*11}")

    for epoch in range(1, epochs + 1):
        # Read p99 from all nodes
        p99_map = {}
        for node in NODES:
            text        = fetch_metrics(METRICS_PORTS[node])
            p99_map[node] = parse_p99(text)

        # Compute raw weights
        raw_weights = compute_weights(p99_map)

        # Apply EWA smoothing
        ewa_weights = apply_ewa(prev_weights, raw_weights, EWA_ALPHA)
        prev_weights = ewa_weights

        # Print this epoch
        for i, node in enumerate(NODES):
            p99_str = f"{p99_map[node]:.3f}" if p99_map[node] is not None else "N/A"
            raw_str = f"{raw_weights[node]:.4f}"
            ewa_str = f"{ewa_weights[node]:.4f}"
            ep_col  = str(epoch) if i == 0 else ""
            print(f"  {ep_col:>3}  {node:<8} {p99_str:>10} "
                  f"{raw_str:>11} {ewa_str:>11}")

        print()   # blank line between epochs

        # Save record
        record = {
            "epoch":      epoch,
            "timestamp":  datetime.utcnow().isoformat(),
            "p99_ms":     p99_map,
            "raw_weight": raw_weights,
            "ewa_weight": ewa_weights,
        }
        all_records.append(record)

        # Wait for next interval (unless last epoch)
        if epoch < epochs:
            print(f"  ⏳ Next sample in {interval_sec}s...\n")
            time.sleep(interval_sec)

    return all_records

# ── Summary table ────────────────────────────────────────

def print_summary(all_records):
    """Print a clean per-node summary across all epochs."""
    from collections import defaultdict
    node_p99    = defaultdict(list)
    node_weight = defaultdict(list)

    for rec in all_records:
        for node in NODES:
            if rec["p99_ms"][node] is not None:
                node_p99[node].append(rec["p99_ms"][node])
            node_weight[node].append(rec["ewa_weight"][node])

    print(f"\n{'='*65}")
    print(f"  SUMMARY — Average over {len(all_records)} epochs")
    print(f"{'='*65}")
    print(f"  {'Node':<8} {'Avg p99 (ms)':>14} {'Avg EWA weight':>16} "
          f"{'Expected':>10}")
    print(f"  {'-'*8} {'-'*14} {'-'*16} {'-'*10}")

    for node in NODES:
        avg_p99    = round(sum(node_p99[node])    / len(node_p99[node]),    3) \
                     if node_p99[node] else None
        avg_weight = round(sum(node_weight[node]) / len(node_weight[node]), 4) \
                     if node_weight[node] else None

        p99_str    = f"{avg_p99:.3f} ms"    if avg_p99    is not None else "N/A"
        weight_str = f"{avg_weight:.4f}"    if avg_weight is not None else "N/A"

        # Expected: node1/2/3 (1ms) should have higher weight than node4/5 (5ms)
        if node in ["node1", "node2", "node3"]:
            expected = "HIGH (fast node)"
        else:
            expected = "LOW  (slow node)"

        print(f"  {node:<8} {p99_str:>14} {weight_str:>16} {expected:>10}")

    print(f"{'='*65}")
    print(f"  ✅ Fast nodes (node1/2/3) should show HIGHER weight")
    print(f"  ✅ Slow nodes (node4/5)   should show LOWER  weight")
    print(f"{'='*65}\n")

# ── Save results ─────────────────────────────────────────

def save_results(all_records):
    out_path = os.path.join(SCRIPT_DIR, "weight-sampler-results.json")
    with open(out_path, "w") as f:
        json.dump(all_records, f, indent=2)
    print(f"  📄 Results saved to: {out_path}")
    return out_path

# ── Main ─────────────────────────────────────────────────

def main():
    duration = TOTAL_DURATION_SEC
    interval = SAMPLE_INTERVAL_SEC

    # Allow quick test mode: python weight_sampler.py 60 10
    if len(sys.argv) == 3:
        duration = int(sys.argv[1])
        interval = int(sys.argv[2])

    all_records = run_sampler(duration, interval)
    print_summary(all_records)
    out_path = save_results(all_records)
    print(f"  Run complete. Use weight_sampler_results.json for Task 3 plotting.\n")

if __name__ == "__main__":
    main()