import time
import sys
import os
import urllib.request
import math
import json
from datetime import datetime

# Add project root to path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, SCRIPT_DIR)

from config import (
    LATENCY_PROFILE, TEST_DURATION_SEC, WRITE_RATE_OPS,
    NODES, METRICS_PORTS
)
from injector_bridge import apply_profile, reset_all, verify_profile

# ─────────────────────────────────────────────
# Metrics helpers
# ─────────────────────────────────────────────

def fetch_metrics(port):
    try:
        url = f"http://localhost:{port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        return ""

def parse_p99(metrics_text):
    """Parse fsync_duration_ns histogram → p99 in ms."""
    buckets = {}
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
            return round(le / 1_000_000, 3)
    return None

def parse_p50(metrics_text):
    """Parse fsync_duration_ns histogram → p50 in ms."""
    buckets = {}
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

    target = 0.50 * total_count
    for le in sorted(k for k in buckets if k != math.inf):
        if buckets[le] >= target:
            return round(le / 1_000_000, 3)
    return None

# ─────────────────────────────────────────────
# Cluster health check
# ─────────────────────────────────────────────

def wait_for_cluster(timeout=30):
    """Wait until all 5 nodes are reachable on their metrics port."""
    print("\n[Harness] Checking cluster is up...", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        up = 0
        for node in NODES:
            port = METRICS_PORTS[node]
            try:
                urllib.request.urlopen(f"http://localhost:{port}/metrics", timeout=2)
                up += 1
            except Exception:
                pass
        if up == len(NODES):
            print(f" ✅ All {len(NODES)} nodes up")
            return True
        print(".", end="", flush=True)
        time.sleep(2)
    print(f" ❌ Only {up}/{len(NODES)} nodes reachable after {timeout}s")
    return False

# ─────────────────────────────────────────────
# Test workload (simple write simulator)
# ─────────────────────────────────────────────

def run_workload(duration_sec, ops_per_sec):
    """
    Simulates write workload by polling metrics endpoints.
    In a real setup this would send gRPC writes — here we
    just keep sampling metrics so Prometheus data accumulates.
    """
    print(f"\n[Harness] Running workload for {duration_sec}s "
          f"at ~{ops_per_sec} ops/sec simulation...")

    interval = 1.0 / ops_per_sec if ops_per_sec > 0 else 0.01
    end_time = time.time() + duration_sec
    ops_done = 0

    while time.time() < end_time:
        # Poll a random node's metrics to keep data flowing
        node = NODES[ops_done % len(NODES)]
        fetch_metrics(METRICS_PORTS[node])
        ops_done += 1
        remaining = end_time - time.time()
        if remaining <= 0:
            break
        time.sleep(min(interval, remaining))

    print(f"[Harness] Workload complete — {ops_done} polling ops done")

# ─────────────────────────────────────────────
# Results collection
# ─────────────────────────────────────────────

def collect_results(profile_name):
    """Read p50 + p99 from all nodes and return as a list of dicts."""
    print("\n[Harness] Collecting final metrics from all nodes...")
    results = []
    for node in NODES:
        port = METRICS_PORTS[node]
        text = fetch_metrics(port)
        p50 = parse_p50(text)
        p99 = parse_p99(text)
        results.append({
            "node":       node,
            "profile":    profile_name,
            "p50_ms":     p50,
            "p99_ms":     p99,
            "timestamp":  datetime.utcnow().isoformat()
        })
    return results

def print_results_table(results):
    print(f"\n{'='*60}")
    print(f"  HARNESS RESULT — Profile: {results[0]['profile'].upper()}")
    print(f"{'='*60}")
    print(f"  {'Node':<8} {'p50 (ms)':>12} {'p99 (ms)':>12}")
    print(f"  {'-'*8} {'-'*12} {'-'*12}")
    for r in results:
        p50_str = f"{r['p50_ms']:.3f} ms" if r['p50_ms'] is not None else "N/A"
        p99_str = f"{r['p99_ms']:.3f} ms" if r['p99_ms'] is not None else "N/A"
        print(f"  {r['node']:<8} {p50_str:>12} {p99_str:>12}")
    print(f"{'='*60}\n")

def save_results(results, profile_name):
    out_dir  = os.path.join(PROJECT_DIR, "harness")
    out_path = os.path.join(out_dir, f"result-{profile_name}-{int(time.time())}.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[Harness] 📄 Results saved to: {out_path}")

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"  WR-RAFT TEST HARNESS")
    print(f"  Profile       : {LATENCY_PROFILE}")
    print(f"  Test duration : {TEST_DURATION_SEC}s")
    print(f"  Write rate    : {WRITE_RATE_OPS} ops/sec")
    print(f"{'='*60}")

    # Step 1 — Check cluster is up
    if not wait_for_cluster():
        print("[Harness] ❌ Cluster not reachable. Start it first with: docker compose up")
        sys.exit(1)

    # Step 2 — Apply latency profile (setup)
    ok = apply_profile(LATENCY_PROFILE)
    if not ok:
        print("[Harness] ❌ Could not apply profile. Aborting.")
        sys.exit(1)

    # Step 3 — Show what's active
    verify_profile(LATENCY_PROFILE)

    # Step 4 — Wait a moment for netem rules to settle
    print(f"\n[Harness] ⏳ Waiting 5s for netem rules to take effect...")
    time.sleep(5)

    # Step 5 — Run workload
    try:
        run_workload(TEST_DURATION_SEC, WRITE_RATE_OPS)
    except KeyboardInterrupt:
        print("\n[Harness] ⚠️  Interrupted by user")

    # Step 6 — Collect results
    results = collect_results(LATENCY_PROFILE)
    print_results_table(results)
    save_results(results, LATENCY_PROFILE)

    # Step 7 — Reset (teardown) — always runs even if test failed
    reset_all()

    print("[Harness] ✅ Test complete.\n")

if __name__ == "__main__":
    main()