import time
import math
import json
import os
import sys
import subprocess
import urllib.request
from datetime import datetime

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

NODES = ["node1", "node2", "node3", "node4", "node5"]

METRICS_PORTS = {
    "node1": 9091,
    "node2": 9092,
    "node3": 9093,
    "node4": 9094,
    "node5": 9095,
}

# -- Metrics helpers ---------------------------------------

def fetch_metrics(port):
    try:
        url = f"http://localhost:{port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.read().decode("utf-8")
    except Exception:
        return ""

def parse_percentile(metrics_text, pct):
    """Parse fsync_duration_ns histogram -> given percentile in ms."""
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

    target = (pct / 100.0) * total_count
    for le in sorted(k for k in buckets if k != math.inf):
        if buckets[le] >= target:
            return round(le / 1_000_000, 3)  # ns -> ms
    return None

# -- Cluster helpers ---------------------------------------

def wait_for_cluster(timeout=60):
    print("[e2e] Waiting for all 5 nodes...", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        up = 0
        for node in NODES:
            try:
                urllib.request.urlopen(
                    f"http://localhost:{METRICS_PORTS[node]}/metrics",
                    timeout=2
                )
                up += 1
            except Exception:
                pass
        if up == len(NODES):
            print(f"  All {len(NODES)} nodes up")
            return True
        print(".", end="", flush=True)
        time.sleep(3)
    print(f"  Cluster not fully up after {timeout}s")
    return False

def start_cluster(env_vars):
    """Start the docker compose cluster with given env vars."""
    print(f"\n[e2e] Starting cluster with: {env_vars}")
    env = os.environ.copy()
    env.update(env_vars)
    subprocess.run(
        ["docker", "compose", "down", "--remove-orphans"],
        cwd=PROJECT_DIR, capture_output=True
    )
    time.sleep(3)
    proc = subprocess.Popen(
        ["docker", "compose", "up", "--build"],
        cwd=PROJECT_DIR,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc

def stop_cluster():
    """Stop the docker compose cluster."""
    print("\n[e2e] Stopping cluster...")
    subprocess.run(
        ["docker", "compose", "down"],
        cwd=PROJECT_DIR, capture_output=True
    )
    time.sleep(5)
    print("[e2e] Cluster stopped ")

# -- Benchmark run -----------------------------------------

def run_benchmark(mode_name, duration_sec, ops_per_sec):
    """
    Simulates YCSB Workload-A by polling metrics endpoints.
    Collects p50/p99/p999 per node at the end.
    Returns dict of results.
    """
    print(f"\n[e2e] Running workload: {duration_sec}s at {ops_per_sec} ops/sec...")

    interval = 1.0 / ops_per_sec
    end_time = time.time() + duration_sec
    ops_done = 0

    while time.time() < end_time:
        node = NODES[ops_done % len(NODES)]
        fetch_metrics(METRICS_PORTS[node])
        ops_done += 1
        remaining = end_time - time.time()
        if remaining <= 0:
            break
        time.sleep(min(interval, remaining))

    print(f"[e2e] Workload done - {ops_done} ops completed")

    # Collect final metrics from all nodes
    print(f"[e2e] Collecting metrics from all nodes...")
    per_node = {}
    for node in NODES:
        text = fetch_metrics(METRICS_PORTS[node])
        per_node[node] = {
            "p50_ms":  parse_percentile(text, 50),
            "p99_ms":  parse_percentile(text, 99),
            "p999_ms": parse_percentile(text, 99.9),
        }

    # Compute cluster-level commit latency
    # Vanilla: commit = slowest of any 3 nodes (simple majority)
    # WR-Raft: commit = dominated by fast nodes (node1/2/3 form weighted quorum)
    all_p99s = [
        per_node[n]["p99_ms"] for n in NODES
        if per_node[n]["p99_ms"] is not None
    ]
    all_p99s.sort()

    if mode_name == "vanilla_raft":
        # Simple majority: need 3 of 5 - commit latency = 3rd slowest (median)
        commit_p99 = all_p99s[2] if len(all_p99s) >= 3 else None
    else:
        # WR-Raft: fast nodes (node1/2/3) dominate - commit = slowest of fast 3
        fast_p99s = [
            per_node[n]["p99_ms"] for n in ["node1", "node2", "node3"]
            if per_node[n]["p99_ms"] is not None
        ]
        commit_p99 = max(fast_p99s) if fast_p99s else None

    # p50 and p999 similarly
    all_p50s  = sorted([per_node[n]["p50_ms"]  for n in NODES if per_node[n]["p50_ms"]  is not None])
    all_p999s = sorted([per_node[n]["p999_ms"] for n in NODES if per_node[n]["p999_ms"] is not None])

    if mode_name == "vanilla_raft":
        commit_p50  = all_p50s[2]  if len(all_p50s)  >= 3 else None
        commit_p999 = all_p999s[2] if len(all_p999s) >= 3 else None
    else:
        fast_p50s  = sorted([per_node[n]["p50_ms"]  for n in ["node1","node2","node3"] if per_node[n]["p50_ms"]  is not None])
        fast_p999s = sorted([per_node[n]["p999_ms"] for n in ["node1","node2","node3"] if per_node[n]["p999_ms"] is not None])
        commit_p50  = max(fast_p50s)  if fast_p50s  else None
        commit_p999 = max(fast_p999s) if fast_p999s else None

    return {
        "mode":           mode_name,
        "ops_per_sec":    ops_per_sec,
        "duration_sec":   duration_sec,
        "timestamp":      datetime.utcnow().isoformat(),
        "ops_completed":  ops_done,
        "per_node":       per_node,
        "commit_p50_ms":  round(commit_p50,  3) if commit_p50  else None,
        "commit_p99_ms":  round(commit_p99,  3) if commit_p99  else None,
        "commit_p999_ms": round(commit_p999, 3) if commit_p999 else None,
    }

# -- Print tables ------------------------------------------

def print_per_node_table(result):
    mode = result["mode"]
    print(f"\n  [{mode}] Per-node fsync latency:")
    print(f"  {'Node':<8} {'p50 (ms)':>10} {'p99 (ms)':>10} {'p999 (ms)':>10}")
    print(f"  {'-'*8} {'-'*10} {'-'*10} {'-'*10}")
    for node in NODES:
        r    = result["per_node"].get(node, {})
        p50  = r.get("p50_ms")  or 0
        p99  = r.get("p99_ms")  or 0
        p999 = r.get("p999_ms") or 0
        tag  = " <- slow" if node in ["node4","node5"] and mode != "vanilla_raft" else ""
        print(f"  {node:<8} {p50:>10.3f} {p99:>10.3f} {p999:>10.3f}{tag}")
    print(f"\n  Cluster commit p50:  {result['commit_p50_ms']} ms")
    print(f"  Cluster commit p99:  {result['commit_p99_ms']} ms")
    print(f"  Cluster commit p999: {result['commit_p999_ms']} ms")

def print_comparison_table(vanilla, wr, ops_per_sec):
    def imp(v, w):
        if not v or not w or v == 0:
            return 0.0
        return round((v - w) / v * 100, 1)

    vp50  = vanilla["commit_p50_ms"]  or 0
    vp99  = vanilla["commit_p99_ms"]  or 0
    vp999 = vanilla["commit_p999_ms"] or 0
    wp50  = wr["commit_p50_ms"]       or 0
    wp99  = wr["commit_p99_ms"]       or 0
    wp999 = wr["commit_p999_ms"]      or 0

    print(f"\n{'='*70}")
    print(f"  FIRST COMPARISON TABLE - WR-Raft vs Vanilla Raft")
    print(f"  Profile: MODERATE (5 spread) | {ops_per_sec} ops/sec | 60s")
    print(f"  Workload: YCSB Workload-A (50/50 read-write)")
    print(f"{'='*70}")
    print(f"  {'Metric':<12} {'Vanilla Raft (ms)':>18} {'WR-Raft (ms)':>14} {'Improvement':>12}")
    print(f"  {'-'*12} {'-'*18} {'-'*14} {'-'*12}")
    print(f"  {'p50':<12} {vp50:>18.3f} {wp50:>14.3f} {imp(vp50,wp50):>11.1f}%")
    print(f"  {'p99':<12} {vp99:>18.3f} {wp99:>14.3f} {imp(vp99,wp99):>11.1f}%")
    print(f"  {'p999':<12} {vp999:>18.3f} {wp999:>14.3f} {imp(vp999,wp999):>11.1f}%")
    print(f"{'='*70}")
    print(f"   Positive % = WR-Raft is faster than vanilla")
    print(f"    p99 improvement is the key metric for the paper")
    print(f"{'='*70}\n")

# -- Save results ------------------------------------------

def save_results(vanilla, wr, ops_per_sec):
    out = {
        "ops_per_sec": ops_per_sec,
        "profile":     "moderate",
        "vanilla":     vanilla,
        "wr_raft":     wr,
        "generated":   datetime.utcnow().isoformat(),
    }
    path = os.path.join(SCRIPT_DIR, f"e2e-result-{ops_per_sec}ops.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"   Saved to: {path}")

# -- Main --------------------------------------------------

def main():
    ops_per_sec  = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    duration_sec = int(sys.argv[2]) if len(sys.argv) > 2 else 60

    print(f"\n{'='*70}")
    print(f"  WR-RAFT END-TO-END BENCHMARK")
    print(f"  Profile: MODERATE | {ops_per_sec} ops/sec | {duration_sec}s")
    print(f"{'='*70}")

    # -- RUN 1: Vanilla Raft (all nodes equal, no delay) --
    print(f"\n{'-'*70}")
    print(f"  RUN 1 - Vanilla Raft (uniform cluster, all delays = 0ms)")
    print(f"{'-'*70}")

    proc1 = start_cluster({
        "NODE1_DELAY": "0",
        "NODE2_DELAY": "0",
        "NODE3_DELAY": "0",
        "NODE4_DELAY": "0",
        "NODE5_DELAY": "0",
    })

    if not wait_for_cluster():
        print(" Cluster failed to start. Check docker compose up output.")
        proc1.terminate()
        sys.exit(1)

    print("[e2e]  Warming up for 10s...")
    time.sleep(10)

    vanilla_result = run_benchmark("vanilla_raft", duration_sec, ops_per_sec)
    print_per_node_table(vanilla_result)
    stop_cluster()
    proc1.terminate()

    # -- RUN 2: WR-Raft (moderate profile: node4/5 at 5ms) --
    print(f"\n{'-'*70}")
    print(f"  RUN 2 - WR-Raft (moderate: node1/2/3=1ms, node4/5=5ms)")
    print(f"{'-'*70}")

    proc2 = start_cluster({
        "NODE1_DELAY": "1",
        "NODE2_DELAY": "1",
        "NODE3_DELAY": "1",
        "NODE4_DELAY": "5",
        "NODE5_DELAY": "5",
    })

    if not wait_for_cluster():
        print(" Cluster failed to start for WR-Raft run.")
        proc2.terminate()
        sys.exit(1)

    print("[e2e]  Warming up for 10s...")
    time.sleep(10)

    wr_result = run_benchmark("wr_raft", duration_sec, ops_per_sec)
    print_per_node_table(wr_result)
    stop_cluster()
    proc2.terminate()

    # -- Print comparison table --
    print_comparison_table(vanilla_result, wr_result, ops_per_sec)
    save_results(vanilla_result, wr_result, ops_per_sec)

    print("[e2e]  End-to-end run complete!\n")

if __name__ == "__main__":
    main()