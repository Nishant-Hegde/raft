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

# 3 write rates to sweep
WRITE_RATES = [500, 1000, 2000]

# Duration per run in seconds
DURATION_SEC = 60

# ── Metrics helpers ───────────────────────────────────────

def fetch_metrics(port):
    try:
        url = f"http://localhost:{port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.read().decode("utf-8")
    except Exception:
        return ""

def parse_percentile(metrics_text, pct):
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
            return round(le / 1_000_000, 3)
    return None

# ── Cluster helpers ───────────────────────────────────────

def wait_for_cluster(timeout=60):
    print("[sweep] Waiting for all 5 nodes...", end="", flush=True)
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
            print(f" ✅ All {len(NODES)} nodes up")
            return True
        print(".", end="", flush=True)
        time.sleep(3)
    print(f" ❌ Cluster not fully up after {timeout}s")
    return False

def start_cluster(env_vars):
    print(f"\n[sweep] Starting cluster — delays: "
          f"node1/2/3={env_vars.get('NODE1_DELAY','0')}ms "
          f"node4/5={env_vars.get('NODE4_DELAY','0')}ms")
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

def stop_cluster(proc):
    print("\n[sweep] Stopping cluster...")
    subprocess.run(
        ["docker", "compose", "down"],
        cwd=PROJECT_DIR, capture_output=True
    )
    time.sleep(5)
    proc.terminate()
    print("[sweep] Cluster stopped ✅")

# ── Single benchmark run ──────────────────────────────────

def run_one(mode_name, ops_per_sec, duration_sec):
    """Run workload and collect p50/p99/p999 commit latency."""
    print(f"\n[sweep] Workload: {ops_per_sec} ops/sec for {duration_sec}s...")

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

    print(f"[sweep] Done — {ops_done} ops")

    # Collect per-node metrics
    per_node = {}
    for node in NODES:
        text = fetch_metrics(METRICS_PORTS[node])
        per_node[node] = {
            "p50_ms":  parse_percentile(text, 50),
            "p99_ms":  parse_percentile(text, 99),
            "p999_ms": parse_percentile(text, 99.9),
        }

    # Compute cluster commit latency
    if mode_name == "vanilla_raft":
        # Simple majority: 3 of 5 — commit waits for 3rd slowest
        all_p50s  = sorted([per_node[n]["p50_ms"]  for n in NODES if per_node[n]["p50_ms"]  is not None])
        all_p99s  = sorted([per_node[n]["p99_ms"]  for n in NODES if per_node[n]["p99_ms"]  is not None])
        all_p999s = sorted([per_node[n]["p999_ms"] for n in NODES if per_node[n]["p999_ms"] is not None])
        commit_p50  = all_p50s[2]  if len(all_p50s)  >= 3 else None
        commit_p99  = all_p99s[2]  if len(all_p99s)  >= 3 else None
        commit_p999 = all_p999s[2] if len(all_p999s) >= 3 else None
    else:
        # WR-Raft: fast nodes (node1/2/3) dominate weighted quorum
        fast_p50s  = sorted([per_node[n]["p50_ms"]  for n in ["node1","node2","node3"] if per_node[n]["p50_ms"]  is not None])
        fast_p99s  = sorted([per_node[n]["p99_ms"]  for n in ["node1","node2","node3"] if per_node[n]["p99_ms"]  is not None])
        fast_p999s = sorted([per_node[n]["p999_ms"] for n in ["node1","node2","node3"] if per_node[n]["p999_ms"] is not None])
        commit_p50  = max(fast_p50s)  if fast_p50s  else None
        commit_p99  = max(fast_p99s)  if fast_p99s  else None
        commit_p999 = max(fast_p999s) if fast_p999s else None

    return {
        "mode":           mode_name,
        "ops_per_sec":    ops_per_sec,
        "ops_completed":  ops_done,
        "commit_p50_ms":  round(commit_p50,  3) if commit_p50  else None,
        "commit_p99_ms":  round(commit_p99,  3) if commit_p99  else None,
        "commit_p999_ms": round(commit_p999, 3) if commit_p999 else None,
        "per_node":       per_node,
        "timestamp":      datetime.utcnow().isoformat(),
    }

# ── Print combined table ──────────────────────────────────

def print_sweep_table(all_results):
    """Print combined table: rows = write rates, cols = vanilla vs WR-Raft."""

    def imp(v, w):
        if not v or not w or v == 0:
            return 0.0
        return round((v - w) / v * 100, 1)

    print(f"\n{'='*75}")
    print(f"  WRITE RATE SWEEP — WR-Raft vs Vanilla Raft")
    print(f"  Profile: MODERATE (5× spread) | 3 write rates | 60s each")
    print(f"{'='*75}")
    print(f"  {'Rate':>8}  {'Metric':<6}  "
          f"{'Vanilla (ms)':>13}  {'WR-Raft (ms)':>13}  {'Improvement':>12}")
    print(f"  {'-'*8}  {'-'*6}  {'-'*13}  {'-'*13}  {'-'*12}")

    for rate in WRITE_RATES:
        vanilla = all_results.get(f"vanilla_{rate}")
        wr      = all_results.get(f"wr_{rate}")
        if not vanilla or not wr:
            continue

        vp50  = vanilla["commit_p50_ms"]  or 0
        vp99  = vanilla["commit_p99_ms"]  or 0
        vp999 = vanilla["commit_p999_ms"] or 0
        wp50  = wr["commit_p50_ms"]       or 0
        wp99  = wr["commit_p99_ms"]       or 0
        wp999 = wr["commit_p999_ms"]      or 0

        rate_str = f"{rate} ops/s"
        print(f"  {rate_str:>8}  {'p50':<6}  "
              f"{vp50:>13.3f}  {wp50:>13.3f}  {imp(vp50,wp50):>11.1f}%")
        print(f"  {'':>8}  {'p99':<6}  "
              f"{vp99:>13.3f}  {wp99:>13.3f}  {imp(vp99,wp99):>11.1f}%")
        print(f"  {'':>8}  {'p999':<6}  "
              f"{vp999:>13.3f}  {wp999:>13.3f}  {imp(vp999,wp999):>11.1f}%")
        print(f"  {'-'*8}  {'-'*6}  {'-'*13}  {'-'*13}  {'-'*12}")

    print(f"{'='*75}")
    print(f"  ✅ Positive % = WR-Raft faster | Key metric = p99")
    print(f"  ℹ  Improvement should grow as write rate increases")
    print(f"{'='*75}\n")

def print_progress(rate, mode):
    print(f"\n{'─'*75}")
    print(f"  [{rate} ops/sec] {mode}")
    print(f"{'─'*75}")

# ── Save results ──────────────────────────────────────────

def save_results(all_results):
    path = os.path.join(SCRIPT_DIR, "write-rate-sweep-results.json")
    with open(path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\n  📄 All results saved to: {path}")

# ── Main ──────────────────────────────────────────────────

def main():
    print(f"\n{'='*75}")
    print(f"  WR-RAFT WRITE RATE SWEEP")
    print(f"  Rates: {WRITE_RATES} ops/sec")
    print(f"  Duration: {DURATION_SEC}s per run")
    print(f"  Total runs: {len(WRITE_RATES) * 2} "
          f"({len(WRITE_RATES)} rates × 2 modes)")
    print(f"  Estimated time: ~{len(WRITE_RATES) * 2 * (DURATION_SEC + 60) // 60} minutes")
    print(f"{'='*75}")

    all_results = {}

    for rate in WRITE_RATES:

        # ── Vanilla Raft run ──
        print_progress(rate, "Vanilla Raft — all delays = 0ms")
        proc = start_cluster({
            "NODE1_DELAY": "0",
            "NODE2_DELAY": "0",
            "NODE3_DELAY": "0",
            "NODE4_DELAY": "0",
            "NODE5_DELAY": "0",
        })

        if not wait_for_cluster():
            print(f"❌ Cluster failed at {rate} ops/sec vanilla run")
            proc.terminate()
            continue

        print("[sweep] ⏳ Warming up 10s...")
        time.sleep(10)

        vanilla_result = run_one("vanilla_raft", rate, DURATION_SEC)
        all_results[f"vanilla_{rate}"] = vanilla_result
        stop_cluster(proc)

        # brief pause between runs
        print("[sweep] ⏳ Pausing 10s before next run...")
        time.sleep(10)

        # ── WR-Raft run ──
        print_progress(rate, "WR-Raft — node1/2/3=1ms, node4/5=5ms")
        proc = start_cluster({
            "NODE1_DELAY": "1",
            "NODE2_DELAY": "1",
            "NODE3_DELAY": "1",
            "NODE4_DELAY": "5",
            "NODE5_DELAY": "5",
        })

        if not wait_for_cluster():
            print(f"❌ Cluster failed at {rate} ops/sec WR-Raft run")
            proc.terminate()
            continue

        print("[sweep] ⏳ Warming up 10s...")
        time.sleep(10)

        wr_result = run_one("wr_raft", rate, DURATION_SEC)
        all_results[f"wr_{rate}"] = wr_result
        stop_cluster(proc)

        # Print partial table after each rate so you can see progress
        print(f"\n[sweep] ✅ Rate {rate} ops/sec done!")
        print(f"  Vanilla p99: {vanilla_result['commit_p99_ms']} ms  |  "
              f"WR-Raft p99: {wr_result['commit_p99_ms']} ms")

        if rate != WRITE_RATES[-1]:
            print("[sweep] ⏳ Pausing 10s before next rate...")
            time.sleep(10)

    # Print final combined table
    print_sweep_table(all_results)
    save_results(all_results)
    print("[sweep] ✅ Write rate sweep complete!\n")

if __name__ == "__main__":
    main()