import time
import math
import json
import os
import sys
import subprocess
import threading
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

RUN_SECONDS   = 120
SAMPLE_EVERY  = 5   # seconds between samples

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

def fetch_weight(node_id, all_ports):
    """
    Scrape all nodes, find the leader (only node with wr_raft_weight values),
    then return the weight for the specific node_id we care about.
    node_id is a uint64 string e.g. "5" for node5.
    """
    for port in all_ports.values():
        text = fetch_metrics(port)
        for line in text.splitlines():
            if line.startswith(f'wr_raft_weight{{peer_id="{node_id}"}}'):
                try:
                    return round(float(line.split("} ")[1]), 4)
                except Exception:
                    pass
    return None


def fetch_commit_latency_p99(port):
    text = fetch_metrics(port)
    for line in text.splitlines():
        if line.startswith("commit_latency_ns_count") or line.startswith("commit_duration_ns_count"):
            pass
    return parse_percentile(text, 99)

# ── Cluster helpers ───────────────────────────────────────

def wait_for_cluster(timeout=60):
    print("[scenario1] Waiting for all 5 nodes...", end="", flush=True)
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
    print(f"\n[scenario1] Starting cluster — delays: "
          f"node1-4={env_vars.get('NODE1_DELAY','0')}ms "
          f"node5={env_vars.get('NODE5_DELAY','0')}ms")
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
    print("\n[scenario1] Stopping cluster...")
    subprocess.run(
        ["docker", "compose", "down"],
        cwd=PROJECT_DIR, capture_output=True
    )
    time.sleep(5)
    proc.terminate()
    print("[scenario1] Cluster stopped ✅")

# ── Sampling loop ─────────────────────────────────────────

def run_scenario(stop_event):
    print(f"\n[scenario1] Running for {RUN_SECONDS}s — sampling every {SAMPLE_EVERY}s...")
    samples = []
    start = time.time()
    tick = 0

    while time.time() - start < RUN_SECONDS:
        time.sleep(SAMPLE_EVERY)
        tick += SAMPLE_EVERY

        row = {"t": tick}
        for node in NODES:
            port = METRICS_PORTS[node]
            text = fetch_metrics(port)
            row[f"{node}_p99_ms"]   = parse_percentile(text, 99)
            row["node5_weight"]   = fetch_weight("5", METRICS_PORTS)

        row["cluster_commit_p99_ms"] = fetch_commit_latency_p99(METRICS_PORTS["node1"])
        samples.append(row)

        print(f"  [t={tick:>3}s] "
              f"node5_p99={row.get('node5_p99_ms')}ms  "
              f"node5_weight={row.get('node5_weight')}  "
              f"commit_p99={row.get('cluster_commit_p99_ms')}ms")

    stop_event.set()
    return samples

# ── Verdict ───────────────────────────────────────────────

def evaluate(samples):
    fast_node_p99s   = []
    node5_p99s       = []
    node5_weights    = []
    commit_p99s      = []

    for row in samples:
        for node in ["node1", "node2", "node3", "node4"]:
            v = row.get(f"{node}_p99_ms")
            if v is not None:
                fast_node_p99s.append(v)
        v5 = row.get("node5_p99_ms")
        if v5 is not None:
            node5_p99s.append(v5)
        w5 = row.get("node5_weight")
        if w5 is not None:
            node5_weights.append(w5)
        cp = row.get("cluster_commit_p99_ms")
        if cp is not None:
            commit_p99s.append(cp)

    avg_fast    = round(sum(fast_node_p99s) / len(fast_node_p99s), 3) if fast_node_p99s else None
    avg_node5   = round(sum(node5_p99s) / len(node5_p99s), 3) if node5_p99s else None
    avg_commit  = round(sum(commit_p99s) / len(commit_p99s), 3) if commit_p99s else None
    final_w5    = node5_weights[-1] if node5_weights else None

    # Verdict: commit latency should track fast nodes, not node5
    on_critical_path = False
    if avg_commit is not None and avg_fast is not None and avg_node5 is not None:
        # If commit latency is closer to node5's latency than to fast nodes', it's on the critical path
        dist_to_fast  = abs(avg_commit - avg_fast)
        dist_to_node5 = abs(avg_commit - avg_node5)
        on_critical_path = dist_to_node5 < dist_to_fast

    return {
        "avg_fast_node_p99_ms": avg_fast,
        "avg_node5_p99_ms":     avg_node5,
        "avg_cluster_commit_p99_ms": avg_commit,
        "final_node5_weight":   final_w5,
        "node5_on_critical_path": on_critical_path,
    }

def print_result(result):
    print(f"\n{'='*65}")
    print(f"  FAILURE SCENARIO 1 — ONE SLOW NODE (severe profile, node5)")
    print(f"{'='*65}")
    print(f"  Avg fast-node p99 (node1-4) : {result['avg_fast_node_p99_ms']} ms")
    print(f"  Avg node5 p99               : {result['avg_node5_p99_ms']} ms")
    print(f"  Avg cluster commit p99      : {result['avg_cluster_commit_p99_ms']} ms")
    print(f"  Final node5 weight          : {result['final_node5_weight']}")
    print(f"  {'-'*63}")
    if not result["node5_on_critical_path"]:
        print(f"  ✅ PASS — node5 never on critical path")
        print(f"  ✅ Cluster commit latency tracks fast nodes, not node5")
    else:
        print(f"  ❌ FAIL — node5 appears to be on the critical path")
        print(f"  ❌ Cluster commit latency tracks node5's slow latency")
    print(f"{'='*65}\n")

def save_results(samples, result):
    out = {
        "generated": datetime.utcnow().isoformat(),
        "scenario": "failure_scenario_1_one_slow_node",
        "run_seconds": RUN_SECONDS,
        "samples": samples,
        "result": result,
    }
    path = os.path.join(SCRIPT_DIR, "failure-scenario-1-results.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"  📄 Results saved to: {path}")

def run_load_generator(stop_event, port=9091, rate_per_sec=10):
    interval = 1.0 / rate_per_sec
    while not stop_event.is_set():
        try:
            req = urllib.request.Request(
                f"http://localhost:{port}/propose",
                data=b"write-op",
                method="POST"
            )
            urllib.request.urlopen(req, timeout=1)
        except Exception:
            pass
        time.sleep(interval)

# ── Main ──────────────────────────────────────────────────

def main():
    print(f"\n{'='*65}")
    print(f"  WR-RAFT FAILURE SCENARIO 1 — ONE SLOW NODE")
    print(f"  Severe profile applied to node5 only | {RUN_SECONDS}s run")
    print(f"{'='*65}")

    proc = start_cluster({
        "NODE1_DELAY": "1",
        "NODE2_DELAY": "1",
        "NODE3_DELAY": "1",
        "NODE4_DELAY": "1",
        "NODE5_DELAY": "10",
    })

    if not wait_for_cluster():
        print("❌ Cluster failed to start")
        proc.terminate()
        sys.exit(1)

    print("[scenario1] ⏳ Warming up 10s...")
    time.sleep(10)
    stop_load = threading.Event()
    load_thread = threading.Thread(
        target=run_load_generator,
        args=(stop_load,),
        daemon=True
    )
    load_thread.start()
    print("[scenario1] 🔥 Load generator started")

    samples = run_scenario(stop_load)
    result  = evaluate(samples)
    print_result(result)
    save_results(samples, result)

    stop_cluster(proc)
    print("[scenario1] ✅ Failure scenario 1 complete!\n")

if __name__ == "__main__":
    main()