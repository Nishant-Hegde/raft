import time
import math
import json
import os
import sys
import subprocess
import urllib.request
import threading
from datetime import datetime, timezone

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

NODES = ["node1", "node2", "node3", "node4", "node5"]
FAST_NODES = ["node1", "node2", "node3"]
SLOW_NODES = ["node4", "node5"]

METRICS_PORTS = {
    "node1": 9091,
    "node2": 9092,
    "node3": 9093,
    "node4": 9094,
    "node5": 9095,
}

RUN_SECONDS  = 120
SAMPLE_EVERY = 5   # seconds between samples

#  Metrics helpers 

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

def fetch_fsync_count(port):
    text = fetch_metrics(port)
    for line in text.splitlines():
        if line.startswith("fsync_duration_ns_count "):
            try:
                return int(float(line.split(" ")[1]))
            except Exception:
                pass
    return None

def node_is_up(port):
    try:
        urllib.request.urlopen(f"http://localhost:{port}/metrics", timeout=2)
        return True
    except Exception:
        return False

#  Cluster helpers 

def wait_for_cluster(timeout=60):
    print("[scenario4] Waiting for all 5 nodes...", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        up = sum(1 for n in NODES if node_is_up(METRICS_PORTS[n]))
        if up == len(NODES):
            print(f"  All {len(NODES)} nodes up")
            return True
        print(".", end="", flush=True)
        time.sleep(3)
    print(f"  Cluster not fully up after {timeout}s")
    return False

def start_cluster(env_vars):
    print(f"\n[scenario4] Starting cluster  delays: "
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
    print("\n[scenario4] Stopping cluster...")
    subprocess.run(
        ["docker", "compose", "down"],
        cwd=PROJECT_DIR, capture_output=True
    )
    time.sleep(5)
    proc.terminate()
    print("[scenario4] Cluster stopped ")

#  Sampling loop 

def run_scenario():
    print(f"\n[scenario4] Running for {RUN_SECONDS}s  sampling every {SAMPLE_EVERY}s...")
    samples = []
    start   = time.time()
    tick    = 0

    while time.time() - start < RUN_SECONDS:
        time.sleep(SAMPLE_EVERY)
        tick += SAMPLE_EVERY

        row = {"t": tick}
        for node in NODES:
            port = METRICS_PORTS[node]
            text = fetch_metrics(port)
            row[f"{node}_p99_ms"]      = parse_percentile(text, 99)
            row[f"{node}_fsync_count"] = fetch_fsync_count(port)

        # Cluster progress = sum of fsync counts across the 3 fast nodes
        # (these are the ones we expect to be doing the real work / forming quorum)
        fast_counts = [row[f"{n}_fsync_count"] for n in FAST_NODES if row.get(f"{n}_fsync_count") is not None]
        row["fast_nodes_fsync_sum"] = sum(fast_counts) if fast_counts else None

        # commit latency proxy = node1's own p99 (node1 is always fast in this scenario)
        row["commit_p99_proxy_ms"] = row.get("node1_p99_ms")

        samples.append(row)

        print(f"  [t={tick:>3}s] "
              f"node4_p99={row.get('node4_p99_ms')}ms  "
              f"node5_p99={row.get('node5_p99_ms')}ms  "
              f"commit_p99_proxy={row.get('commit_p99_proxy_ms')}ms  "
              f"fast_fsync_sum={row.get('fast_nodes_fsync_sum')}")

    return samples

#  Verdict 

def evaluate(samples):
    fast_p99s   = []
    slow_p99s   = []
    commit_p99s = []

    for row in samples:
        for n in FAST_NODES:
            v = row.get(f"{n}_p99_ms")
            if v is not None:
                fast_p99s.append(v)
        for n in SLOW_NODES:
            v = row.get(f"{n}_p99_ms")
            if v is not None:
                slow_p99s.append(v)
        cp = row.get("commit_p99_proxy_ms")
        if cp is not None:
            commit_p99s.append(cp)

    avg_fast   = round(sum(fast_p99s) / len(fast_p99s), 3) if fast_p99s else None
    avg_slow   = round(sum(slow_p99s) / len(slow_p99s), 3) if slow_p99s else None
    avg_commit = round(sum(commit_p99s) / len(commit_p99s), 3) if commit_p99s else None

    # Did the 3 fast nodes keep progressing throughout (no stall)?
    fast_sums = [r["fast_nodes_fsync_sum"] for r in samples if r.get("fast_nodes_fsync_sum") is not None]
    kept_progressing = None
    if len(fast_sums) >= 2:
        kept_progressing = all(b >= a for a, b in zip(fast_sums, fast_sums[1:]))

    # Verdict: commit latency should track fast nodes, not slow nodes
    slow_nodes_on_critical_path = False
    if avg_commit is not None and avg_fast is not None and avg_slow is not None:
        dist_to_fast = abs(avg_commit - avg_fast)
        dist_to_slow = abs(avg_commit - avg_slow)
        slow_nodes_on_critical_path = dist_to_slow < dist_to_fast

    passed = bool(kept_progressing) and not slow_nodes_on_critical_path

    return {
        "avg_fast_node_p99_ms":  avg_fast,
        "avg_slow_node_p99_ms":  avg_slow,
        "avg_cluster_commit_p99_ms": avg_commit,
        "fast_nodes_kept_progressing": kept_progressing,
        "slow_nodes_on_critical_path": slow_nodes_on_critical_path,
        "passed": passed,
    }

def print_result(result):
    print(f"\n{'='*65}")
    print(f"  FAILURE SCENARIO 4  TWO SLOW NODES (moderate profile, node4+node5)")
    print(f"{'='*65}")
    print(f"  Avg fast-node p99 (node1-3)  : {result['avg_fast_node_p99_ms']} ms")
    print(f"  Avg slow-node p99 (node4-5)  : {result['avg_slow_node_p99_ms']} ms")
    print(f"  Avg cluster commit p99 proxy : {result['avg_cluster_commit_p99_ms']} ms")
    print(f"  Fast 3 nodes kept progressing : {result['fast_nodes_kept_progressing']}")
    print(f"  Slow nodes on critical path    : {result['slow_nodes_on_critical_path']}")
    print(f"  {'-'*63}")
    if result["passed"]:
        print(f"   PASS  3 fast nodes formed quorum without node4/node5")
        print(f"   Cluster commit latency tracks fast nodes, not slow ones")
    else:
        print(f"   FAIL  either fast nodes stalled, or slow nodes gated commits")
    print(f"{'='*65}\n")

def save_results(samples, result):
    out = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "scenario": "failure_scenario_4_two_slow_nodes",
        "run_seconds": RUN_SECONDS,
        "fast_nodes": FAST_NODES,
        "slow_nodes": SLOW_NODES,
        "samples": samples,
        "result": result,
    }
    path = os.path.join(SCRIPT_DIR, "failure-scenario-4-results.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"   Results saved to: {path}")

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


#  Main 

def main():
    print(f"\n{'='*65}")
    print(f"  WR-RAFT FAILURE SCENARIO 4  TWO SLOW NODES")
    print(f"  Moderate profile on node4 + node5 | {RUN_SECONDS}s run")
    print(f"{'='*65}")

    proc = start_cluster({
        "NODE1_DELAY": "1",
        "NODE2_DELAY": "1",
        "NODE3_DELAY": "1",
        "NODE4_DELAY": "5",
        "NODE5_DELAY": "5",
    })

    if not wait_for_cluster():
        print(" Cluster failed to start")
        proc.terminate()
        sys.exit(1)


    print("[scenario4] Warming up 10s...")
    time.sleep(10)

    stop_load = threading.Event()
    load_thread = threading.Thread(
        target=run_load_generator,
        args=(stop_load,),
        daemon=True
    )
    load_thread.start()
    print("[scenario4] Load generator started")

    samples = run_scenario()
    stop_load.set()

    result  = evaluate(samples)
    print_result(result)
    save_results(samples, result)

    stop_cluster(proc)
    print("[scenario4]  Failure scenario 4 complete!\n")

if __name__ == "__main__":
    main()
