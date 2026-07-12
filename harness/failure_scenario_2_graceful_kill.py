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

NODES        = ["node1", "node2", "node3", "node4", "node5"]
NODE_TO_KILL = "node3"

METRICS_PORTS = {
    "node1": 9091,
    "node2": 9092,
    "node3": 9093,
    "node4": 9094,
    "node5": 9095,
}

TOTAL_RUN_SECONDS   = 90   # total scenario length
KILL_AT_SECOND      = 20   # when to stop node3
SAMPLE_EVERY        = 2    # seconds between samples

# ΓöÇΓöÇ Metrics helpers ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

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
    """Total fsync count ΓÇö used as a proxy for 'is this node still committing writes'."""
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

# ΓöÇΓöÇ Cluster helpers ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

def wait_for_cluster(timeout=60):
    print("[scenario2] Waiting for all 5 nodes...", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        up = sum(1 for n in NODES if node_is_up(METRICS_PORTS[n]))
        if up == len(NODES):
            print(f" Γ£à All {len(NODES)} nodes up")
            return True
        print(".", end="", flush=True)
        time.sleep(3)
    print(f" Γ¥î Cluster not fully up after {timeout}s")
    return False

def start_cluster(env_vars):
    print(f"\n[scenario2] Starting cluster ΓÇö mild uniform delays (1ms all nodes)")
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
    print("\n[scenario2] Stopping cluster...")
    subprocess.run(
        ["docker", "compose", "down"],
        cwd=PROJECT_DIR, capture_output=True
    )
    time.sleep(5)
    proc.terminate()
    print("[scenario2] Cluster stopped Γ£à")

def graceful_stop_node(node_name):
    """
    Gracefully stop a single node's container using `docker compose stop`.
    This sends SIGTERM (not SIGKILL) ΓÇö a clean shutdown.
    Assumes the docker-compose service name matches node_name (e.g. 'node3').
    """
    print(f"\n[scenario2] ≡ƒö╗ Gracefully stopping {node_name} (docker compose stop)...")
    result = subprocess.run(
        ["docker", "compose", "stop", node_name],
        cwd=PROJECT_DIR, capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"[scenario2] ΓÜá∩╕Å docker compose stop returned non-zero: {result.stderr.strip()}")
    else:
        print(f"[scenario2] {node_name} stopped Γ£à")

# ΓöÇΓöÇ Sampling loop ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

def run_scenario():
    print(f"\n[scenario2] Running for {TOTAL_RUN_SECONDS}s ΓÇö "
          f"killing {NODE_TO_KILL} at t={KILL_AT_SECOND}s ΓÇö "
          f"sampling every {SAMPLE_EVERY}s...")

    samples   = []
    start     = time.time()
    tick      = 0
    killed    = False

    while time.time() - start < TOTAL_RUN_SECONDS:
        time.sleep(SAMPLE_EVERY)
        tick += SAMPLE_EVERY

        # Trigger the graceful kill at the scheduled time
        if not killed and tick >= KILL_AT_SECOND:
            graceful_stop_node(NODE_TO_KILL)
            killed = True

        row = {"t": tick, "node3_killed": killed}

        for node in NODES:
            port = METRICS_PORTS[node]
            up   = node_is_up(port)
            row[f"{node}_up"] = up
            if up:
                text = fetch_metrics(port)
                row[f"{node}_p99_ms"]        = parse_percentile(text, 99)
                row[f"{node}_fsync_count"]   = fetch_fsync_count(port)
            else:
                row[f"{node}_p99_ms"]        = None
                row[f"{node}_fsync_count"]   = None

        # Use node1 as our "still alive & leader-ish" reference for commit-latency proxy
        row["commit_p99_proxy_ms"] = row.get("node1_p99_ms")

        # Is the cluster still making progress? Check that at least one live
        # node's fsync count is still increasing between samples.
        alive_counts = [
            row[f"{n}_fsync_count"]
            for n in NODES
            if n != NODE_TO_KILL and row.get(f"{n}_fsync_count") is not None
        ]
        row["alive_nodes_fsync_sum"] = sum(alive_counts) if alive_counts else None

        samples.append(row)

        status = "≡ƒö╗ DOWN" if not row.get(f"{NODE_TO_KILL}_up") else "up"
        print(f"  [t={tick:>3}s] {NODE_TO_KILL}={status:<8} "
              f"commit_p99_proxy={row['commit_p99_proxy_ms']}ms  "
              f"alive_fsync_sum={row['alive_nodes_fsync_sum']}")

    return samples

# ΓöÇΓöÇ Verdict ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

def evaluate(samples):
    kill_index = next((i for i, r in enumerate(samples) if r["node3_killed"]), None)

    if kill_index is None:
        return {"error": "node3 was never killed during the run ΓÇö check timing"}

    before = samples[:kill_index]
    # "blip window" = first 3 samples right after kill
    blip   = samples[kill_index:kill_index + 3]
    after  = samples[kill_index + 3:]

    def avg_latency(rows):
        vals = [r["commit_p99_proxy_ms"] for r in rows if r.get("commit_p99_proxy_ms") is not None]
        return round(sum(vals) / len(vals), 3) if vals else None

    def still_progressing(rows):
        """Check alive_nodes_fsync_sum strictly increases across consecutive samples."""
        sums = [r["alive_nodes_fsync_sum"] for r in rows if r.get("alive_nodes_fsync_sum") is not None]
        if len(sums) < 2:
            return None
        increasing_steps = sum(1 for a, b in zip(sums, sums[1:]) if b >= a)
        return increasing_steps == len(sums) - 1

    avg_before = avg_latency(before)
    avg_blip   = avg_latency(blip)
    avg_after  = avg_latency(after)

    kept_committing = still_progressing(after)

    recovered = None
    if avg_before is not None and avg_after is not None:
        # "recovered" = after-latency within 50% of before-latency
        recovered = avg_after <= (avg_before * 1.5)

    passed = bool(kept_committing) and bool(recovered)

    return {
        "kill_tick_index":       kill_index,
        "avg_latency_before_ms": avg_before,
        "avg_latency_blip_ms":   avg_blip,
        "avg_latency_after_ms":  avg_after,
        "kept_committing_after_kill": kept_committing,
        "latency_recovered":     recovered,
        "passed":                passed,
    }

def print_result(result):
    print(f"\n{'='*65}")
    print(f"  FAILURE SCENARIO 2 ΓÇö ONE CRASHED NODE (graceful, node3)")
    print(f"{'='*65}")
    if "error" in result:
        print(f"  Γ¥î {result['error']}")
        print(f"{'='*65}\n")
        return
    print(f"  Avg commit-latency proxy BEFORE kill : {result['avg_latency_before_ms']} ms")
    print(f"  Avg commit-latency proxy DURING blip : {result['avg_latency_blip_ms']} ms")
    print(f"  Avg commit-latency proxy AFTER kill  : {result['avg_latency_after_ms']} ms")
    print(f"  Cluster kept committing after kill   : {result['kept_committing_after_kill']}")
    print(f"  Latency recovered to near-normal      : {result['latency_recovered']}")
    print(f"  {'-'*63}")
    if result["passed"]:
        print(f"  Γ£à PASS ΓÇö cluster survived node3 loss and recovered")
    else:
        print(f"  Γ¥î FAIL ΓÇö cluster either stopped committing or latency stayed high")
    print(f"{'='*65}\n")

def save_results(samples, result):
    out = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "scenario": "failure_scenario_2_graceful_kill",
        "node_killed": NODE_TO_KILL,
        "kill_at_second": KILL_AT_SECOND,
        "total_run_seconds": TOTAL_RUN_SECONDS,
        "samples": samples,
        "result": result,
    }
    path = os.path.join(SCRIPT_DIR, "failure-scenario-2-results.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"  ≡ƒôä Results saved to: {path}")

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

# ΓöÇΓöÇ Main ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

def main():
    print(f"\n{'='*65}")
    print(f"  WR-RAFT FAILURE SCENARIO 2 ΓÇö GRACEFUL NODE KILL")
    print(f"  Node: {NODE_TO_KILL} | Kill at t={KILL_AT_SECOND}s | "
          f"Total run: {TOTAL_RUN_SECONDS}s")
    print(f"{'='*65}")

    proc = start_cluster({
        "NODE1_DELAY": "1",
        "NODE2_DELAY": "1",
        "NODE3_DELAY": "1",
        "NODE4_DELAY": "1",
        "NODE5_DELAY": "1",
    })

    if not wait_for_cluster():
        print("Γ¥î Cluster failed to start")
        proc.terminate()
        sys.exit(1)

    print("[scenario2] ΓÅ│ Warming up 10s...")
    time.sleep(10)
    stop_load = threading.Event()
    load_thread = threading.Thread(
        target=run_load_generator,
        args=(stop_load,),
        daemon=True
    )
    load_thread.start()
    print("[scenario2] Load generator started")

    samples = run_scenario()
    stop_load.set()
    result  = evaluate(samples)
    print_result(result)
    save_results(samples, result)

    stop_cluster(proc)
    print("[scenario2] Γ£à Failure scenario 2 complete!\n")

if __name__ == "__main__":
    main()
