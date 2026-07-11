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
NODE_TO_KILL       = "node2"              # docker-compose service name (used for restart)
NODE_2_CONTAINER   = "internb-raft-node2-1"  # actual container name (used for docker kill)

METRICS_PORTS = {
    "node1": 9091,
    "node2": 9092,
    "node3": 9093,
    "node4": 9094,
    "node5": 9095,
}

TOTAL_RUN_SECONDS   = 180  # total scenario length - extra buffer for slow container restart
KILL_AT_SECOND      = 20   # when to SIGKILL node2
DOWNTIME_SECONDS    = 30   # how long node2 stays dead
SAMPLE_EVERY        = 2    # seconds between samples

# RESTART_AT_SECOND = KILL_AT_SECOND + DOWNTIME_SECONDS = 50

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
    print("[scenario3] Waiting for all 5 nodes...", end="", flush=True)
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
    print(f"\n[scenario3] Starting cluster ΓÇö mild uniform delays (1ms all nodes)")
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
    print("\n[scenario3] Stopping cluster...")
    subprocess.run(
        ["docker", "compose", "down"],
        cwd=PROJECT_DIR, capture_output=True
    )
    time.sleep(5)
    proc.terminate()
    print("[scenario3] Cluster stopped Γ£à")

def sigkill_node(node_name):
    """Hard-kill a container ΓÇö SIGKILL, no graceful shutdown."""
    print(f"\n[scenario3] ≡ƒÆÑ SIGKILL {node_name} (docker kill)...")
    result = subprocess.run(
        ["docker", "kill", "--signal=SIGKILL", node_name],
        cwd=PROJECT_DIR, capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"[scenario3] ΓÜá∩╕Å docker kill returned non-zero: {result.stderr.strip()}")
    else:
        print(f"[scenario3] {node_name} killed Γ£à")

def restart_node(node_name):
    """Bring a killed container back up via docker compose start."""
    print(f"\n[scenario3] ≡ƒöü Restarting {node_name} (docker compose start)...")
    result = subprocess.run(
        ["docker", "compose", "start", node_name],
        cwd=PROJECT_DIR, capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"[scenario3] ΓÜá∩╕Å docker compose start returned non-zero: {result.stderr.strip()}")
    else:
        print(f"[scenario3] {node_name} restarted Γ£à")

# ΓöÇΓöÇ Sampling loop ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

def run_scenario():
    restart_at = KILL_AT_SECOND + DOWNTIME_SECONDS
    print(f"\n[scenario3] Running for {TOTAL_RUN_SECONDS}s ΓÇö "
          f"SIGKILL {NODE_TO_KILL} at t={KILL_AT_SECOND}s ΓÇö "
          f"restart at t={restart_at}s ΓÇö sampling every {SAMPLE_EVERY}s...")

    samples = []
    start   = time.time()
    tick    = 0
    killed  = False
    restarted = False

    while time.time() - start < TOTAL_RUN_SECONDS:
        time.sleep(SAMPLE_EVERY)
        tick += SAMPLE_EVERY

        if not killed and tick >= KILL_AT_SECOND:
            sigkill_node(NODE_2_CONTAINER)
            killed = True

        if killed and not restarted and tick >= restart_at:
            restart_node(NODE_TO_KILL)
            restarted = True

        row = {"t": tick, "node2_killed": killed, "node2_restarted": restarted}

        for node in NODES:
            port = METRICS_PORTS[node]
            up   = node_is_up(port)
            row[f"{node}_up"] = up
            row[f"{node}_fsync_count"] = fetch_fsync_count(port) if up else None
            row[f"{node}_p99_ms"]      = parse_percentile(fetch_metrics(port), 99) if up else None

        # cluster progress = sum of fsync counts across the 4 nodes that were never killed
        always_up_nodes = [n for n in NODES if n != NODE_TO_KILL]
        cluster_counts = [row[f"{n}_fsync_count"] for n in always_up_nodes if row.get(f"{n}_fsync_count") is not None]
        row["cluster_fsync_sum"] = sum(cluster_counts) if cluster_counts else None

        row["commit_p99_proxy_ms"] = row.get("node1_p99_ms")

        samples.append(row)

        n2_status = "up" if row.get("node2_up") else "≡ƒÆÑ DOWN"
        print(f"  [t={tick:>3}s] node2={n2_status:<8} "
              f"node2_fsync={row.get('node2_fsync_count')}  "
              f"cluster_fsync_sum={row['cluster_fsync_sum']}  "
              f"commit_p99_proxy={row['commit_p99_proxy_ms']}ms")

    return samples

# ΓöÇΓöÇ Verdict ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

def evaluate(samples):
    kill_index    = next((i for i, r in enumerate(samples) if r["node2_killed"]), None)
    restart_index = next((i for i, r in enumerate(samples) if r["node2_restarted"]), None)

    if kill_index is None:
        return {"error": "node2 was never killed during the run ΓÇö check timing"}
    if restart_index is None:
        return {"error": "node2 was never restarted during the run ΓÇö check timing"}

    during_downtime = samples[kill_index:restart_index]
    after_restart   = samples[restart_index:]

    def strictly_progressing(rows, key):
        vals = [r[key] for r in rows if r.get(key) is not None]
        if len(vals) < 2:
            return None
        return all(b >= a for a, b in zip(vals, vals[1:]))

    # 1. Did the cluster (4 always-up nodes) keep committing while node2 was dead?
    cluster_progressed_during_downtime = strictly_progressing(during_downtime, "cluster_fsync_sum")

    # 2. Did node2's own fsync count resume growing after restart? (catching up)
    node2_counts_after = [r["node2_fsync_count"] for r in after_restart if r.get("node2_fsync_count") is not None]
    node2_resumed = len(node2_counts_after) >= 2 and node2_counts_after[-1] > node2_counts_after[0]

    # 3. Did cluster writes continue (not blocked) while node2 was catching up?
    cluster_progressed_after_restart = strictly_progressing(after_restart, "cluster_fsync_sum")

    # 4. Did node2 approach parity with the other nodes by the end of the run?
    last_row = samples[-1]
    other_counts = [last_row.get(f"{n}_fsync_count") for n in NODES if n not in (NODE_TO_KILL,) and last_row.get(f"{n}_fsync_count") is not None]
    avg_other_final = round(sum(other_counts) / len(other_counts), 1) if other_counts else None
    node2_final = last_row.get("node2_fsync_count")
    catch_up_ratio = round(node2_final / avg_other_final, 3) if (node2_final and avg_other_final) else None

    passed = bool(cluster_progressed_during_downtime) and bool(node2_resumed) and bool(cluster_progressed_after_restart)

    return {
        "kill_tick_index":     kill_index,
        "restart_tick_index":  restart_index,
        "cluster_progressed_during_downtime": cluster_progressed_during_downtime,
        "node2_resumed_after_restart":        node2_resumed,
        "cluster_progressed_after_restart":   cluster_progressed_after_restart,
        "node2_final_fsync_count":  node2_final,
        "avg_other_nodes_final_fsync_count": avg_other_final,
        "node2_catch_up_ratio":     catch_up_ratio,
        "passed": passed,
    }

def print_result(result):
    print(f"\n{'='*65}")
    print(f"  FAILURE SCENARIO 3 ΓÇö HARD CRASH + RECOVERY (SIGKILL, node2)")
    print(f"{'='*65}")
    if "error" in result:
        print(f"  Γ¥î {result['error']}")
        print(f"{'='*65}\n")
        return
    print(f"  Cluster kept committing during node2 downtime : {result['cluster_progressed_during_downtime']}")
    print(f"  Node2 resumed writing after restart           : {result['node2_resumed_after_restart']}")
    print(f"  Cluster kept committing while node2 caught up  : {result['cluster_progressed_after_restart']}")
    print(f"  Node2 final fsync count                        : {result['node2_final_fsync_count']}")
    print(f"  Avg other nodes' final fsync count             : {result['avg_other_nodes_final_fsync_count']}")
    print(f"  Node2 catch-up ratio (1.0 = full parity)       : {result['node2_catch_up_ratio']}")
    print(f"  {'-'*63}")
    if result["passed"]:
        print(f"  Γ£à PASS ΓÇö node2 crashed, recovered, and caught up without blocking commits")
    else:
        print(f"  Γ¥î FAIL ΓÇö see flags above for which condition failed")
    print(f"{'='*65}\n")

def save_results(samples, result):
    out = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "scenario": "failure_scenario_3_hard_crash",
        "node_killed": NODE_TO_KILL,
        "kill_at_second": KILL_AT_SECOND,
        "downtime_seconds": DOWNTIME_SECONDS,
        "total_run_seconds": TOTAL_RUN_SECONDS,
        "samples": samples,
        "result": result,
    }
    path = os.path.join(SCRIPT_DIR, "failure-scenario-3-results.json")
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
    print(f"  WR-RAFT FAILURE SCENARIO 3 ΓÇö HARD CRASH + RECOVERY")
    print(f"  Node: {NODE_TO_KILL} | SIGKILL at t={KILL_AT_SECOND}s | "
          f"Down for {DOWNTIME_SECONDS}s | Total run: {TOTAL_RUN_SECONDS}s")
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

    print("[scenario3] Warming up 10s...")
    time.sleep(10)

    stop_load = threading.Event()
    load_thread = threading.Thread(
        target=run_load_generator,
        args=(stop_load,),
        daemon=True
    )
    load_thread.start()
    print("[scenario3] Load generator started")

    samples = run_scenario()
    stop_load.set()

    result  = evaluate(samples)
    print_result(result)
    save_results(samples, result)

    stop_cluster(proc)
    print("[scenario3] Γ£à Failure scenario 3 complete!\n")

if __name__ == "__main__":
    main()
