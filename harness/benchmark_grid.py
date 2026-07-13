import time
import math
import json
import csv
import os
import re
import sys
import threading
import subprocess
import urllib.request
import concurrent.futures
from datetime import datetime

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

NODES = ["node1", "node2", "node3", "node4", "node5"]

METRICS_PORTS = {
    "node1": 9091, "node2": 9092, "node3": 9093,
    "node4": 9094, "node5": 9095,
}

NODE_ID_TO_NAME = {
    "1": "node1", "2": "node2", "3": "node3", "4": "node4", "5": "node5",
}

# -- Grid definition -----------------------------------------
# Read axis dropped (agreed with B): WR-Raft affects the commit path
# only - reads are served from local applied state without quorum
# involvement, so read/write mix is not a meaningful axis here.
# Grid is 3 heterogeneity profiles x 2 modes = 6 cells.

HETEROGENEITY_PROFILES = {
    "uniform":  {"NODE1_DELAY": "1", "NODE2_DELAY": "1", "NODE3_DELAY": "1", "NODE4_DELAY": "1",  "NODE5_DELAY": "1"},
    "moderate": {"NODE1_DELAY": "1", "NODE2_DELAY": "1", "NODE3_DELAY": "1", "NODE4_DELAY": "5",  "NODE5_DELAY": "5"},
    "severe":   {"NODE1_DELAY": "1", "NODE2_DELAY": "1", "NODE3_DELAY": "1", "NODE4_DELAY": "10", "NODE5_DELAY": "10"},
}

TARGET_OPS_PER_SEC   = 1000
RUN_SECONDS          = 60
WARMUP_SECONDS       = 10
BACKGROUND_OPS_PER_SEC = 20    # light load kept running during warmup + weight-poll,
                                # so the EWA has real AppendEntries latency to react to
                                # before the actual measured run even starts

# -- Metrics helpers (supplementary per-node fsync stats) ----

def fetch_metrics(port):
    try:
        url = f"http://localhost:{port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.read().decode("utf-8")
    except Exception:
        return ""

def calc_percentile(samples, pct):
    if not samples:
        return None
    s = sorted(samples)
    idx = min(int(len(s) * (pct / 100.0)), len(s) - 1)
    return round(s[idx], 3)

# -- Real client-timed writes ---------------------------------

def timed_propose(port, data=b"bench-op", timeout=5):
    start = time.perf_counter()
    try:
        req = urllib.request.Request(
            f"http://localhost:{port}/propose",
            data=data,
            method="POST"
        )
        urllib.request.urlopen(req, timeout=timeout)
        elapsed = (time.perf_counter() - start) * 1000
        return elapsed, "success"
    except urllib.error.URLError as e:
        if isinstance(e.reason, TimeoutError) or "timed out" in str(e.reason).lower():
            return None, "timeout"
        return None, "error"
    except Exception:
        return None, "error"
    
# -- Background load generator --------------------------------
# Keeps real writes flowing continuously so weights have something
# to converge on BEFORE the measured run starts. Runs in a daemon
# thread; stop_event signals it to stop.

def background_load(port_getter, stop_event, ops_per_sec):
    interval = 1.0 / ops_per_sec
    while not stop_event.is_set():
        port = port_getter()
        if port is not None:
            timed_propose(port)
        stop_event.wait(interval)

# -- Leader / mode discovery -----------------------------------

def find_leader_port(timeout=15):
    deadline = time.time() + timeout
    pattern = re.compile(r"(\d+) became leader at term")
    while time.time() < deadline:
        try:
            result = subprocess.run(
                ["docker", "compose", "logs"],
                cwd=PROJECT_DIR, capture_output=True, text=True, timeout=10
            )
            matches = pattern.findall(result.stdout)
            if matches:
                leader_id = matches[-1]
                leader_name = NODE_ID_TO_NAME.get(leader_id)
                if leader_name:
                    return METRICS_PORTS[leader_name], leader_name
        except Exception:
            pass
        time.sleep(2)
    return None, None

def verify_weighting_mode(expected_mode):
    try:
        result = subprocess.run(
            ["docker", "compose", "logs"],
            cwd=PROJECT_DIR, capture_output=True, text=True, timeout=10
        )
    except Exception as e:
        print(f"[grid]   Could not read logs to verify mode: {e}")
        return False

    log_text = result.stdout
    confirmed = 0
    for node in NODES:
        node_lines = [l for l in log_text.splitlines() if l.startswith(f"{node}-1")]
        found = any(f"WR-Raft weighting: {expected_mode}" in l for l in node_lines)
        if found:
            confirmed += 1
        else:
            print(f"[grid]   {node}: did NOT confirm '{expected_mode}' in logs")
    return confirmed == len(NODES)

def check_weight_uniformity(leader_port, expect_uniform, leader_id=None):
    text = fetch_metrics(leader_port)
    weights = {}
    for line in text.splitlines():
        if line.startswith("wr_raft_weight{"):
            try:
                peer_id = line.split('peer_id="')[1].split('"')[0]
                value = float(line.split("} ")[1])
                weights[peer_id] = value
            except Exception:
                pass

    if not weights:
        if expect_uniform:
            return True, weights
        else:
            print("[grid]   No wr_raft_weight metric found - cannot verify WR-Raft mode")
            return False, weights

    # Leader's own weight entry appears to follow a different rule than
    # follower weights (no self-measured AppendEntries latency) - exclude
    # it from the spread check so we're only judging what the EWA does
    # with observed peer latency, not the leader's own placeholder value.
    # Flagged to B for confirmation; documenting as a known caveat either way.
    compare_weights = {k: v for k, v in weights.items() if k != leader_id} if leader_id else weights

    if not compare_weights:
        return expect_uniform, weights

    values = list(compare_weights.values())
    spread = max(values) - min(values)
    is_uniform = spread < 0.05
    passed = is_uniform if expect_uniform else not is_uniform
    return passed, weights

def wait_for_weight_condition(leader_port, expect_uniform, leader_id=None, max_wait=40, poll_every=3):
    deadline = time.time() + max_wait
    last_weights = {}
    while time.time() < deadline:
        passed, weights = check_weight_uniformity(leader_port, expect_uniform, leader_id=leader_id)
        last_weights = weights
        if passed:
            return True, weights
        time.sleep(poll_every)
    return False, last_weights

# -- Cluster helpers --------------------------------------------

def wait_for_cluster(timeout=60):
    print("[grid]   Waiting for all 5 nodes...", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        up = 0
        for node in NODES:
            try:
                urllib.request.urlopen(
                    f"http://localhost:{METRICS_PORTS[node]}/metrics", timeout=2
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
    print(f"[grid] Starting cluster with: {env_vars}")
    env = os.environ.copy()
    env.pop("WR_WEIGHTING", None)   # clear any leaked shell-level value first
    env.update(env_vars)
    subprocess.run(
        ["docker", "compose", "down", "--remove-orphans"],
        cwd=PROJECT_DIR, capture_output=True
    )
    time.sleep(5)
    proc = subprocess.Popen(
        ["docker", "compose", "up", "--build"],
        cwd=PROJECT_DIR, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return proc

def stop_cluster(proc):
    subprocess.run(["docker", "compose", "down"], cwd=PROJECT_DIR, capture_output=True)
    time.sleep(5)
    proc.terminate()

# -- Per-cell run --------------------------------------------

def run_cell(mode, profile_name):
    print(f"\n{'='*65}")
    print(f"  CELL: mode={mode}  profile={profile_name}")
    print(f"{'='*65}")

    env_vars = dict(HETEROGENEITY_PROFILES[profile_name])
    if mode == "vanilla":
        env_vars["WR_WEIGHTING"] = "off"

    proc = start_cluster(env_vars)

    if not wait_for_cluster():
        print("  Cluster failed to start - skipping cell")
        stop_cluster(proc)
        return None

    expected_mode = "DISABLED" if mode == "vanilla" else "ENABLED"

    # -- Start background load IMMEDIATELY, before warmup even finishes.
    #    This is the fix: weights only move on real AppendEntries
    #    responses, not heartbeats. Without real traffic flowing during
    #    warmup/weight-check, the EWA has nothing to react to and stays
    #    near its 1.0 default the whole time we're checking it.
    leader_port_holder = {"port": None}
    stop_bg = threading.Event()
    bg_thread = threading.Thread(
        target=background_load,
        args=(lambda: leader_port_holder["port"], stop_bg, BACKGROUND_OPS_PER_SEC),
        daemon=True
    )
    bg_thread.start()

    print(f"  Warming up {WARMUP_SECONDS}s (background load already flowing)...")
    time.sleep(WARMUP_SECONDS)

    if not verify_weighting_mode(expected_mode):
        print(f"  ABORTING CELL - mode verification failed ({expected_mode} not confirmed on all nodes)")
        stop_bg.set()
        stop_cluster(proc)
        return None

    leader_port, leader_name = find_leader_port()
    if leader_port is None:
        print("  ABORTING CELL - no leader found")
        stop_bg.set()
        stop_cluster(proc)
        return None

    leader_port_holder["port"] = leader_port   # background load now targets the real leader
    
    leader_id = leader_name.replace("node", "") 
    # Uniform profile has no real heterogeneity - weights should stay
    # near-equal even with weighting ON, since there's nothing for the
    # EWA to differentiate. Only moderate/severe profiles should show
    # divergence when weighted.
    if profile_name == "uniform":
        expect_uniform = True
    else:
        expect_uniform = (mode == "vanilla")

    print(f"  Leader: {leader_name} (port {leader_port}) - polling weight condition (background load flowing)...")
    weight_ok, weights = wait_for_weight_condition(leader_port, expect_uniform, leader_id=leader_id)
    if not weight_ok:
        print("  ABORTING CELL - weight-uniformity assertion failed, do not trust this cell")
        print(f"  Last observed weights: {weights}")
        stop_bg.set()
        stop_cluster(proc)
        return None

    print(f"  Weight condition confirmed: {weights}")

    # Stop the light background thread - the real measured run below
    # takes over as the actual load for this cell.
    stop_bg.set()
    bg_thread.join(timeout=2)

    print(f"  Running {RUN_SECONDS}s at {TARGET_OPS_PER_SEC} ops/sec (measured, concurrent)...")

    latencies = []
    outcome_counts = {"success": 0, "timeout": 0, "error": 0}
    lock = threading.Lock()
    end_time = time.time() + RUN_SECONDS

    def worker():
        while time.time() < end_time:
            elapsed_ms, outcome = timed_propose(leader_port)
            with lock:
                outcome_counts[outcome] += 1
                if elapsed_ms is not None:
                    latencies.append(elapsed_ms)

    num_workers = 50
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker) for _ in range(num_workers)]
        concurrent.futures.wait(futures)

    total_attempts = sum(outcome_counts.values())
    throughput = round(len(latencies) / RUN_SECONDS, 1) if latencies else None

    print(f"  Outcomes: success={outcome_counts['success']} "
          f"timeout={outcome_counts['timeout']} error={outcome_counts['error']} "
          f"(total attempted={total_attempts})")

    # Dump raw per-request latencies for this cell so we can inspect the
    # distribution shape (e.g. bimodal), not just aggregate percentiles.
    raw_path = os.path.join(SCRIPT_DIR, f"raw-latencies-{mode}-{profile_name}.json")
    with open(raw_path, "w") as f:
        json.dump({
            "mode": mode,
            "profile": profile_name,
            "raw_latencies_ms": latencies,
            "outcome_counts": outcome_counts,
        }, f)
    print(f"  Raw latencies saved to: {raw_path}")
    

    row = {
        "mode": mode,
        "heterogeneity_profile": profile_name,
        "p50_ms":  calc_percentile(latencies, 50),
        "p99_ms":  calc_percentile(latencies, 99),
        "p999_ms": calc_percentile(latencies, 99.9),
        "throughput_ops_sec": throughput,
        "ops_attempted": total_attempts,
        "ops_succeeded": outcome_counts["success"],
        "ops_timeout": outcome_counts["timeout"],
        "ops_error": outcome_counts["error"],
        "weights_at_check": weights,
    }

    stop_cluster(proc)
    print(f"  Cell done: p50={row['p50_ms']}ms p99={row['p99_ms']}ms throughput={throughput}ops/s")
    return row

# -- Main grid loop --------------------------------------------

def main():
    print(f"\n{'='*65}")
    print(f"  WR-RAFT BENCHMARK GRID (writes-only, verified toggle)")
    print(f"  3 heterogeneity profiles x 2 modes = 6 runs")
    print(f"  Read axis dropped: WR-Raft affects commit path only,")
    print(f"  reads don't touch quorum/weights (agreed with B)")
    print(f"{'='*65}")

    results = []
    skipped = []
    cell_num = 0
    total_cells = 2 * len(HETEROGENEITY_PROFILES)

    for mode in ["vanilla", "weighted"]:
        for profile_name in ["severe"]:
            cell_num += 1
            print(f"\n>>> Cell {cell_num}/{total_cells} <<<")
            row = run_cell(mode, profile_name)
            if row:
                results.append(row)
            else:
                skipped.append(f"{mode}/{profile_name}")

    csv_path = os.path.join(SCRIPT_DIR, "benchmark-grid-results.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "mode", "heterogeneity_profile",
            "p50_ms", "p99_ms", "p999_ms", "throughput_ops_sec",
            "ops_attempted", "ops_succeeded"
        ])
        writer.writeheader()
        for r in results:
            writer.writerow({k: r[k] for k in writer.fieldnames})

    json_path = os.path.join(SCRIPT_DIR, "benchmark-grid-results.json")
    with open(json_path, "w") as f:
        json.dump({
            "generated": datetime.now().astimezone().isoformat(),
            "results": results,
            "skipped_cells": skipped,
            "note": (
                "Read/write mix axis intentionally dropped. WR-Raft affects "
                "the commit path only - reads are served from local applied "
                "state without quorum involvement, so read mix is not a "
                "meaningful axis for this evaluation. Grid is 3 heterogeneity "
                "profiles x 2 modes (vanilla/weighted), writes-only. Uniform "
                "profile expects uniform weights under BOTH modes since there "
                "is no real heterogeneity for the EWA to react to. A light "
                "background write load (20 ops/sec) runs during warmup and "
                "weight-convergence polling so the EWA has real AppendEntries "
                "latency to react to before the measured run begins."
            ),
        }, f, indent=2)

    print(f"\n{'='*65}")
    print(f"  GRID COMPLETE - {len(results)}/{total_cells} cells captured")
    if skipped:
        print(f"  SKIPPED (failed verification): {skipped}")
    print(f"  CSV:  {csv_path}")
    print(f"  JSON: {json_path}")
    print(f"{'='*65}\n")

if __name__ == "__main__":
    main()