import time
import math
import json
import csv
import os
import re
import sys
import subprocess
import urllib.request
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

TARGET_OPS_PER_SEC = 1000
RUN_SECONDS         = 60
WARMUP_SECONDS      = 10

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
        return (time.perf_counter() - start) * 1000
    except Exception:
        return None

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

def check_weight_uniformity(leader_port, expect_uniform):
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
        # Vanilla mode: gauge is legitimately never populated (confirmed
        # with B - EpochHistoryLogger callback never fires when
        # UpdateEWAWeight is never called). Absence is expected there.
        if expect_uniform:
            return True, weights
        else:
            print("[grid]   No wr_raft_weight metric found - cannot verify WR-Raft mode")
            return False, weights

    values = list(weights.values())
    spread = max(values) - min(values)
    is_uniform = spread < 0.05
    passed = is_uniform if expect_uniform else not is_uniform
    return passed, weights

def wait_for_weight_condition(leader_port, expect_uniform, max_wait=20, poll_every=3):
    """
    Poll check_weight_uniformity repeatedly instead of checking once.
    Weights need a few AppendEntries cycles to diverge - a single check
    right after warmup can catch them still sitting at their initial
    1.0 default, producing a false FAIL even when the system is working.
    """
    deadline = time.time() + max_wait
    last_weights = {}
    while time.time() < deadline:
        passed, weights = check_weight_uniformity(leader_port, expect_uniform)
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
    time.sleep(5)   # give Docker a bit more time to fully release ports/containers
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
    # weighted mode: leave WR_WEIGHTING unset entirely

    proc = start_cluster(env_vars)

    if not wait_for_cluster():
        print("  Cluster failed to start - skipping cell")
        stop_cluster(proc)
        return None

    print(f"  Warming up {WARMUP_SECONDS}s...")
    time.sleep(WARMUP_SECONDS)

    expected_mode = "DISABLED" if mode == "vanilla" else "ENABLED"
    if not verify_weighting_mode(expected_mode):
        print(f"  ABORTING CELL - mode verification failed ({expected_mode} not confirmed on all nodes)")
        stop_cluster(proc)
        return None

    leader_port, leader_name = find_leader_port()
    if leader_port is None:
        print("  ABORTING CELL - no leader found")
        stop_cluster(proc)
        return None

    # Uniform profile has no real heterogeneity - weights should stay
    # near-equal even with weighting ON, since there's nothing for the
    # EWA to differentiate. Only moderate/severe profiles should show
    # divergence when weighted.
    if profile_name == "uniform":
        expect_uniform = True
    else:
        expect_uniform = (mode == "vanilla")

    weight_ok, weights = wait_for_weight_condition(leader_port, expect_uniform)
    if not weight_ok:
        print("  ABORTING CELL - weight-uniformity assertion failed, do not trust this cell")
        print(f"  Last observed weights: {weights}")
        stop_cluster(proc)
        return None

    print(f"  Leader: {leader_name} (port {leader_port}) - running {RUN_SECONDS}s at {TARGET_OPS_PER_SEC} ops/sec...")

    interval  = 1.0 / TARGET_OPS_PER_SEC
    end_time  = time.time() + RUN_SECONDS
    ops_done  = 0
    latencies = []

    while time.time() < end_time:
        elapsed_ms = timed_propose(leader_port)
        if elapsed_ms is not None:
            latencies.append(elapsed_ms)
        ops_done += 1
        remaining = end_time - time.time()
        if remaining <= 0:
            break
        time.sleep(max(0, min(interval, remaining)))

    throughput = round(len(latencies) / RUN_SECONDS, 1) if latencies else None

    row = {
        "mode": mode,
        "heterogeneity_profile": profile_name,
        "p50_ms":  calc_percentile(latencies, 50),
        "p99_ms":  calc_percentile(latencies, 99),
        "p999_ms": calc_percentile(latencies, 99.9),
        "throughput_ops_sec": throughput,
        "ops_attempted": ops_done,
        "ops_succeeded": len(latencies),
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
        for profile_name in HETEROGENEITY_PROFILES:
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
                "is no real heterogeneity for the EWA to react to."
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