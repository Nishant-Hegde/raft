import time
import math
import json
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
    "node1": 9091,
    "node2": 9092,
    "node3": 9093,
    "node4": 9094,
    "node5": 9095,
}

NODE_ID_TO_NAME = {
    "1": "node1", "2": "node2", "3": "node3", "4": "node4", "5": "node5",
}

# -- Metrics helpers (supplementary per-node fsync stats) --

def fetch_metrics(port):
    try:
        url = f"http://localhost:{port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.read().decode("utf-8")
    except Exception:
        return ""

def parse_percentile(metrics_text, pct):
    """Histogram BUCKET EDGES, not real values. Supplementary only."""
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

def calc_percentile(samples, pct):
    """Real percentile from actual sorted client-timed samples."""
    if not samples:
        return None
    s = sorted(samples)
    idx = min(int(len(s) * (pct / 100.0)), len(s) - 1)
    return round(s[idx], 3)

# -- Load generation (real writes, timed client-side) -------

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

# -- Leader / mode discovery ---------------------------------

def find_leader_port(timeout=15):
    """
    Grep docker compose logs for the most recent 'became leader' line
    and map it back to that node's metrics port. Returns (None, None)
    if no leader found within timeout.
    """
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
                leader_id = matches[-1]  # most recent leader wins
                leader_name = NODE_ID_TO_NAME.get(leader_id)
                if leader_name:
                    return METRICS_PORTS[leader_name], leader_name
        except Exception:
            pass
        time.sleep(2)
    return None, None

def verify_weighting_mode(expected_mode):
    """
    Grep each node's logs for the startup line confirming which mode
    it actually booted into. expected_mode is 'ENABLED' or 'DISABLED'.
    Returns True only if ALL nodes confirm the expected mode - never
    trust the env var alone.
    """
    print(f"[e2e] Verifying all nodes booted with weighting {expected_mode}...")
    try:
        result = subprocess.run(
            ["docker", "compose", "logs"],
            cwd=PROJECT_DIR, capture_output=True, text=True, timeout=10
        )
    except Exception as e:
        print(f"[e2e] Could not read logs to verify mode: {e}")
        return False

    log_text = result.stdout
    confirmed = 0
    for node in NODES:
        # docker compose logs prefixes each line with "<service>-1  | "
        node_lines = [l for l in log_text.splitlines() if l.startswith(f"{node}-1")]
        found = any(f"WR-Raft weighting: {expected_mode}" in l for l in node_lines)
        if found:
            confirmed += 1
        else:
            print(f"[e2e]   {node}: did NOT confirm '{expected_mode}' in logs")

    if confirmed == len(NODES):
        print(f"[e2e]   All {len(NODES)} nodes confirmed weighting {expected_mode}")
        return True
    else:
        print(f"[e2e]   Only {confirmed}/{len(NODES)} nodes confirmed - mode verification FAILED")
        return False

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
        if expect_uniform:
            # Vanilla mode may simply not export this metric at all -
            # log-based mode verification already confirmed DISABLED,
            # so treat absence as acceptable here rather than aborting.
            print("[e2e]   No wr_raft_weight metric found on leader - "
                  "expected for vanilla mode (weighting disabled), treating as PASS")
            return True, weights
        else:
            print("[e2e]   No wr_raft_weight metric found on leader - "
                  "cannot verify WR-Raft mode is actually weighting")
            return False, weights

    values = list(weights.values())
    spread = max(values) - min(values)
    is_uniform = spread < 0.05

    if expect_uniform:
        passed = is_uniform
        label = "vanilla (expect uniform weights)"
    else:
        passed = not is_uniform
        label = "WR-Raft (expect non-uniform weights with a slow node present)"

    print(f"[e2e]   Mode: {label}")
    print(f"[e2e]   Weights: {weights}")
    print(f"[e2e]   Spread: {round(spread, 4)} -> {'PASS' if passed else 'FAIL'}")
    return passed, weights

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
    print(f"\n[e2e] Starting cluster with: {env_vars}")
    env = os.environ.copy()
    env.pop("WR_WEIGHTING", None)  # clear any leaked shell-level value before applying this run's intent
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
    print("\n[e2e] Stopping cluster...")
    subprocess.run(
        ["docker", "compose", "down"],
        cwd=PROJECT_DIR, capture_output=True
    )
    time.sleep(5)
    print("[e2e] Cluster stopped")

# -- Benchmark run -----------------------------------------

def run_benchmark(mode_name, duration_sec, ops_per_sec, leader_port):
    print(f"\n[e2e] Running workload: {duration_sec}s at {ops_per_sec} ops/sec "
          f"against leader port {leader_port}...")

    interval  = 1.0 / ops_per_sec
    end_time  = time.time() + duration_sec
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

    print(f"[e2e] Workload done - {ops_done} ops attempted, {len(latencies)} succeeded")

    commit_p50  = calc_percentile(latencies, 50)
    commit_p99  = calc_percentile(latencies, 99)
    commit_p999 = calc_percentile(latencies, 99.9)

    per_node = {}
    for node in NODES:
        text = fetch_metrics(METRICS_PORTS[node])
        per_node[node] = {
            "p50_ms":  parse_percentile(text, 50),
            "p99_ms":  parse_percentile(text, 99),
            "p999_ms": parse_percentile(text, 99.9),
        }

    return {
        "mode":           mode_name,
        "ops_per_sec":    ops_per_sec,
        "duration_sec":   duration_sec,
        "timestamp":      datetime.utcnow().isoformat(),
        "ops_completed":  ops_done,
        "ops_succeeded":  len(latencies),
        "per_node":       per_node,
        "commit_p50_ms":  commit_p50,
        "commit_p99_ms":  commit_p99,
        "commit_p999_ms": commit_p999,
    }

# -- Full run: bring up cluster, verify mode, verify weights, benchmark --

def run_full_scenario(label, mode_name, weighting_env, delay_env, duration_sec, ops_per_sec):
    """
    weighting_env: {} for WR-Raft (unset), or {"WR_WEIGHTING": "off"} for vanilla
    delay_env: NODE1_DELAY..NODE5_DELAY dict - SAME across both runs so only
               the weighting toggle differs, per B's fix for the original
               experimental design flaw.
    """
    print(f"\n{'-'*70}")
    print(f"  {label}")
    print(f"{'-'*70}")

    env_vars = dict(delay_env)
    env_vars.update(weighting_env)

    proc = start_cluster(env_vars)

    if not wait_for_cluster():
        print("Cluster failed to start.")
        proc.terminate()
        sys.exit(1)

    print("[e2e] Warming up for 10s...")
    time.sleep(10)

    expected_mode = "DISABLED" if weighting_env.get("WR_WEIGHTING") == "off" else "ENABLED"
    if not verify_weighting_mode(expected_mode):
        print("[e2e] ABORTING - mode verification failed, do not trust results from a broken toggle")
        stop_cluster()
        proc.terminate()
        sys.exit(1)

    leader_port, leader_name = find_leader_port()
    if leader_port is None:
        print("[e2e] ABORTING - no leader found")
        stop_cluster()
        proc.terminate()
        sys.exit(1)
    print(f"[e2e] Leader is {leader_name} (port {leader_port})")

    expect_uniform = (expected_mode == "DISABLED")
    weight_ok, weights = check_weight_uniformity(leader_port, expect_uniform)
    if not weight_ok:
        print("[e2e] ABORTING - weight-uniformity assertion failed. "
              "Do not trust this run: either the toggle silently no-op'd, "
              "or weights haven't converged yet. Check timing/logs before rerunning.")
        stop_cluster()
        proc.terminate()
        sys.exit(1)

    result = run_benchmark(mode_name, duration_sec, ops_per_sec, leader_port)
    result["leader_at_start"] = leader_name
    result["weights_at_check"] = weights
    print_per_node_table(result)

    stop_cluster()
    proc.terminate()
    return result

# -- Print tables ------------------------------------------

def print_per_node_table(result):
    mode = result["mode"]
    print(f"\n  [{mode}] Per-node fsync latency (supplementary, bucket-derived):")
    print(f"  {'Node':<8} {'p50 (ms)':>10} {'p99 (ms)':>10} {'p999 (ms)':>10}")
    print(f"  {'-'*8} {'-'*10} {'-'*10} {'-'*10}")
    for node in NODES:
        r    = result["per_node"].get(node, {})
        p50  = r.get("p50_ms")  or 0
        p99  = r.get("p99_ms")  or 0
        p999 = r.get("p999_ms") or 0
        print(f"  {node:<8} {p50:>10.3f} {p99:>10.3f} {p999:>10.3f}")
    print(f"\n  Ops attempted:  {result['ops_completed']}")
    print(f"  Ops succeeded:  {result['ops_succeeded']}")
    print(f"  Real commit p50  (client-timed): {result['commit_p50_ms']} ms")
    print(f"  Real commit p99  (client-timed): {result['commit_p99_ms']} ms")
    print(f"  Real commit p999 (client-timed): {result['commit_p999_ms']} ms")

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
    print(f"  COMPARISON TABLE - Vanilla vs WR-Raft (SAME delay profile)")
    print(f"  {ops_per_sec} ops/sec | real POST /propose writes, client-timed")
    print(f"{'='*70}")
    print(f"  {'Metric':<12} {'Vanilla (ms)':>18} {'WR-Raft (ms)':>14} {'Improvement':>12}")
    print(f"  {'-'*12} {'-'*18} {'-'*14} {'-'*12}")
    print(f"  {'p50':<12} {vp50:>18.3f} {wp50:>14.3f} {imp(vp50,wp50):>11.1f}%")
    print(f"  {'p99':<12} {vp99:>18.3f} {wp99:>14.3f} {imp(vp99,wp99):>11.1f}%")
    print(f"  {'p999':<12} {vp999:>18.3f} {wp999:>14.3f} {imp(vp999,wp999):>11.1f}%")
    print(f"{'='*70}")
    print(f"  Positive % = WR-Raft is faster than vanilla")
    print(f"  Both runs used the SAME node delay profile - only weighting differs")
    print(f"{'='*70}\n")

# -- Save results ------------------------------------------

def save_results(vanilla, wr, ops_per_sec):
    out = {
        "ops_per_sec": ops_per_sec,
        "vanilla":     vanilla,
        "wr_raft":     wr,
        "generated":   datetime.utcnow().isoformat(),
        "note": (
            "commit_p50/p99/p999_ms are real client-side timed POST /propose "
            "latencies. Both runs used the same node delay profile; only "
            "WR_WEIGHTING differs. Mode and weight-uniformity were verified "
            "before each benchmark ran (see weights_at_check per run)."
        ),
    }
    path = os.path.join(SCRIPT_DIR, f"e2e-result-{ops_per_sec}ops.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"  Saved to: {path}")

# -- Main --------------------------------------------------

def main():
    ops_per_sec  = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    duration_sec = int(sys.argv[2]) if len(sys.argv) > 2 else 60

    print(f"\n{'='*70}")
    print(f"  WR-RAFT END-TO-END BENCHMARK (vanilla vs weighted, isolated)")
    print(f"  {ops_per_sec} ops/sec | {duration_sec}s")
    print(f"{'='*70}")

    # SAME delay profile for both runs - only WR_WEIGHTING differs.
    # This isolates the algorithm, per B's fix for the original design flaw.
    delay_profile = {
        "NODE1_DELAY": "1",
        "NODE2_DELAY": "1",
        "NODE3_DELAY": "1",
        "NODE4_DELAY": "5",
        "NODE5_DELAY": "5",
    }

    vanilla_result = run_full_scenario(
        label="RUN 1 - Vanilla Raft (WR_WEIGHTING=off, moderate delay profile)",
        mode_name="vanilla_raft",
        weighting_env={"WR_WEIGHTING": "off"},
        delay_env=delay_profile,
        duration_sec=duration_sec,
        ops_per_sec=ops_per_sec,
    )

    wr_result = run_full_scenario(
        label="RUN 2 - WR-Raft (weighting enabled, SAME moderate delay profile)",
        mode_name="wr_raft",
        weighting_env={},  # unset -> weighting enabled
        delay_env=delay_profile,
        duration_sec=duration_sec,
        ops_per_sec=ops_per_sec,
    )

    print_comparison_table(vanilla_result, wr_result, ops_per_sec)
    save_results(vanilla_result, wr_result, ops_per_sec)

    print("[e2e] End-to-end run complete!\n")

if __name__ == "__main__":
    main()
