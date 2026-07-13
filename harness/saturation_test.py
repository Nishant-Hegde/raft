import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from benchmark_grid import (
    start_cluster, stop_cluster, wait_for_cluster, find_leader_port,
    verify_weighting_mode, check_weight_uniformity, wait_for_weight_condition,
    background_load, run_rate_limited, calc_percentile,
    HETEROGENEITY_PROFILES
)
import threading
import time
import json

def test_rate(mode, profile_name, ops_per_sec, duration_sec=30):
    print(f"\n{'='*65}")
    print(f"  SATURATION TEST: mode={mode} profile={profile_name} rate={ops_per_sec}ops/sec")
    print(f"{'='*65}")

    env_vars = dict(HETEROGENEITY_PROFILES[profile_name])
    if mode == "vanilla":
        env_vars["WR_WEIGHTING"] = "off"

    proc = start_cluster(env_vars)
    if not wait_for_cluster():
        print("  Cluster failed to start")
        stop_cluster(proc)
        return None

    leader_port_holder = {"port": None}
    stop_bg = threading.Event()
    bg_thread = threading.Thread(
        target=background_load,
        args=(lambda: leader_port_holder["port"], stop_bg, 20),
        daemon=True
    )
    bg_thread.start()

    print("  Warming up 10s...")
    time.sleep(10)

    expected_mode = "DISABLED" if mode == "vanilla" else "ENABLED"
    verify_weighting_mode(expected_mode)

    leader_port, leader_name = find_leader_port()
    leader_port_holder["port"] = leader_port

    expect_uniform = (mode == "vanilla") or (profile_name == "uniform")
    weight_ok, weights = wait_for_weight_condition(leader_port, expect_uniform)
    print(f"  Weights: {weights}")

    stop_bg.set()
    bg_thread.join(timeout=2)

    print(f"  Running {duration_sec}s at controlled rate {ops_per_sec} ops/sec...")
    latencies, outcomes = run_rate_limited(leader_port, ops_per_sec, duration_sec)

    result = {
        "mode": mode, "profile": profile_name, "target_rate": ops_per_sec,
        "p50_ms": calc_percentile(latencies, 50),
        "p99_ms": calc_percentile(latencies, 99),
        "count": len(latencies),
        "outcomes": outcomes,
        "weights": weights,
    }
    print(f"  RESULT: p50={result['p50_ms']}ms p99={result['p99_ms']}ms n={result['count']}")

    stop_cluster(proc)
    return result

def main():
    all_results = []
    for rate in [50, 100]:
        for mode in ["vanilla", "weighted"]:
            r = test_rate(mode, "severe", ops_per_sec=rate, duration_sec=30)
            if r:
                all_results.append(r)

    with open("harness/saturation-test-results.json", "w") as f:
        json.dump(all_results, f, indent=2)

    print(f"\n{'='*65}")
    print("  SATURATION TEST SUMMARY")
    print(f"{'='*65}")
    for r in all_results:
        print(f"  {r['mode']:<10} rate={r['target_rate']:<5} p50={r['p50_ms']}ms p99={r['p99_ms']}ms")

if __name__ == "__main__":
    main()