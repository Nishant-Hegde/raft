import subprocess
import json
import time
import sys
import os
import urllib.request
import math

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROFILES_FILE = os.path.join(SCRIPT_DIR, "profiles.json")

def load_profiles():
    with open(PROFILES_FILE, "r") as f:
        return json.load(f)

def get_container_name(node_id):
    return f"raft-{node_id}-1"

def run_fio_in_container(container):
    """
    Runs fio --rw=write inside the container against /wal.
    Returns the raw JSON output from fio.
    """
    fio_cmd = (
        "fio --name=probe --rw=write --bs=4k --size=1m "
        "--filename=/wal/fio-probe.tmp --direct=0 "
        "--output-format=json --runtime=5 --time_based"
    )
    result = subprocess.run(
        ["docker", "exec", container, "sh", "-c", fio_cmd],
        capture_output=True,
        text=True,
        timeout=30
    )
    if result.returncode != 0:
        print(f"  ⚠️  fio error in {container}: {result.stderr.strip()[:100]}")
        return None
    return result.stdout

def parse_fio_p99(fio_output):
    """
    Parses fio JSON output and returns p99 write latency in milliseconds.
    fio reports latency in nanoseconds in the JSON.
    """
    try:
        # fio sometimes prints extra text before JSON — find the { 
        json_start = fio_output.index("{")
        clean = fio_output[json_start:]
        data = json.loads(clean)
        # Path: jobs[0] -> write -> clat_ns -> percentile -> 99.000000
        clat = data["jobs"][0]["write"]["clat_ns"]
        p99_ns = clat["percentile"]["99.000000"]
        return round(p99_ns / 1_000_000, 3)  # convert ns to ms
    except Exception as e:
        print(f"  ⚠️  Could not parse fio output: {e}")
        return None

def fetch_prometheus_p99(port):
    """
    Reads fsync_duration_ns p99 from Prometheus metrics endpoint.
    Returns p99 in ms.
    """
    try:
        url = f"http://localhost:{port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as resp:
            text = resp.read().decode("utf-8")
    except Exception as e:
        print(f"  ⚠️  Could not reach metrics port {port}: {e}")
        return None

    buckets = {}
    total_count = 0.0

    for line in text.splitlines():
        if line.startswith("#"):
            continue
        if line.startswith("fsync_duration_ns_bucket{"):
            try:
                le_str = line.split('le="')[1].split('"')[0]
                count = float(line.split("} ")[1])
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

    target = 0.99 * total_count
    for le in sorted(k for k in buckets if k != math.inf):
        if buckets[le] >= target:
            return round(le / 1_000_000, 3)
    return None

def probe_all_nodes(config, rounds=3):
    """
    Runs fio probe on all nodes for `rounds` iterations.
    Returns results as a list of dicts.
    """
    nodes = config["nodes"]
    ports = config["metrics_ports"]
    all_results = []

    for round_num in range(1, rounds + 1):
        print(f"\n  📍 Round {round_num}/{rounds}")
        print(f"  {'Node':<8} {'fio p99 (ms)':>14} {'Prometheus p99 (ms)':>21} {'Difference':>12} {'Match':>7}")
        print(f"  {'-'*8} {'-'*14} {'-'*21} {'-'*12} {'-'*7}")

        for node in nodes:
            container = get_container_name(node)
            port = ports[node]

            # Run fio probe
            fio_output = run_fio_in_container(container)
            fio_p99 = parse_fio_p99(fio_output) if fio_output else None

            # Read Prometheus
            prom_p99 = fetch_prometheus_p99(port)

            # Compare
            if fio_p99 is not None and prom_p99 is not None:
                diff = round(abs(fio_p99 - prom_p99), 3)
                match = "✅" if diff <= 5.0 else "⚠️ "
            else:
                diff = None
                match = "❌"

            fio_str  = f"{fio_p99:.3f} ms"  if fio_p99  is not None else "N/A"
            prom_str = f"{prom_p99:.3f} ms" if prom_p99 is not None else "N/A"
            diff_str = f"{diff:.3f} ms"     if diff     is not None else "N/A"

            print(f"  {node:<8} {fio_str:>14} {prom_str:>21} {diff_str:>12} {match:>7}")

            all_results.append({
                "round": round_num,
                "node": node,
                "fio_p99_ms": fio_p99,
                "prometheus_p99_ms": prom_p99,
                "difference_ms": diff,
                "match": match
            })

        if round_num < rounds:
            print(f"\n  ⏳ Waiting 10 seconds before next round...")
            time.sleep(10)

    return all_results

def print_summary(all_results):
    """Prints a summary across all rounds."""
    print(f"\n{'='*65}")
    print("  FIO PROBE SUMMARY — All Rounds")
    print(f"{'='*65}")

    # Average fio p99 per node
    from collections import defaultdict
    node_fio = defaultdict(list)
    node_prom = defaultdict(list)

    for r in all_results:
        if r["fio_p99_ms"] is not None:
            node_fio[r["node"]].append(r["fio_p99_ms"])
        if r["prometheus_p99_ms"] is not None:
            node_prom[r["node"]].append(r["prometheus_p99_ms"])

    print(f"  {'Node':<8} {'Avg fio p99':>13} {'Avg Prom p99':>14} {'Avg Diff':>10}")
    print(f"  {'-'*8} {'-'*13} {'-'*14} {'-'*10}")

    for node in load_profiles()["nodes"]:
        avg_fio  = round(sum(node_fio[node])  / len(node_fio[node]),  3) if node_fio[node]  else None
        avg_prom = round(sum(node_prom[node]) / len(node_prom[node]), 3) if node_prom[node] else None
        avg_diff = round(abs(avg_fio - avg_prom), 3) if avg_fio and avg_prom else None

        fio_str  = f"{avg_fio:.3f} ms"  if avg_fio  is not None else "N/A"
        prom_str = f"{avg_prom:.3f} ms" if avg_prom is not None else "N/A"
        diff_str = f"{avg_diff:.3f} ms" if avg_diff is not None else "N/A"

        print(f"  {node:<8} {fio_str:>13} {prom_str:>14} {diff_str:>10}")

    print(f"{'='*65}")
    print("  fio probe = direct block-device measurement from inside container")
    print("  Prometheus = fsync histogram from Go storage layer")
    print("  Both should be close — large diff means measurement mismatch")
    print(f"{'='*65}\n")

def save_results(all_results):
    out_path = os.path.join(SCRIPT_DIR, "fio-probe-results.json")
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"  📄 Results saved to: {out_path}")

def main():
    config = load_profiles()

    rounds = 3
    if len(sys.argv) > 1:
        try:
            rounds = int(sys.argv[1])
        except ValueError:
            print("Usage: python fio_probe.py [rounds]")
            print("Example: python fio_probe.py 3")
            sys.exit(1)

    print(f"\n🔍 FIO Write-Probe — measuring actual device write latency")
    print(f"   Runs fio --rw=write inside each container against /wal")
    print(f"   Compares result to Prometheus fsync_duration_ns p99")
    print(f"   Rounds: {rounds} | Interval: 10 seconds between rounds")
    print(f"{'='*65}")

    all_results = probe_all_nodes(config, rounds=rounds)
    print_summary(all_results)
    save_results(all_results)

if __name__ == "__main__":
    main()