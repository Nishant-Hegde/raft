import argparse
import time
import json
import sys
import urllib.request
import urllib.error
import concurrent.futures

def fetch_metrics(url):
    try:
        req = urllib.request.Request(url + "/metrics", method="GET")
        with urllib.request.urlopen(req, timeout=2) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        return ""

def get_weights(metrics_text):
    weights = {}
    for line in metrics_text.splitlines():
        if line.startswith("wr_raft_weight{peer_id="):
            # wr_raft_weight{peer_id="1"} 1
            try:
                parts = line.split("}")
                peer_id = parts[0].split('peer_id="')[1].split('"')[0]
                val = float(parts[1].strip())
                weights[peer_id] = val
            except Exception:
                pass
    return weights

def drive_load_worker(url, end_time, latencies):
    if time.time() > end_time:
        return
    
    start = time.perf_counter()
    try:
        req = urllib.request.Request(url + "/propose", data=b"e", method="POST")
        with urllib.request.urlopen(req, timeout=2) as resp:
            resp.read()
    except Exception:
        pass
    finally:
        elapsed = time.perf_counter() - start
        if latencies is not None:
            latencies.append(elapsed * 1000.0) # store in ms

def run_experiment():
    parser = argparse.ArgumentParser()
    parser.add_argument("--leader-url", default="http://127.0.0.1:9091", help="URL of the leader node")
    parser.add_argument("--slow-nodes", required=True, help="Comma separated list of slow node IDs")
    args = parser.parse_args()

    leader_url = args.leader_url
    slow_nodes = [x.strip() for x in args.slow_nodes.split(",")]

    print(f"Starting convergence gate against {leader_url}, waiting for nodes {slow_nodes} to drop below 0.5")

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)
    
    ops_per_sec = 100
    interval = 1.0 / ops_per_sec

    start_time = time.time()
    timeout = 120.0
    converged = False
    final_weights = {}
    time_to_converge = 0.0

    next_req_time = time.time()
    last_poll_time = 0.0
    
    while True:
        now = time.time()
        if now - start_time > timeout:
            break

        while next_req_time < now:
            executor.submit(drive_load_worker, leader_url, now + 5.0, None)
            next_req_time += interval

        if now - last_poll_time >= 2.0:
            last_poll_time = now
            metrics = fetch_metrics(leader_url)
            if metrics:
                weights = get_weights(metrics)
                final_weights = weights
                all_slow_dropped = True
                for sn in slow_nodes:
                    if sn not in weights or weights[sn] >= 0.5:
                        all_slow_dropped = False
                        break
                
                if all_slow_dropped and len(weights) > 0:
                    converged = True
                    time_to_converge = now - start_time
                    break
        time.sleep(0.01)

    if not converged:
        print("FAILED")
        print(f"Final Weights: {final_weights}")
        sys.exit(1)

    print(f"Converged! Time to converge: {time_to_converge:.2f}s")
    print(f"Weights: {final_weights}")
    
    print("Starting 60s measurement window...")
    measurement_duration = 60.0
    end_measure = time.time() + measurement_duration
    
    latencies = []
    next_req_time = time.time()
    
    while time.time() < end_measure:
        now = time.time()
        while next_req_time < now:
            executor.submit(drive_load_worker, leader_url, end_measure + 5.0, latencies)
            next_req_time += interval
        time.sleep(0.01)

    executor.shutdown(wait=True)
    
    if not latencies:
        print("No successful requests recorded during measurement window.")
        sys.exit(1)
        
    latencies.sort()
    count = len(latencies)
    p50 = latencies[int(count * 0.50)]
    p99 = latencies[int(count * 0.99)]
    
    print("\n--- Summary ---")
    print(f"Converged: {'Yes' if converged else 'No'}")
    print(f"Time-to-converge: {time_to_converge:.2f}s")
    print(f"Weights: {final_weights}")
    print(f"Ops completed: {count}")
    print(f"p50: {p50:.2f}ms")
    print(f"p99: {p99:.2f}ms")

if __name__ == "__main__":
    run_experiment()
