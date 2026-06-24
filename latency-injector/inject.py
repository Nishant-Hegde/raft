import subprocess
import sys
import time
import json
import os

# Load profiles from config file
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROFILES_FILE = os.path.join(SCRIPT_DIR, "profiles.json")

def load_profiles():
    with open(PROFILES_FILE, "r") as f:
        return json.load(f)

def get_container_name(node_id):
    return f"raft-{node_id}-1"

def run_in_container(container, cmd):
    full_cmd = ["docker", "exec", container, "sh", "-c", cmd]
    result = subprocess.run(full_cmd, capture_output=True, text=True)
    return result

def clear_delay(container):
    run_in_container(container, "tc qdisc del dev eth0 root 2>/dev/null || true")

def apply_delay(container, delay_ms, jitter_ms=0):
    if jitter_ms > 0:
        cmd = f"tc qdisc add dev eth0 root netem delay {delay_ms}ms {jitter_ms}ms"
    else:
        cmd = f"tc qdisc add dev eth0 root netem delay {delay_ms}ms"
    result = run_in_container(container, cmd)
    if result.returncode != 0:
        print(f"  ⚠️  Warning: {result.stderr.strip()}")
    else:
        print(f"  ✅ Applied {delay_ms}ms delay (jitter: {jitter_ms}ms) to {container}")

def show_current(container):
    result = run_in_container(container, "tc qdisc show dev eth0")
    return result.stdout.strip()

def inject_profile(profile_name):
    config = load_profiles()

    if profile_name not in config["profiles"]:
        print(f"❌ Unknown profile '{profile_name}'. Available: {', '.join(config['profiles'].keys())}")
        sys.exit(1)

    profile = config["profiles"][profile_name]
    nodes = config["nodes"]

    print(f"\n🔧 Injecting profile: {profile_name.upper()}")
    print(f"   Description : {profile['description']}")
    print(f"   Spread      : {profile['spread']}")
    print("─" * 55)
    print(f"   {'Node':<8} {'Target p99 (ms)':<18} {'netem delay (ms)'}")
    print("─" * 55)
    for node in nodes:
        target = profile["target_p99_ms"][node]
        delay  = profile["netem_delay_ms"][node]
        print(f"   {node:<8} {target:<18} {delay}")
    print("─" * 55)

    print("\nApplying delays...")
    for node in nodes:
        container = get_container_name(node)
        delay_ms  = profile["netem_delay_ms"][node]
        jitter_ms = profile["netem_jitter_ms"][node]
        clear_delay(container)
        apply_delay(container, delay_ms, jitter_ms)

    print("\n⏳ Waiting 5 seconds for rules to stabilise...")
    time.sleep(5)

    print("\n📋 Verification — tc rules now active per node:")
    print("─" * 55)
    for node in nodes:
        container = get_container_name(node)
        status = show_current(container)
        # Pull out just the delay value for clean display
        delay_part = ""
        for part in status.split():
            if "delay" in status:
                delay_part = status
                break
        print(f"  {node}: {status}")

    print(f"\n✅ Profile '{profile_name}' is now active.")

def reset_all():
    config = load_profiles()
    nodes = config["nodes"]
    print("\n🔄 Resetting all nodes — removing all delays...")
    for node in nodes:
        container = get_container_name(node)
        clear_delay(container)
        print(f"  ✅ Cleared {node}")
    print("\n✅ All nodes reset to zero delay.")

def list_profiles():
    config = load_profiles()
    print("\n📋 Available latency profiles:")
    print("─" * 55)
    for name, profile in config["profiles"].items():
        print(f"\n  [{name.upper()}]")
        print(f"  Description : {profile['description']}")
        print(f"  Spread      : {profile['spread']}")
        nodes_summary = ", ".join(
            f"{n}→{profile['netem_delay_ms'][n]}ms"
            for n in config["nodes"]
        )
        print(f"  Delays      : {nodes_summary}")
    print()

def main():
    if len(sys.argv) < 2:
        print("\nUsage:")
        print("  python inject.py mild        # Apply mild profile (2x spread)")
        print("  python inject.py moderate    # Apply moderate profile (5x spread)")
        print("  python inject.py severe      # Apply severe profile (10x spread)")
        print("  python inject.py reset       # Remove all delays")
        print("  python inject.py list        # Show all profiles")
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "reset":
        reset_all()
    elif command == "list":
        list_profiles()
    else:
        inject_profile(command)

if __name__ == "__main__":
    main()