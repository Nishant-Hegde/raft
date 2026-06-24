import subprocess
import sys
import os

# Path to the latency-injector folder
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
INJECT_SCRIPT = os.path.join(PROJECT_DIR, "latency-injector", "inject.py")

def apply_profile(profile_name):
    """
    Applies a latency profile by calling inject.py programmatically.
    Equivalent to: python latency-injector/inject.py <profile>
    """
    print(f"\n[Harness] Applying latency profile: {profile_name.upper()}")
    result = subprocess.run(
        [sys.executable, INJECT_SCRIPT, profile_name],
        capture_output=False  # Let output print to terminal
    )
    if result.returncode != 0:
        print(f"[Harness] ❌ Failed to apply profile '{profile_name}'")
        return False
    print(f"[Harness] ✅ Profile '{profile_name}' applied successfully")
    return True

def reset_all():
    """
    Resets all latency rules — calls inject.py reset.
    """
    print(f"\n[Harness] Resetting all latency rules...")
    result = subprocess.run(
        [sys.executable, INJECT_SCRIPT, "reset"],
        capture_output=False
    )
    if result.returncode != 0:
        print(f"[Harness] ⚠️  Reset may have failed")
    else:
        print(f"[Harness] ✅ All nodes reset to zero delay")

def verify_profile(profile_name):
    """
    Lists the profile so the harness can log what's active.
    """
    print(f"\n[Harness] Active profile settings:")
    subprocess.run(
        [sys.executable, INJECT_SCRIPT, "list"],
        capture_output=False
    )