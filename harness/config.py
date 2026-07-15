import os

# The profile to use - read from env var, default to "moderate"
LATENCY_PROFILE = os.environ.get("LATENCY_PROFILE", "moderate").lower()

# How long to run the test workload (seconds)
TEST_DURATION_SEC = int(os.environ.get("TEST_DURATION_SEC", "60"))

# How many write ops per second to send
WRITE_RATE_OPS = int(os.environ.get("WRITE_RATE_OPS", "100"))

NODE_HOSTS = {
    "node1": "10.20.201.33",   # laptop A
    "node2": "10.20.201.33",   # laptop A
    "node3": "10.20.201.62",   # laptop B
    "node4": "10.20.201.62",   # laptop B
    "node5": "10.20.201.101",   # laptop C
}
NODES = ["node1", "node2", "node3", "node4", "node5"]