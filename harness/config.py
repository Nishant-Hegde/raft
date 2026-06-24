import os

# The profile to use — read from env var, default to "moderate"
LATENCY_PROFILE = os.environ.get("LATENCY_PROFILE", "moderate").lower()

# How long to run the test workload (seconds)
TEST_DURATION_SEC = int(os.environ.get("TEST_DURATION_SEC", "60"))

# How many write ops per second to send
WRITE_RATE_OPS = int(os.environ.get("WRITE_RATE_OPS", "100"))

# Node gRPC ports
NODE_PORTS = {
    "node1": 50051,
    "node2": 50052,
    "node3": 50053,
    "node4": 50054,
    "node5": 50055,
}

# Node metrics ports
METRICS_PORTS = {
    "node1": 9091,
    "node2": 9092,
    "node3": 9093,
    "node4": 9094,
    "node5": 9095,
}

NODES = ["node1", "node2", "node3", "node4", "node5"]