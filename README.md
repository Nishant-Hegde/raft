# WR-Raft: Write-latency–Responsive Raft

A fork of [etcd-io/raft](https://github.com/etcd-io/raft) that weights the commit quorum by measured per-follower storage write latency.
Instead of a plain node-count majority, the leader computes an exponentially weighted average (EWA) of each follower's fsync latency and normalizes these into per-node weights.
The commit threshold becomes "strictly more than half the total weight," so slow-disk nodes contribute less and the leader does not block on them.
Elections remain standard unweighted majority (see [Known Limitations](#known-limitations) for why).

## Architecture

Changes relative to upstream etcd-io/raft:

### Storage write latency measurement

Each storage backend can implement the `LatencyReporter` interface:

```go
type LatencyReporter interface {
    LastWriteLatencyNs() int64
}
```

The included `InstrumentedStorage` wraps any `Storage` and records the wall-clock duration of each `Append()` call (including any injected delay via `-extra-delay-ms`).

### Latency propagation

On a successful `MsgAppResp`, the follower attaches its most recent storage write latency in the `StorageWriteLatencyNs` field.
This is the latency of the previously completed write, not the current batch (one-batch offset), which is acceptable for an EWA average.

### EWA weight update

When the leader receives a `MsgAppResp` with a nonzero `StorageWriteLatencyNs`, it calls `tracker.UpdateEWAWeight(peerID, latencyNs)`:

- Converts latency to milliseconds and computes raw weight as `1.0 / latencyMs`.
- Blends with the previous weight using EWA: `w_new = alpha * raw + (1 - alpha) * w_old`.
- Default alpha: `0.2`.
- Normalizes all weights to sum to `n` (the number of voters).

### Weight floor

A floor of `epsilon = 0.05` prevents any node's weight from collapsing to zero.
Floored nodes are clamped exactly to epsilon; only unfloored nodes participate in renormalization.
This ensures a slow node can recover if its disk improves — without the floor, a near-zero weight makes the node permanently irrelevant to quorum.

### Adaptive jitter damping

If weight variance across the cluster exceeds a threshold, the effective alpha is lowered cluster-wide for a cooldown period.
Damping must be uniform across all nodes.
Mixed alphas under a shared normalization constraint create a parasitic fixed point where the damped node's weight inverts upward — the slower-adapting node appears relatively stable while faster-adapting nodes chase it, producing weight oscillations rather than convergence.

### Epoch-based weight broadcast

The leader packs the current weight vector into the `AppendEntries` `Context` field.
Followers echo this epoch context back in their `MsgAppResp`, allowing the leader to confirm that a weighted quorum acknowledged a given epoch's weight vector before transitioning to the next epoch.

### Commit threshold

Commit requires strictly more than half the total weight (i.e., `yesWeight > totalWeight / 2`).
This is implemented in `quorum.WeightedVoteResult` and `quorum.WeightedCommittedIndex`, which replace the upstream count-based majority on all commit and quorum-active checks.

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Go 1.22+ (for building/testing outside Docker)

### Run a 5-node cluster with a slow node

```powershell
# PowerShell
$env:NODE5_DELAY = "10"
docker compose up --build
```

```bash
# Linux / macOS
NODE5_DELAY=10 docker compose up --build
```

This injects a 10ms artificial fsync delay on node5. Nodes 1–4 run with 0ms delay.

### Propose entries and observe weights

```bash
# POST entries to the leader's /propose endpoint (node1 = port 9091)
curl -X POST http://localhost:9091/propose -d "hello"

# Scrape the wr_raft_weight metric from the leader
curl -s http://localhost:9091/metrics | grep wr_raft_weight
```

Measured example with `NODE5_DELAY=10`:

```
wr_raft_weight{peer_id="1"} 1.16
wr_raft_weight{peer_id="2"} 1.16
wr_raft_weight{peer_id="3"} 1.16
wr_raft_weight{peer_id="4"} 1.16
wr_raft_weight{peer_id="5"} 0.37
```

Node5's weight drops to ~0.37 while fast nodes sit at ~1.16.

## Benchmarking

The same binary supports both WR-Raft and vanilla Raft via the `WR_WEIGHTING` environment variable:

| `WR_WEIGHTING` | Mode | Behaviour |
|---|---|---|
| unset (default) | WR-Raft | EWA weights updated, weighted commit quorum |
| `off` or `false` | Vanilla Raft | Weights stay uniform (all 1.0), commit degenerates to plain majority |

Each node logs its mode at startup:

```
WR-Raft weighting: ENABLED
WR-Raft weighting: DISABLED (vanilla)
```

Same binary, same cluster, same hardware and latency profile — the two modes are directly comparable.

```powershell
# PowerShell — run vanilla Raft
$env:WR_WEIGHTING = "off"
$env:NODE5_DELAY = "10"
docker compose up --build
```

```bash
# Linux / macOS — run vanilla Raft
WR_WEIGHTING=off NODE5_DELAY=10 docker compose up --build
```

## Config Reference

### `raft.Config` fields

| Field | Type | Default | Description |
|---|---|---|---|
| `AlphaBase` | `float64` | `0.2` | EWA smoothing factor. Higher = faster adaptation, more jitter. |
| `FloorEpsilon` | `float64` | `0.05` | Minimum weight any node can hold. Prevents weight collapse to zero. |
| `DisableWeighting` | `bool` | `false` | When `true`, the leader never calls `UpdateEWAWeight`. Weights stay uniform, quorum degenerates to plain majority. |
| `EpochHistoryLogger` | `func(epoch, ct, maxAppended, weights)` | `nil` | Optional callback fired once per epoch transition with a copy of the weight vector. |

### `node-server` flags

| Flag | Default | Description |
|---|---|---|
| `-id` | `node1` | Node ID (`node1`..`node5`) |
| `-grpc-port` | `50051` | gRPC port for inter-node Raft transport |
| `-metrics-port` | `9090` | HTTP port for Prometheus `/metrics`, `/health`, `/status`, `/propose` |
| `-wal-dir` | `/wal` | WAL directory (should be tmpfs in Docker) |
| `-extra-delay-ms` | `0` | Artificial delay in ms added to each storage write |

### Environment variables

| Variable | Description |
|---|---|
| `NODE1_DELAY` .. `NODE5_DELAY` | Passed to `-extra-delay-ms` via `docker-compose.yml` |
| `WR_WEIGHTING` | `off` or `false` disables weighting (vanilla Raft). Unset or any other value = enabled. |

## Known Limitations

### a) Elections are not weighted

Weighted elections as originally designed permit split-brain: weight vectors propagate asynchronously via `AppendEntries`, so two candidates can hold different weight vectors and each independently exceed 0.5 of "total weight" using disjoint voter sets.
This matches the known impossibility result for asynchronous weight reassignment (Bessani et al., ICDCS 2023).
Elections therefore use standard unweighted majority.

### b) Commit-rule intersection gap

The commit rule requires only a weight majority, not also a node-count majority.
With sufficiently skewed weights (e.g., `2.6, 0.6, 0.6, 0.6, 0.6`) a single high-weight node can form a commit quorum alone; if it then fails, a standard count-majority election can elect a leader that never saw that committed entry.

In practice the EWA and weight floor (`epsilon = 0.05`) do not produce such extreme skew, but the guarantee is not formally enforced.
The fix — require BOTH a weight majority AND a count majority, with the count threshold configurable at or above `floor(n/2)+1` — is designed and approved but not yet implemented.

### c) `wr_raft_weight` is populated only on the leader

Only the leader computes and updates weights.
Followers do not have access to the weight vector outside of the epoch context echoed in `AppendEntries`.
Scrape the leader's `/metrics` endpoint for weight observability.

### d) One-batch latency offset

The `StorageWriteLatencyNs` reported on a `MsgAppResp` reflects the most recently completed storage write, not necessarily the write for that exact batch of entries.
This is a one-batch offset.
For an EWA that smooths over many rounds, this is acceptable — a follower consistently writing at 10ms reports ~10ms every round, offset by one batch.
The very first response may report 0.
