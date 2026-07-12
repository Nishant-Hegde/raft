// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

// leader_self_weight_test.go — verifies that the leader's own EWA weight is
// comparable to peers' in a uniform-latency cluster. Before the fix, the
// leader's self-directed MsgAppResp carried no StorageWriteLatencyNs,
// causing UpdateEWAWeight(self, 0) to no-op. The leader's raw weight stayed
// at 1.0 while followers dropped toward 1/latencyMs, and normalization
// inflated the leader to ~2.5-2.9x the peers.

import (
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// TestLeaderSelfWeightUniformCluster sets up a 3-node cluster where every
// node (including the leader) has the same simulated fsync latency. After
// several rounds of proposals + MsgAppResp, all weights should be roughly
// equal. No single node should exceed 2x the minimum.
func TestLeaderSelfWeightUniformCluster(t *testing.T) {
	const latency = 4 * time.Millisecond // uniform for all nodes

	// Create storages with identical simulated fsync latency.
	s1 := newTestMemoryStorage(withPeers(1, 2, 3))
	s1.SimulatedFsyncLatency = latency
	s2 := newTestMemoryStorage(withPeers(1, 2, 3))
	s2.SimulatedFsyncLatency = latency
	s3 := newTestMemoryStorage(withPeers(1, 2, 3))
	s3.SimulatedFsyncLatency = latency

	r := newTestRaft(1, 10, 1, s1)
	r.becomeCandidate()
	r.becomeLeader()

	// Drain the leader's initial unstable entries (the no-op) and persist
	// them so the storage records its latency before the self-ack fires.
	unstable := r.raftLog.nextUnstableEnts()
	if err := s1.Append(unstable); err != nil {
		t.Fatalf("failed to append leader unstable entries: %v", err)
	}

	// Simulate several rounds: propose an entry, then step MsgAppResp from
	// each follower (with their storage latency), mimicking what the real
	// Ready loop does.
	for round := 0; round < 10; round++ {
		// Propose an entry — this triggers appendEntry, which sends a
		// self-directed MsgAppResp (now with StorageWriteLatencyNs).
		r.Step(&pb.Message{
			From:    new(uint64(1)),
			To:      new(uint64(1)),
			Type:    pb.MsgProp.Enum(),
			Entries: []*pb.Entry{{Data: []byte("data")}},
		})

		// Persist the leader's own entries so the LatencyReporter has
		// a fresh sample before the next self-ack.
		unstable = r.raftLog.nextUnstableEnts()
		if len(unstable) > 0 {
			if err := s1.Append(unstable); err != nil {
				t.Fatalf("round %d: failed to append: %v", round, err)
			}
		}

		// Step MsgAppResp from followers 2 and 3 with their latency.
		for _, followerID := range []uint64{2, 3} {
			idx := r.raftLog.lastIndex()
			reject := false
			to := uint64(1)
			typ := pb.MsgAppResp
			lat := s2.LastFsyncLatencyNs // same for s2 and s3
			r.Step(&pb.Message{
				From:                  &followerID,
				To:                    &to,
				Type:                  &typ,
				Index:                 &idx,
				Reject:                &reject,
				StorageWriteLatencyNs: &lat,
			})
		}
	}

	// Check weights.
	if len(r.trk.Weight) == 0 {
		t.Fatal("trk.Weight is empty; UpdateEWAWeight was never called")
	}

	t.Logf("Weights after 10 rounds (uniform %v latency):", latency)
	minW := r.trk.Weight[1]
	maxW := r.trk.Weight[1]
	for id := uint64(1); id <= 3; id++ {
		w := r.trk.Weight[id]
		t.Logf("  node %d: weight = %.4f", id, w)
		if w < minW {
			minW = w
		}
		if w > maxW {
			maxW = w
		}
	}

	// In a uniform cluster, no node's weight should exceed 2x the minimum.
	// Before the fix, the leader was ~4-5x the followers.
	ratio := maxW / minW
	t.Logf("  max/min ratio = %.2f (must be < 2.0)", ratio)
	if ratio >= 2.0 {
		t.Errorf("weight ratio %.2f >= 2.0: leader weight is inflated; "+
			"expected roughly uniform weights in a uniform-latency cluster", ratio)
	}
}
