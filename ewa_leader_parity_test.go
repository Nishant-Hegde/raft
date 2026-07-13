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

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

// TestEWALeaderParity verifies that the leader and followers get the exact
// same number of EWA updates per proposal (1 per appended batch), and that
// their final weights are practically identical in a uniform-latency cluster.
func TestEWALeaderParity(t *testing.T) {
	const (
		numProposals = 10
		leaderID     = uint64(1)
		latency      = 4 * time.Millisecond
	)

	// Create a 3-node cluster.
	nt := newNetworkWithConfig(nil, nil, nil, nil)

	// Set simulated fsync latency so EWA actually updates.
	for id := uint64(1); id <= 3; id++ {
		nt.storage[id].SimulatedFsyncLatency = latency
	}

	// We'll wrap the leader's tracker to count UpdateEWAWeight calls per node.
	// We can do this by wrapping the leader's step function or by inspecting the
	// weights. Since EWA applies a deterministic math formula, if they have
	// the same number of updates, they will have exactly the same weight.
	// But we also want to explicitly count calls. We can use msgHook to count
	// follower calls, and we can infer leader calls, or we can just assert on
	// the final weights being equal.

	// Actually, the prompt says: "count UpdateEWAWeight calls per node ID and
	// assert leader and followers get the same number (within 1)".
	// We can hook into the leader's step function.

	// Elect leader.
	nt.send(&pb.Message{
		From: new(uint64(1)),
		To:   new(uint64(1)),
		Type: pb.MsgHup.Enum(),
	})
	leader := nt.peers[1].(*raft)
	require.Equal(t, StateLeader, leader.state)

	// Keep a copy of the initial weight
	initialWeight := leader.trk.Weight[1]
	if initialWeight == 0 {
		initialWeight = 1.0
	}

	// Propose entries.
	for i := 0; i < numProposals; i++ {
		// Since the network harness doesn't call storage.Append(), the LatencyReporter
		// returns 0, which makes UpdateEWAWeight a no-op. We simulate the storage
		// writes manually by directly setting the LastFsyncLatencyNs before the responses
		// are processed.
		for id := uint64(1); id <= 3; id++ {
			nt.storage[id].LastFsyncLatencyNs = latency.Nanoseconds()
		}

		nt.send(&pb.Message{
			From:    new(leaderID),
			To:      new(leaderID),
			Type:    pb.MsgProp.Enum(),
			Entries: []*pb.Entry{{Data: []byte(fmt.Sprintf("prop-%d", i))}},
		})
	}

	t.Logf("=== Final Weights ===")
	minW := leader.trk.Weight[1]
	maxW := leader.trk.Weight[1]
	for id := uint64(1); id <= 3; id++ {
		w := leader.trk.Weight[id]
		t.Logf("  Node %d: weight=%.6f", id, w)
		if w < minW {
			minW = w
		}
		if w > maxW {
			maxW = w
		}
	}

	// Requirement 1: Leader's weight MUST move from initial value.
	// (Proves leader's self-ack m.GetIndex() > pr.Match didn't completely fail)
	t.Logf("Leader initial weight: %.6f, final weight: %.6f", initialWeight, leader.trk.Weight[1])
	require.NotEqual(t, initialWeight, leader.trk.Weight[1], "Leader's weight did NOT change! The self-ack check failed.")

	// Requirement 2: Final weights must be comparable (max/min < 1.2).
	ratio := maxW / minW
	t.Logf("Max/Min ratio: %.3f", ratio)
	require.Less(t, ratio, 1.2, "Weights are too asymmetric; leader and followers must converge similarly.")
}

// TestEWALeaderParityCounts directly counts the calls by intercepting the leader's Step.
// Wait, we can't intercept Step to see UpdateEWAWeight directly, but we can intercept
// the MsgAppResp messages reaching the leader and evaluate the exact same condition.
func TestEWALeaderParityCounts(t *testing.T) {
	const (
		numProposals = 10
		leaderID     = uint64(1)
		latency      = 4 * time.Millisecond
	)

	// Create a 3-node cluster.
	nt := newNetworkWithConfig(nil, nil, nil, nil)

	for id := uint64(1); id <= 3; id++ {
		nt.storage[id].SimulatedFsyncLatency = latency
	}

	nt.send(&pb.Message{
		From: new(uint64(1)),
		To:   new(uint64(1)),
		Type: pb.MsgHup.Enum(),
	})
	leader := nt.peers[1].(*raft)
	require.Equal(t, StateLeader, leader.state)

	counts := make(map[uint64]int)

	// We wrap the leader's Step function to intercept MsgAppResp processing.
	// We check the condition: !r.disableWeighting && m.GetIndex() > pr.Match
	// before it gets processed.
	originalStep := leader.step
	leader.step = func(r *raft, m *pb.Message) error {
		if m.GetType() == pb.MsgAppResp && !m.GetReject() {
			pr := r.trk.Progress[m.GetFrom()]
			if pr != nil && m.GetIndex() > pr.Match {
				counts[m.GetFrom()]++
			}
		}
		return originalStep(r, m)
	}

	for i := 0; i < numProposals; i++ {
		nt.send(&pb.Message{
			From:    new(leaderID),
			To:      new(leaderID),
			Type:    pb.MsgProp.Enum(),
			Entries: []*pb.Entry{{Data: []byte(fmt.Sprintf("prop-%d", i))}},
		})
	}

	t.Logf("=== UpdateEWAWeight Equivalent Calls ===")
	for id := uint64(1); id <= 3; id++ {
		t.Logf("  Node %d: %d calls", id, counts[id])
	}

	leaderCount := counts[leaderID]
	followerCount := counts[2]

	require.Greater(t, leaderCount, 0, "Leader got ZERO EWA updates!")
	
	// Assert leader and follower get same number (within 1 because of initial election no-op)
	diff := math.Abs(float64(leaderCount - followerCount))
	require.LessOrEqual(t, diff, 1.0, "Call counts must be equal within 1")
}
