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

// ewa_callcount_investigation_test.go — investigates whether the leader gets
// fewer UpdateEWAWeight calls than followers, which would explain why the
// leader's normalized weight remains inflated after the self-ack fix.
//
// Hypothesis: the leader self-acks once per appendEntry (once per proposal),
// but followers can send MsgAppResp multiple times per proposal (once for the
// initial MsgApp, and again for the commit-advancing MsgApp from bcastAppend).
// This means UpdateEWAWeight is called fewer times for the leader's own ID
// than for each follower, causing the leader's EWA to converge more slowly.

import (
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)


// TestEWACallCountNetworkHarness runs the same investigation through the
// network test harness to see the full message cascade, including the
// commit-triggered bcastAppend.
func TestEWACallCountNetworkHarness(t *testing.T) {
	const (
		numProposals = 10
		leaderID     = uint64(1)
		latency      = 1 * time.Millisecond
	)

	// msgAppResp counts per source node.
	counts := map[uint64]int{}

	nt := newNetworkWithConfig(nil, nil, nil, nil)

	// Set a hook to count ALL MsgAppResp delivered to the leader.
	nt.msgHook = func(m *pb.Message) bool {
		if m.GetType() == pb.MsgAppResp && m.GetTo() == leaderID && !m.GetReject() {
			counts[m.GetFrom()]++
		}
		return true
	}

	// Elect leader.
	nt.send(&pb.Message{
		From: new(uint64(1)),
		To:   new(uint64(1)),
		Type: pb.MsgHup.Enum(),
	})
	leader := nt.peers[1].(*raft)
	if leader.state != StateLeader {
		t.Fatalf("node 1 not leader")
	}

	// Set fsync latency.
	for id := uint64(1); id <= 3; id++ {
		nt.storage[id].SimulatedFsyncLatency = latency
	}

	// Reset counts after election.
	counts = map[uint64]int{}

	// Propose.
	for i := 0; i < numProposals; i++ {
		nt.send(&pb.Message{
			From:    new(leaderID),
			To:      new(leaderID),
			Type:    pb.MsgProp.Enum(),
			Entries: []*pb.Entry{{Data: []byte(fmt.Sprintf("p%d", i))}},
		})
	}

	// NOTE: msgHook only sees messages that go through nw.send() → filter().
	// The leader's self-ack goes through advanceMessagesAfterAppend() → r.Step()
	// internally, bypassing the network. So the leader count here will be 0.

	t.Logf("=== Network harness MsgAppResp counts (to leader) ===")
	t.Logf("NOTE: leader self-ack bypasses msgHook (internal path)")
	t.Logf("")
	for id := uint64(1); id <= 3; id++ {
		role := "follower"
		if id == leaderID {
			role = "LEADER (via msgHook — misses self-ack)"
		}
		t.Logf("  node %d (%s): %d", id, role, counts[id])
	}

	t.Logf("")
	t.Logf("  Each follower sends %d MsgAppResp per proposal (%.1fx per proposal)",
		counts[2], float64(counts[2])/float64(numProposals))
	t.Logf("  The leader self-acks once per proposal via appendEntry → advanceMessagesAfterAppend")
	t.Logf("  So per proposal: leader=1, each follower=%.1f", float64(counts[2])/float64(numProposals))

	t.Logf("")
	t.Logf("=== Final weights ===")
	for id := uint64(1); id <= 3; id++ {
		t.Logf("  node %d: weight=%.6f", id, leader.trk.Weight[id])
	}

	if counts[2] > numProposals {
		t.Logf("")
		t.Logf("*** CONFIRMED: followers get %d MsgAppResp per %d proposals (%.1fx per proposal) ***",
			counts[2], numProposals, float64(counts[2])/float64(numProposals))
		t.Logf("*** The leader only self-acks 1x per proposal. ***")
		t.Logf("*** This %.1fx asymmetry causes the leader's EWA to converge slower. ***",
			float64(counts[2])/float64(numProposals))
	}
}
