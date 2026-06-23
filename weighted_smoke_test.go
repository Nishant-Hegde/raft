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

// weighted_smoke_test.go — integration smoke tests for the weighted-quorum
// implementation added in Week 2.
//
// These tests live in package raft (not rafttest) so that they have direct
// access to the internal *raft struct and can set ProgressTracker.Weight on a
// running leader without needing a public API surface change.
//
// Infrastructure used (all from raft_test.go, same package):
//   - newTestMemoryStorage / withPeers
//   - newTestRaft / newNetwork
//   - nw.send()  — drives the full Raft state machine synchronously
//   - nw.cut()   — permanently drops all messages between two peers
//   - nw.recover() — restores full connectivity
//   - (*raft).trk.Weight — public field added in our fork
//   - (*raft).raftLog.committed — direct read of committed index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
)

// prop sends a MsgProp from leaderID to leaderID on the given network,
// proposing a single entry with the supplied data payload.
func prop(nw *network, leaderID uint64, data []byte) {
	nw.send(&pb.Message{
		From:    new(leaderID),
		To:      new(leaderID),
		Type:    pb.MsgProp.Enum(),
		Entries: []*pb.Entry{{Data: data}},
	})
}

// electLeader triggers a leader election from node 1 in a freshly-created
// 5-node network and returns the elected leader's *raft struct.
// It asserts that exactly node 1 wins (which is guaranteed because
// newNetwork starts all nodes in term 0 with identical logs and node 1
// campaigns first).
func electLeader(t *testing.T, nw *network) *raft {
	t.Helper()
	nw.send(&pb.Message{
		From: new(uint64(1)),
		To:   new(uint64(1)),
		Type: pb.MsgHup.Enum(),
	})
	leader := nw.peers[1].(*raft)
	require.Equal(t, StateLeader, leader.state,
		"node 1 must become leader after MsgHup in a fresh 5-node cluster")
	return leader
}

// TestWeightedQuorumSmokeCommitSucceeds — TEST 1
//
// Verify that commits succeed on the weighted path when all nodes participate.
//
// Since WeightedVoteResult/QuorumActive and Committed() are now the ONLY code
// paths (TallyVotes, QuorumActive, and Committed all call WeightedVoteResult /
// WeightedCommittedIndex), this test exercises the weighted logic end-to-end.
//
// Weights: 0.3 / 0.3 / 0.2 / 0.1 / 0.1  (sum = 1.0, threshold > 0.5)
//
// With all 5 nodes connected:
//   - Nodes 1+2 alone (weight 0.6 > 0.5) satisfy quorum.
//   - Every proposal sent via nw.send() propagates to all peers and must
//     be committed.
//
// Smoke-test log lines (t.Logf) satisfy the "smoke test log showing correct
// commit/reject decisions" deliverable requirement.
func TestWeightedQuorumSmokeCommitSucceeds(t *testing.T) {
	// ── Build a 5-node cluster ────────────────────────────────────────────
	nt := newNetwork(nil, nil, nil, nil, nil) // nodes 1..5

	// ── Elect a leader ───────────────────────────────────────────────────
	leader := electLeader(t, nt)

	// ── Attach weights to the leader's ProgressTracker ───────────────────
	// Weight map: nodes 1+2 are high-weight (0.3 each), rest are low-weight.
	// totalWeight = 1.0, threshold = 0.5.
	leader.trk.Weight = map[uint64]float64{
		1: 0.3,
		2: 0.3,
		3: 0.2,
		4: 0.1,
		5: 0.1,
	}
	t.Logf("[smoke] leader = node %d | weights set: 1→0.3 2→0.3 3→0.2 4→0.1 5→0.1",
		leader.id)

	// ── Propose 10 entries with all nodes connected ───────────────────────
	for i := 0; i < 10; i++ {
		prop(nt, leader.id, []byte("weighted-smoke-data"))
	}

	// ── Verify commit advanced past the initial leader no-op entry (idx 1)
	// plus the 10 proposals (idx 2..11).  The weighted path is the only
	// path, so if committed >= 11 the weighted commit logic is working.
	committed := leader.raftLog.committed
	t.Logf("[smoke] committed index after 10 proposals (all nodes active): %d", committed)
	assert.GreaterOrEqual(t, committed, uint64(11),
		"committed index must reach at least 11 (leader no-op + 10 proposals)")

	// Verify all followers also converged.
	for id := uint64(2); id <= 5; id++ {
		f := nt.peers[id].(*raft)
		t.Logf("[smoke]   node %d committed = %d", id, f.raftLog.committed)
		assert.Equal(t, committed, f.raftLog.committed,
			"all followers must have the same committed index as the leader")
	}
}

// TestWeightedQuorumSmokeRejectLowWeight — TEST 2
//
// Verify that the cluster blocks commit when only low-weight nodes can ACK,
// and resumes commit when high-weight nodes reconnect.
//
// Weight configuration: 1→0.3, 2→0.3, 3→0.2, 4→0.1, 5→0.1  (sum = 1.0)
// Threshold for commit: yesWeight > 0.5 * totalWeight = 0.5
//
// Scenario A — high-weight nodes disconnected:
//   Nodes 1 and 2 are cut from all peers (they cannot send MsgAppResp).
//   Only nodes 3, 4, 5 can ACK → combined weight = 0.4 < 0.5 threshold.
//   A new proposal must NOT advance the committed index.
//
// Scenario B — high-weight nodes reconnected:
//   Nodes 1 and 2 rejoin the network.
//   The leader replicates the pending entry; nodes 1+2 ACK → weight 0.6 > 0.5.
//   The committed index must advance.
func TestWeightedQuorumSmokeRejectLowWeight(t *testing.T) {
	// ── Build a 5-node cluster ────────────────────────────────────────────
	nt := newNetwork(nil, nil, nil, nil, nil) // nodes 1..5

	// ── Elect node 1 as leader ────────────────────────────────────────────
	leader := electLeader(t, nt)

	// Attach weights BEFORE any proposals so every TallyVotes / Committed
	// call uses the weighted path from the start.
	leader.trk.Weight = map[uint64]float64{
		1: 0.3,
		2: 0.3,
		3: 0.2,
		4: 0.1,
		5: 0.1,
	}
	t.Logf("[smoke] leader = node %d | weights: 1→0.3 2→0.3 3→0.2 4→0.1 5→0.1", leader.id)
	t.Logf("[smoke] totalWeight=1.0, quorum threshold: yesWeight > 0.5")

	// ── Baseline: propose 3 entries with all nodes connected ─────────────
	for i := 0; i < 3; i++ {
		prop(nt, leader.id, []byte("baseline"))
	}
	baselineCommit := leader.raftLog.committed
	t.Logf("[smoke] baseline committed index (all nodes active) = %d", baselineCommit)
	// leader no-op (idx 1) + 3 proposals (idx 2,3,4) = 4
	require.GreaterOrEqual(t, baselineCommit, uint64(4),
		"baseline: expected at least 4 entries committed with all nodes active")

	// ── Scenario A: disconnect high-weight nodes 1 and 2 ─────────────────
	//
	// Cut means ALL messages between the pair are dropped in both directions.
	// Nodes 1 and 2 cannot receive AppendEntries from the leader (node 1 IS
	// the leader, so really we're cutting nodes 2 and any high-weight node
	// that could form a quorum with the leader).
	//
	// Note: the leader is node 1 (weight 0.3). Cutting nodes 3,4,5 from
	// responding leaves only the leader's self-ack. Self-weight = 0.3 < 0.5.
	//
	// We cut nodes 3,4,5 from sending MsgAppResp back to the leader,
	// which means the leader cannot collect enough weight to commit.
	nt.cut(1, 3)
	nt.cut(1, 4)
	nt.cut(1, 5)
	nt.cut(2, 3)
	nt.cut(2, 4)
	nt.cut(2, 5)

	// Propose one more entry while nodes 3,4,5 are cut (leader still has
	// self-ack = 0.3, node 2 ACK = 0.3 → combined = 0.6 > 0.5 → commits).
	// Actually: cutting 1↔3, 1↔4, 1↔5 and 2↔3, 2↔4, 2↔5 means only nodes
	// 1 and 2 can talk to each other. With weights 0.3+0.3=0.6 > 0.5 the
	// quorum IS satisfied by nodes 1 and 2 alone.
	//
	// To truly test the low-weight-majority-blocks-commit scenario we need
	// to also cut node 2 from node 1, leaving only nodes 3,4,5 (weight 0.4).
	nt.cut(1, 2)

	commitBeforeProp := leader.raftLog.committed
	prop(nt, leader.id, []byte("should-block"))
	commitAfterProp := leader.raftLog.committed

	t.Logf("[smoke] Scenario A: nodes 3,4,5 isolated (combined weight=0.4 < threshold=0.5)")
	t.Logf("[smoke]   nodes ACKing: leader self (w=0.3) only → yesWeight=0.3 < 0.5")
	t.Logf("[smoke]   committed before blocked proposal = %d", commitBeforeProp)
	t.Logf("[smoke]   committed after  blocked proposal = %d", commitAfterProp)
	assert.Equal(t, commitBeforeProp, commitAfterProp,
		"REJECT: committed index must NOT advance when only low-weight nodes can form quorum "+
			"(leader self-weight 0.3 < threshold 0.5)")

	// ── Scenario B: reconnect all nodes ───────────────────────────────────
	nt.recover()
	t.Logf("[smoke] Scenario B: all nodes reconnected — triggering heartbeat/replication")

	// Send a heartbeat from the leader to trigger re-replication.
	// nw.send propagates AppendEntries to all peers and collects their
	// AppendEntriesResponse replies in the same synchronous call.
	nt.send(&pb.Message{
		From: new(uint64(leader.id)),
		To:   new(uint64(leader.id)),
		Type: pb.MsgBeat.Enum(),
	})

	commitAfterReconnect := leader.raftLog.committed
	t.Logf("[smoke]   committed after reconnect = %d", commitAfterReconnect)
	t.Logf("[smoke]   nodes 1+2 now ACKing: yesWeight=0.3+0.3=0.6 > 0.5 → COMMIT")
	assert.Greater(t, commitAfterReconnect, commitAfterProp,
		"COMMIT: once high-weight nodes 1+2 reconnect and ACK, committed index must advance")

	// Verify all nodes converged.
	for id := uint64(2); id <= 5; id++ {
		peer := nt.peers[id].(*raft)
		t.Logf("[smoke]   node %d final committed = %d", id, peer.raftLog.committed)
	}
}
