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

// disable_weighting_test.go — verifies that Config.DisableWeighting gates
// the UpdateEWAWeight call in stepLeader, ensuring the toggle correctly
// switches between WR-Raft (weighted quorum) and vanilla Raft (plain majority).

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
)

// stepLatencyResp steps a MsgAppResp from `from` into the leader `r`,
// carrying the given StorageWriteLatencyNs. The Index is set to the
// leader's last log index so the response is accepted (not rejected).
func stepLatencyResp(r *raft, from uint64, latencyNs int64) {
	idx := r.raftLog.lastIndex()
	reject := false
	to := r.id
	typ := pb.MsgAppResp
	r.Step(&pb.Message{
		From:                  &from,
		To:                    &to,
		Type:                  &typ,
		Index:                 &idx,
		Reject:                &reject,
		StorageWriteLatencyNs: &latencyNs,
	})
}

// TestDisableWeightingPreventsWeightUpdate verifies that with
// DisableWeighting: true, stepping a MsgAppResp carrying a nonzero
// StorageWriteLatencyNs does NOT update trk.Weight. Weights remain
// nil/empty (uniform), making the weighted quorum degenerate to plain
// majority — i.e. vanilla Raft behaviour.
func TestDisableWeightingPreventsWeightUpdate(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1, 2, 3))
	r := newTestRaft(1, 10, 1, storage)
	r.disableWeighting = true
	r.becomeCandidate()
	r.becomeLeader()

	t.Logf("[before] trk.Weight = %v (expect nil or empty)", r.trk.Weight)

	// Step multiple MsgAppResp messages with substantial latency.
	// Without the guard, each of these would trigger UpdateEWAWeight
	// and populate trk.Weight.
	for i := 0; i < 5; i++ {
		stepLatencyResp(r, 2, 10_000_000) // 10 ms
		stepLatencyResp(r, 3, 50_000_000) // 50 ms
	}

	t.Logf("[after]  trk.Weight = %v (expect nil or empty)", r.trk.Weight)

	// With weighting disabled, trk.Weight must remain nil or empty —
	// UpdateEWAWeight was never called, so no weight map was created.
	assert.True(t, len(r.trk.Weight) == 0,
		"DisableWeighting=true: trk.Weight must remain empty; got %v", r.trk.Weight)
}

// TestEnableWeightingUpdatesWeight verifies that with
// DisableWeighting: false (the default), stepping a MsgAppResp carrying
// a nonzero StorageWriteLatencyNs DOES update trk.Weight — the normal
// WR-Raft behaviour. This is the control arm of the toggle test.
func TestEnableWeightingUpdatesWeight(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1, 2, 3))
	r := newTestRaft(1, 10, 1, storage)
	// disableWeighting defaults to false — explicit for clarity.
	r.disableWeighting = false
	r.becomeCandidate()
	r.becomeLeader()

	t.Logf("[before] trk.Weight = %v (expect nil or empty)", r.trk.Weight)

	// Step multiple MsgAppResp messages. With weighting enabled,
	// UpdateEWAWeight populates and adjusts the weight map.
	for i := 0; i < 5; i++ {
		stepLatencyResp(r, 2, 10_000_000) // 10 ms
		stepLatencyResp(r, 3, 50_000_000) // 50 ms
	}

	t.Logf("[after]  trk.Weight = %v (expect non-empty with shifted values)", r.trk.Weight)

	// With weighting enabled, trk.Weight must be non-nil and contain
	// entries for the peers that sent latency reports.
	require.NotNil(t, r.trk.Weight,
		"DisableWeighting=false: trk.Weight must be populated after MsgAppResp with latency")
	assert.Greater(t, len(r.trk.Weight), 0,
		"DisableWeighting=false: trk.Weight must have entries")

	// Node 2 (10ms) should have a different weight than node 3 (50ms),
	// proving the EWA is actually differentiating storage speeds.
	w2 := r.trk.Weight[2]
	w3 := r.trk.Weight[3]
	t.Logf("[detail] weight[2] = %.4f (10ms latency)", w2)
	t.Logf("[detail] weight[3] = %.4f (50ms latency)", w3)
	assert.NotEqual(t, w2, w3,
		"weights for peers with different latencies must differ")
}
