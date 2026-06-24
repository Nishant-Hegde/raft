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

// ewa_weight_test.go — Step 2 tests for the EWA per-follower weight update.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

// makeTracker builds a fresh ProgressTracker with the given voter IDs in
// Voters[0] and a Progress entry for each. No Weight map is set.
func makeTracker(ids ...uint64) tracker.ProgressTracker {
	trk := tracker.MakeProgressTracker(256, 0)
	for _, id := range ids {
		trk.Voters[0][id] = struct{}{}
		trk.Progress[id] = &tracker.Progress{}
	}
	return trk
}

// wsum returns the sum of values in the map.
func wsum(m map[uint64]float64) float64 {
	var s float64
	for _, v := range m {
		s += v
	}
	return s
}

// ── Test 1 ───────────────────────────────────────────────────────────────────

// TestUpdateWeightFormula verifies that a single UpdateEWAWeight call on a
// 3-voter tracker produces exactly the expected weight for the updated node
// using the formula  wi <- alpha*(1/latencyMs) + (1-alpha)*wi_prev.
//
// Setup:
//
//	voters = {1, 2, 3}, no prior weights (wi_prev = 1.0 for all).
//	Call UpdateEWAWeight(1, 4_000_000)  → latency = 4 ms.
//
// Expected raw weight for node 1 (before normalization):
//
//	w1_raw = 0.2 * (1/4.0) + 0.8 * 1.0
//	       = 0.05 + 0.8
//	       = 0.85
//
// Nodes 2 and 3 still default to 1.0 (absent from map).
//
//	total_raw = w1_raw + 1.0 + 1.0 = 2.85
//	scale     = 3 / total_raw
//	w1_final  = w1_raw * scale  ≈ 0.8947368...
func TestUpdateWeightFormula(t *testing.T) {
	const latency = int64(4_000_000) // 4 ms
	trk := makeTracker(1, 2, 3)

	trk.UpdateEWAWeight(1, latency)

	// Analytic computation.
	latencyMs := float64(latency) / 1_000_000.0
	alpha := tracker.EWAlpha
	wPrev := 1.0
	w1raw := alpha*(1.0/latencyMs) + (1-alpha)*wPrev

	// After normalization: sum of raw weights = w1raw + 1.0 + 1.0
	totalRaw := w1raw + 1.0 + 1.0
	scale := 3.0 / totalRaw
	w1want := w1raw * scale
	w2want := 1.0 * scale // nodes 2,3 stayed at default 1.0
	w3want := 1.0 * scale

	got := trk.Weight
	const tol = 1e-12
	assert.InDelta(t, w1want, got[1], tol, "w1 must match analytic formula")
	assert.InDelta(t, w2want, got[2], tol, "w2 must match analytic formula")
	assert.InDelta(t, w3want, got[3], tol, "w3 must match analytic formula")

	gotSum := wsum(got)
	assert.InDelta(t, 3.0, gotSum, tol, "sum of normalized weights must equal n=3")
}

// ── Test 2 ───────────────────────────────────────────────────────────────────

// TestEWANoSignalIsNoOp verifies that when latencyNs <= 0 (no real sample),
// the update behaves as a true mathematical no-op (wRaw = wPrev).
// This ensures that legacy messages without latency data do not perturb
// existing weights and avoids division-by-zero or extreme weight monopolization.
//
// We use a 3-voter cluster with manually set weights that sum to n=3.0.
// This ensures the normalization pass (which scales weights so sum=n) applies
// a scale factor of exactly 1.0, allowing us to observe the true no-op
// without normalization forcing an invalid wPrev back to 1.0.
func TestEWANoSignalIsNoOp(t *testing.T) {
	trk := makeTracker(1, 2, 3)

	// Case A: default wPrev (1.0 for all)
	trk.UpdateEWAWeight(1, 0)
	assert.Equal(t, 1.0, trk.Weight[1], "latencyNs=0 should preserve default wPrev 1.0")

	// Pre-populate weights such that sum = 3.0.
	// We use the exact values requested: 0.5 and 2.3 (plus 0.2 to reach sum 3.0).
	trk.Weight[1] = 0.5
	trk.Weight[2] = 2.3
	trk.Weight[3] = 0.2

	// Case B: wPrev = 0.5
	trk.UpdateEWAWeight(1, 0)
	assert.Equal(t, 0.5, trk.Weight[1], "latencyNs=0 should preserve wPrev 0.5")

	// Case C: wPrev = 2.3
	trk.UpdateEWAWeight(2, 0)
	assert.Equal(t, 2.3, trk.Weight[2], "latencyNs=0 should preserve wPrev 2.3")
}

// ── Test 3 ───────────────────────────────────────────────────────────────────

// TestEWAWeightAdaptsTo2xLatencyChange verifies that when a node's latency changes
// by 2x between rounds, the EWA weight visibly moves in the correct direction.
//
// Scenario: 3-voter cluster. All 3 nodes report latency every round.
//
//   - Phase 1: All 3 nodes report 4 ms each for 40 rounds.
//   - Phase 2: Node 1's latency doubles to 8 ms while nodes 2, 3 remain at 4 ms.
//     Expected after 40 rounds: w1 < phase-1 w1 by a visible margin, and
//     w1 < w2 == w3 (node 1 has lower weight because 1/8ms < 1/4ms).
func TestEWAWeightAdaptsTo2xLatencyChange(t *testing.T) {
	const (
		latencyL  = int64(4_000_000) // 4 ms
		latency2L = int64(8_000_000) // 8 ms (2x latency)
		rounds    = 40
	)
	trk := makeTracker(1, 2, 3)

	t.Logf("[adapts] === Phase 1: %d rounds, all nodes at %d ns (4 ms) ===", rounds, latencyL)
	for i := 0; i < rounds; i++ {
		trk.UpdateEWAWeight(1, latencyL)
		trk.UpdateEWAWeight(2, latencyL)
		trk.UpdateEWAWeight(3, latencyL)
		t.Logf("[adapts]   round %2d  w1=%.9f  w2=%.9f  w3=%.9f  sum=%.9f",
			i+1, trk.Weight[1], trk.Weight[2], trk.Weight[3], wsum(trk.Weight))
	}
	w1afterPhase1 := trk.Weight[1]

	t.Logf("[adapts] phase-1 end: w1=%.9f  w2=%.9f  w3=%.9f  (oscillating near 1.0)",
		trk.Weight[1], trk.Weight[2], trk.Weight[3])

	t.Logf("[adapts] === Phase 2: %d rounds, node1 latency doubles to %d ns (8 ms), nodes 2+3 stay at 4 ms ===",
		rounds, latency2L)
	for i := 0; i < rounds; i++ {
		trk.UpdateEWAWeight(1, latency2L) // node 1: slower
		trk.UpdateEWAWeight(2, latencyL)  // node 2: still 4ms
		trk.UpdateEWAWeight(3, latencyL)  // node 3: still 4ms
		t.Logf("[adapts]   round %2d  w1=%.9f  w2=%.9f  w3=%.9f  sum=%.9f",
			i+1, trk.Weight[1], trk.Weight[2], trk.Weight[3], wsum(trk.Weight))
	}
	w1afterPhase2 := trk.Weight[1]

	t.Logf("[adapts] phase-1 w1=%.9f  phase-2 w1=%.9f  (want: phase2 < phase1)",
		w1afterPhase1, w1afterPhase2)

	// Node 1 switched from 4ms to 8ms (2x latency = lower inverse latency).
	// Its EWA weight must drop visibly below its phase-1 value.
	// Based on the hand-calculated wRaw (0.85 vs 0.825), the weight will drop
	// by roughly 0.35 over 40 rounds (from ~1.05 to ~0.70). We use a safe
	// margin of 0.2 to reflect genuine, meaningful movement.
	const margin = 0.2
	assert.Less(t, w1afterPhase2, w1afterPhase1-margin,
		"node1's weight must decrease visibly when its latency doubles from 4ms to 8ms")

	// Nodes 2 and 3 still report 4ms; they must now outweigh node 1.
	assert.Greater(t, trk.Weight[2], trk.Weight[1],
		"node2 (4ms) must outweigh node1 (8ms)")
	assert.Greater(t, trk.Weight[3], trk.Weight[1],
		"node3 (4ms) must outweigh node1 (8ms)")

	// Sum must remain n=3.
	const tol = 1e-9
	assert.InDelta(t, 3.0, wsum(trk.Weight), tol, "sum must remain n=3 after all updates")
}

// ── Test 4 ───────────────────────────────────────────────────────────────────

// TestNormalizationSumEqualsN verifies that after every individual UpdateEWAWeight
// call, the sum of ALL voter weights is exactly n (where n == number of voters).
func TestNormalizationSumEqualsN(t *testing.T) {
	const n = 5
	ids := []uint64{1, 2, 3, 4, 5}
	trk := makeTracker(ids...)

	latencies := []int64{
		2_000_000, // node 1: 2 ms
		500_000,   // node 2: 0.5 ms
		8_000_000, // node 3: 8 ms
		1_000_000, // node 4: 1 ms
		3_000_000, // node 5: 3 ms
	}

	const tol = 1e-9
	round := 0
	for rep := 0; rep < 3; rep++ { // three full passes over all nodes
		for i, id := range ids {
			round++
			trk.UpdateEWAWeight(id, latencies[i])
			s := wsum(trk.Weight)
			t.Logf("[norm] round %2d  node=%d  latency=%7d ns  sum=%.12f  (want %.1f)",
				round, id, latencies[i], s, float64(n))
			assert.InDelta(t, float64(n), s, tol,
				"sum of weights must equal n=%d after every update", n)
		}
	}
}

// ── Test 5 ───────────────────────────────────────────────────────────────────

// TestEWAWiring verifies that a successful MsgAppResp correctly extracts
// StorageWriteLatencyNs and triggers an EWA weight update in the leader.
func TestEWAWiring(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1, 2, 3))
	r := newTestRaft(1, 10, 1, storage)
	r.becomeCandidate()
	r.becomeLeader()

	// By default, Weight is nil or empty, so weights are implicitly 1.0.
	// Sending an ACK from node 2 with latency > 0 will force the map to initialize
	// and node 2's weight to shift from 1.0.

	latencyNs := int64(4_000_000)
	from := uint64(2)
	to := uint64(1)
	typ := pb.MsgAppResp
	idx := r.raftLog.lastIndex()
	reject := false

	r.Step(&pb.Message{
		From:                  &from,
		To:                    &to,
		Type:                  &typ,
		Index:                 &idx,
		Reject:                &reject,
		StorageWriteLatencyNs: &latencyNs,
	})

	if r.trk.Weight == nil {
		t.Fatalf("trk.Weight is nil; UpdateEWAWeight was not called from MsgAppResp wiring")
	}
	if w := r.trk.Weight[2]; w == 1.0 || w == 0.0 {
		t.Fatalf("trk.Weight[2] = %v; expected weight shift due to EWA update", w)
	}
}
