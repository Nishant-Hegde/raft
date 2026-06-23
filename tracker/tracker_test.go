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

package tracker

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/quorum"
)

// makeTracker is a convenience helper that builds a ProgressTracker with a
// single-majority voter config and pre-populated match indexes.
//
// voters is the slice of voter IDs; matches maps each voter to its Match value.
// Weight is intentionally left nil so callers can set it when needed.
func makeTracker(voters []uint64, matches map[uint64]uint64) ProgressTracker {
	pt := MakeProgressTracker(256, 0)
	cfg := quorum.MajorityConfig{}
	for _, id := range voters {
		cfg[id] = struct{}{}
		pt.Progress[id] = &Progress{Match: matches[id]}
	}
	pt.Voters[0] = cfg
	return pt
}

// TestCommittedBackwardCompatibilityNilWeight proves that Committed() with
// Weight == nil produces the same result as the old unweighted CommittedIndex()
// for several different match-index configurations.
//
// Each case is checked against p.Voters.CommittedIndex(matchAckIndexer(p.Progress))
// — the exact expression that Committed() used to call — so a divergence here
// would flag a real regression.
func TestCommittedBackwardCompatibilityNilWeight(t *testing.T) {
	tests := []struct {
		desc    string
		voters  []uint64
		matches map[uint64]uint64
	}{
		{
			desc:   "singleton leader",
			voters: []uint64{1},
			matches: map[uint64]uint64{
				1: 42,
			},
		},
		{
			desc:   "3-node cluster, all at same index",
			voters: []uint64{1, 2, 3},
			matches: map[uint64]uint64{
				1: 10,
				2: 10,
				3: 10,
			},
		},
		{
			desc:   "3-node cluster, mixed indexes",
			voters: []uint64{1, 2, 3},
			matches: map[uint64]uint64{
				1: 12,
				2: 5,
				3: 6,
			},
		},
		{
			desc:   "5-node cluster, one voter not yet reported",
			voters: []uint64{1, 2, 3, 4, 5},
			matches: map[uint64]uint64{
				1: 101,
				2: 104,
				3: 103,
				4: 103,
				5: 0, // not yet reported (Match == 0)
			},
		},
		{
			desc:   "5-node cluster, varying indexes",
			voters: []uint64{1, 2, 3, 4, 5},
			matches: map[uint64]uint64{
				1: 10,
				2: 10,
				3: 8,
				4: 5,
				5: 5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			p := makeTracker(tt.voters, tt.matches)
			// p.Weight is nil — every voter defaults to 1.0.

			// Reference: the old, unweighted expression.
			expected := uint64(p.Voters.CommittedIndex(matchAckIndexer(p.Progress)))
			got := p.Committed()

			require.Equal(t, expected, got,
				"Committed() with nil Weight must match unweighted CommittedIndex()")
		})
	}
}

// TestCommittedEmptyWeightMapIsBackwardCompatible is the same check with an
// explicitly allocated but empty Weight map (not nil), which must also behave
// identically because every voter is absent from the map and defaults to 1.0.
func TestCommittedEmptyWeightMapIsBackwardCompatible(t *testing.T) {
	p := makeTracker(
		[]uint64{1, 2, 3, 4, 5},
		map[uint64]uint64{1: 10, 2: 10, 3: 8, 4: 5, 5: 5},
	)
	p.Weight = map[uint64]float64{} // empty, not nil

	expected := uint64(p.Voters.CommittedIndex(matchAckIndexer(p.Progress)))
	got := p.Committed()
	require.Equal(t, expected, got,
		"Committed() with empty Weight map must match unweighted CommittedIndex()")
}

// TestCommittedWeightedLiveWiring verifies that ProgressTracker.Committed()
// actually reflects per-voter weights when Weight is populated.
//
// 5-node cluster with weights 0.3/0.3/0.2/0.1/0.1 and match indexes
// 10/10/8/5/5: nodes 1+2 together hold 0.6 >= 0.5*1.0, so the weighted
// committed index is 10 — whereas the unweighted (simple majority) result
// from CommittedIndex() would be 8.
//
// This test directly demonstrates that the live wiring works: Committed()
// returns the weighted result (10), not the unweighted result (8).
func TestCommittedWeightedLiveWiring(t *testing.T) {
	p := makeTracker(
		[]uint64{1, 2, 3, 4, 5},
		map[uint64]uint64{1: 10, 2: 10, 3: 8, 4: 5, 5: 5},
	)
	p.Weight = map[uint64]float64{
		1: 0.3,
		2: 0.3,
		3: 0.2,
		4: 0.1,
		5: 0.1,
	}

	// Unweighted majority result for reference: sorted ascending [5,5,8,10,10],
	// pos = 5-(5/2+1) = 2 → index 8.
	unweighted := uint64(p.Voters.CommittedIndex(matchAckIndexer(p.Progress)))
	require.Equal(t, uint64(8), unweighted, "sanity: unweighted committed index should be 8")

	// Weighted result: nodes 1+2 (weight 0.6) >= threshold 0.5*1.0 → index 10.
	got := p.Committed()
	require.Equal(t, uint64(10), got,
		"Committed() with non-uniform weights should return weighted committed index 10")

	// The two results must differ, proving the live wiring actually uses weights.
	require.NotEqual(t, unweighted, got,
		"weighted and unweighted results should differ for this configuration")
}

// TestCommittedWeightedPartialAck verifies that a single high-weight voter
// cannot self-satisfy quorum when other voters have not yet reported in.
// This exercises the denominator-correctness fix from the previous session:
// totalWeight includes ALL configured voters, not only those that have acked.
//
//	Node 1: weight 0.3, Match 100   ← only reporter
//	Nodes 2-5: weight 1.0 (default), Match 0 (not yet reported)
//
// totalWeight = 0.3 + 4*1.0 = 4.3; threshold = 2.15.
// Node 1 contributes 0.3 << 2.15 → Committed() must return 0.
func TestCommittedWeightedPartialAck(t *testing.T) {
	p := makeTracker(
		[]uint64{1, 2, 3, 4, 5},
		map[uint64]uint64{
			1: 100,
			// 2, 3, 4, 5 absent from matches → Match defaults to 0
		},
	)
	// Ensure nodes 2-5 are in Progress with Match == 0 (no ack yet).
	for _, id := range []uint64{2, 3, 4, 5} {
		p.Progress[id] = &Progress{Match: 0}
	}
	p.Weight = map[uint64]float64{
		1: 0.3,
		// 2-5 absent → default 1.0 each
	}

	got := p.Committed()
	require.Equal(t, uint64(0), got,
		"single voter with acked Match cannot self-satisfy quorum when 4 peers have Match==0")
}

// makeActiveTracker builds a ProgressTracker whose voters have RecentActive
// set according to the activeSet map (true = active, false or absent = inactive).
// Weight is intentionally left nil so callers can set it when needed.
func makeActiveTracker(voters []uint64, activeSet map[uint64]bool) ProgressTracker {
	pt := MakeProgressTracker(256, 0)
	cfg := quorum.MajorityConfig{}
	for _, id := range voters {
		cfg[id] = struct{}{}
		pt.Progress[id] = &Progress{RecentActive: activeSet[id]}
	}
	pt.Voters[0] = cfg
	return pt
}

// TestQuorumActiveNilWeightBackwardCompatibility (test a):
// 5-node cluster, nil Weight, 3 of 5 nodes are RecentActive=true.
// Unweighted majority = 3 >= floor(5/2)+1 = 3 → QuorumActive must return true.
func TestQuorumActiveNilWeightBackwardCompatibility(t *testing.T) {
	p := makeActiveTracker(
		[]uint64{1, 2, 3, 4, 5},
		map[uint64]bool{1: true, 2: true, 3: true, 4: false, 5: false},
	)
	// p.Weight is nil — backward-compatible path.
	require.True(t, p.QuorumActive(),
		"QuorumActive with nil Weight: 3/5 active nodes should satisfy simple majority")
}

// TestQuorumActiveHighWeightMinorityActive (test b):
// 5-node cluster with weights 0.3/0.3/0.2/0.1/0.1. Only nodes 1 and 2 are
// active (total active weight = 0.6 >= threshold 0.5). Even though only 2 of 5
// nodes are active, QuorumActive should return true because high-weight nodes
// hold the majority of weight.
func TestQuorumActiveHighWeightMinorityActive(t *testing.T) {
	p := makeActiveTracker(
		[]uint64{1, 2, 3, 4, 5},
		map[uint64]bool{1: true, 2: true, 3: false, 4: false, 5: false},
	)
	p.Weight = map[uint64]float64{
		1: 0.3,
		2: 0.3,
		3: 0.2,
		4: 0.1,
		5: 0.1,
	}
	// Active weight: 0.3+0.3 = 0.6; totalWeight = 1.0; threshold = 0.5.
	// 0.6 > 0.5 → VoteWon → QuorumActive returns true.
	require.True(t, p.QuorumActive(),
		"QuorumActive: nodes 1+2 (weight 0.6) active should satisfy weighted quorum")
}

// TestQuorumActiveLowWeightMajorityActive (test c):
// Same 5-node cluster. Nodes 3,4,5 are active (weight 0.2+0.1+0.1 = 0.4),
// but nodes 1,2 (weight 0.6) are NOT active. Weighted quorum is NOT satisfied.
func TestQuorumActiveLowWeightMajorityActive(t *testing.T) {
	p := makeActiveTracker(
		[]uint64{1, 2, 3, 4, 5},
		map[uint64]bool{1: false, 2: false, 3: true, 4: true, 5: true},
	)
	p.Weight = map[uint64]float64{
		1: 0.3,
		2: 0.3,
		3: 0.2,
		4: 0.1,
		5: 0.1,
	}
	// Active weight: 0.2+0.1+0.1 = 0.4; totalWeight = 1.0; threshold = 0.5.
	// 0.4 < 0.5 → cannot reach threshold (no missing voters) → VoteLost.
	require.False(t, p.QuorumActive(),
		"QuorumActive: nodes 3+4+5 (weight 0.4) active should NOT satisfy weighted quorum")
}

// TestWeightedVoteResultDirect (test d):
// Exercises MajorityConfig.WeightedVoteResult directly with three sub-cases.
func TestWeightedVoteResultDirect(t *testing.T) {
	cfg := quorum.MajorityConfig{
		1: struct{}{},
		2: struct{}{},
		3: struct{}{},
		4: struct{}{},
		5: struct{}{},
	}
	w := trackerWeightConfig(map[uint64]float64{
		1: 0.3,
		2: 0.3,
		3: 0.2,
		4: 0.1,
		5: 0.1,
	})

	t.Run("VoteWon: nodes 1+2 voted yes, others no", func(t *testing.T) {
		votes := map[uint64]bool{
			1: true,
			2: true,
			3: false,
			4: false,
			5: false,
		}
		// yesWeight=0.6, totalWeight=1.0, threshold=0.5. 0.6 > 0.5 → VoteWon.
		require.Equal(t, quorum.VoteWon, cfg.WeightedVoteResult(votes, w),
			"nodes 1+2 (weight 0.6) voted yes → VoteWon")
	})

	t.Run("VoteLost: only nodes 3+4+5 voted yes, all others voted no", func(t *testing.T) {
		votes := map[uint64]bool{
			1: false,
			2: false,
			3: true,
			4: true,
			5: true,
		}
		// yesWeight=0.4, missingWeight=0.0 (all voted), threshold=0.5.
		// 0.4 < 0.5 and 0.4+0.0 < 0.5 → VoteLost.
		require.Equal(t, quorum.VoteLost, cfg.WeightedVoteResult(votes, w),
			"nodes 3+4+5 (weight 0.4) voted yes, all 5 voted → VoteLost")
	})

	t.Run("VotePending: only node 1 voted yes, nodes 2-5 haven't voted", func(t *testing.T) {
		votes := map[uint64]bool{
			1: true,
			// 2,3,4,5 absent — not yet voted
		}
		// yesWeight=0.3, missingWeight=0.3+0.2+0.1+0.1=0.7, threshold=0.5.
		// 0.3 < 0.5 but 0.3+0.7=1.0 >= 0.5 → VotePending.
		require.Equal(t, quorum.VotePending, cfg.WeightedVoteResult(votes, w),
			"node 1 (weight 0.3) yes + nodes 2-5 pending → VotePending")
	})
}
