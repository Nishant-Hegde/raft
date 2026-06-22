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
