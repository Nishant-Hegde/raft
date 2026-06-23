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

package quorum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWeightedCommittedIndex verifies WeightedCommittedIndex with two cases:
//
//  1. Non-uniform weights — nodes 1 and 2 alone (weight 0.3+0.3=0.6) exceed
//     the 0.5 threshold at index 10, so the committed index must be 10.
//
//  2. Equal weights (0.2 each for all 5 nodes) — the weighted function must
//     produce the same result as a plain unweighted MajorityConfig for the
//     same acked indices, demonstrating graceful reduction to standard majority
//     behavior.
func TestWeightedCommittedIndex(t *testing.T) {
	// Shared acked indices used by both test cases.
	acked := map[uint64]Index{
		1: 10,
		2: 10,
		3: 8,
		4: 5,
		5: 5,
	}

	t.Run("non-uniform weights", func(t *testing.T) {
		// Nodes 1+2 hold weight 0.6 > 0.5*1.0, so index 10 is committed.
		weights := map[uint64]float64{
			1: 0.3,
			2: 0.3,
			3: 0.2,
			4: 0.1,
			5: 0.1,
		}
		got := WeightedCommittedIndex(weights, acked)
		require.Equal(t, Index(10), got, "non-uniform weights: expected committed index 10")
	})

	t.Run("equal weights reduce to majority", func(t *testing.T) {
		// With equal weights of 0.2 each, the weighted quorum threshold is
		// equivalent to a simple majority (>= 3 of 5 nodes).
		weights := map[uint64]float64{
			1: 0.2,
			2: 0.2,
			3: 0.2,
			4: 0.2,
			5: 0.2,
		}

		// Compute the reference result using the standard MajorityConfig.
		// For acked indices [10, 10, 8, 5, 5] sorted ascending: [5,5,8,10,10].
		// pos = 5 - (5/2 + 1) = 2 → index 8.
		mc := MajorityConfig{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}
		expected := mc.CommittedIndex(mapAckIndexer(acked))

		got := WeightedCommittedIndex(weights, acked)
		require.Equal(t, expected, got,
			"equal weights: weighted result should match unweighted MajorityConfig (%v)", expected)
	})
}

// mapWeightConfig is a test-only WeightedConfig backed by a plain map.
type mapWeightConfig map[uint64]float64

func (m mapWeightConfig) Weight(id uint64) (float64, bool) {
	w, ok := m[id]
	return w, ok
}

// TestMajorityConfigWeightedCommittedIndex verifies MajorityConfig.WeightedCommittedIndex
// using the 5-node example from the task, where every voter has acked:
//
//	Node 1: weight 0.3, acked index 10
//	Node 2: weight 0.3, acked index 10
//	Node 3: weight 0.2, acked index 8
//	Node 4: weight 0.1, acked index 5
//	Node 5: weight 0.1, acked index 5
//
// totalWeight = 1.0. Nodes 1+2 together hold cumulative weight 0.6 >= 0.5*1.0,
// so the committed index must be 10.
func TestMajorityConfigWeightedCommittedIndex(t *testing.T) {
	mc := MajorityConfig{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

	l := mapAckIndexer{
		1: 10,
		2: 10,
		3: 8,
		4: 5,
		5: 5,
	}
	w := mapWeightConfig{
		1: 0.3,
		2: 0.3,
		3: 0.2,
		4: 0.1,
		5: 0.1,
	}

	got := mc.WeightedCommittedIndex(l, w)
	require.Equal(t, Index(10), got,
		"5-node non-uniform weights (all acked): expected committed index 10")
}

// TestMajorityConfigWeightedCommittedIndexDefaultWeight verifies that voters
// absent from the WeightedConfig default to weight 1.0. Nodes 4 and 5 have
// no entry in the weight map, so they each contribute 1.0.
//
//	Node 1: weight 0.3, acked index 10
//	Node 2: weight 0.3, acked index 10
//	Node 3: weight 0.2, acked index 8
//	Node 4: weight 1.0 (default), acked index 5
//	Node 5: weight 1.0 (default), acked index 5
//
// totalWeight = 0.3+0.3+0.2+1.0+1.0 = 2.8. threshold = 1.4.
// Descending walk:
//
//	idx=10: cum = 0.6        (< 1.4)
//	idx=8:  cum = 0.8        (< 1.4)
//	idx=5:  cum = 0.8+2.0 = 2.8  (>= 1.4) → committed index 5
func TestMajorityConfigWeightedCommittedIndexDefaultWeight(t *testing.T) {
	mc := MajorityConfig{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

	l := mapAckIndexer{
		1: 10,
		2: 10,
		3: 8,
		4: 5,
		5: 5,
	}
	// Nodes 4 and 5 intentionally absent — they should default to weight 1.0.
	w := mapWeightConfig{
		1: 0.3,
		2: 0.3,
		3: 0.2,
	}

	got := mc.WeightedCommittedIndex(l, w)
	require.Equal(t, Index(5), got,
		"default weight 1.0 for missing voters: expected committed index 5")
}

// TestMajorityConfigWeightedCommittedIndexPartialAck verifies the corrected
// denominator: totalWeight must include ALL configured voters, not just those
// that have acked. This prevents a single high-weight node from self-satisfying
// quorum when the other four peers have not reported in yet.
//
//	Node 1: weight 0.9, acked index 100   ← only reporter
//	Nodes 2-5: weight 0.1 each, NO acked index
//
// totalWeight = 0.9 + 4*0.1 = 1.3. threshold = 0.65.
// Node 1 alone contributes 0.9, but 0.9 / 1.3 ≈ 0.69 which IS >= 0.5…
//
// Wait — let's pick a weight where node 1 cannot satisfy quorum alone:
//	Node 1: weight 0.4, acked index 100
//	Nodes 2-5: weight 0.15 each, NO acked index
//
// totalWeight = 0.4 + 4*0.15 = 1.0. threshold = 0.5.
// Node 1 alone contributes 0.4 < 0.5 → Index(0).
//
// This test uses equal default weights (1.0 each for nodes 2-5) to make
// the arithmetic crystal-clear:
//
//	Node 1: weight 0.3, acked index 100
//	Nodes 2-5: weight 1.0 each (default), NO acked index
//
// totalWeight = 0.3 + 4*1.0 = 4.3. threshold = 2.15.
// Node 1 contributes only 0.3 << 2.15 → Index(0).
func TestMajorityConfigWeightedCommittedIndexPartialAck(t *testing.T) {
	mc := MajorityConfig{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

	// Only node 1 has reported an acked index.
	l := mapAckIndexer{
		1: 100,
	}
	// Nodes 2-5 absent from weight map → they default to 1.0 each.
	// totalWeight = 0.3 + 4*1.0 = 4.3; threshold = 2.15.
	// Node 1 alone contributes 0.3 — far below threshold.
	w := mapWeightConfig{
		1: 0.3,
	}

	got := mc.WeightedCommittedIndex(l, w)
	require.Equal(t, Index(0), got,
		"single high-index voter cannot self-satisfy quorum when 4 peers have not acked: expected Index(0)")
}

