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

// Package quorum exhaustive weighted-threshold tests (configurations 7 & 8).
//
// The six configurations already covered by existing test files are:
//   1. All equal weights (0.2 each)              — TestWeightedCommittedIndex/equal_weights_reduce_to_majority
//   2. Non-uniform (0.3/0.3/0.2/0.1/0.1)        — TestWeightedCommittedIndex/non-uniform_weights
//   3. Default weights (absent → 1.0)            — TestMajorityConfigWeightedCommittedIndexDefaultWeight
//   4. Partial ack (single voter reported)       — TestMajorityConfigWeightedCommittedIndexPartialAck
//   5. High-weight minority active               — TestQuorumActiveHighWeightMinorityActive (tracker pkg)
//   6. Low-weight majority active                — TestQuorumActiveLowWeightMajorityActive (tracker pkg)
//
// This file adds the two remaining edge-case configurations to reach 8 total:
//   7. One dominant node   (weight 0.6 / 0.1 / 0.1 / 0.1 / 0.1)
//   8. Floor weight node   (weight 0.2475 / 0.2475 / 0.2475 / 0.2475 / 0.01)

package quorum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWeightedCommittedIndexDominantNode covers Configuration 7:
// one node holds 0.6 of total weight; the other four share the remaining 0.4.
//
// Weights: node1=0.6, nodes2-5=0.1 each.
// totalWeight = 0.6 + 4×0.1 = 1.0.
// threshold   = 0.5 × 1.0   = 0.5.
func TestWeightedCommittedIndexDominantNode(t *testing.T) {
	mc := MajorityConfig{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}
	w := mapWeightConfig{
		1: 0.6,
		2: 0.1,
		3: 0.1,
		4: 0.1,
		5: 0.1,
	}

	t.Run("dominant node present — alone satisfies quorum", func(t *testing.T) {
		// All five nodes have acked.
		//
		// Acked indices: 1→10, 2→10, 3→8, 4→5, 5→5.
		//
		// Walk descending by acked index:
		//   node1 (w=0.6, idx=10): cumWeight = 0.6  →  0.6 > 0.5  ✓  → committed index = 10
		//
		// Node 1's weight alone exceeds the threshold at the highest index,
		// so the result must be Index(10).
		l := mapAckIndexer{
			1: 10,
			2: 10,
			3: 8,
			4: 5,
			5: 5,
		}
		got := mc.WeightedCommittedIndex(l, w)
		require.Equal(t, Index(10), got,
			"dominant node (w=0.6) acked at idx=10: should satisfy quorum alone → Index(10)")
	})

	t.Run("dominant node absent — remaining nodes cannot satisfy quorum", func(t *testing.T) {
		// Node 1 has NOT acked (absent from the acked map). All four low-weight
		// nodes (2-5) have acked at index 5.
		//
		// totalWeight still counts all 5 configured voters = 1.0 (node 1
		// is in MajorityConfig even if it hasn't acked).
		// threshold = 0.5.
		//
		// Acked entries contributed to the numerator: nodes 2-5 only.
		//   node2 (w=0.1, idx=5)
		//   node3 (w=0.1, idx=5)
		//   node4 (w=0.1, idx=5)
		//   node5 (w=0.1, idx=5)
		//
		// Walk descending by acked index (all at idx=5):
		//   cumWeight = 0.1+0.1+0.1+0.1 = 0.4  →  0.4 > 0.5?  No.
		//
		// Cumulative weight never exceeds threshold → Index(0).
		// This proves: the dominant node's absence blocks quorum even when
		// every other voter has acked.
		l := mapAckIndexer{
			// node 1 deliberately omitted — has not reported an acked index
			2: 5,
			3: 5,
			4: 5,
			5: 5,
		}
		got := mc.WeightedCommittedIndex(l, w)
		require.Equal(t, Index(0), got,
			"dominant node (w=0.6) absent: combined weight of nodes 2-5 (0.4) < threshold (0.5) → Index(0)")
	})
}

// TestWeightedCommittedIndexFloorWeight covers Configuration 8:
// one node carries a near-zero weight (0.01); the other four share the rest equally.
//
// Weights: nodes1-4=0.2475 each, node5=0.01.
// totalWeight = 4×0.2475 + 0.01 = 0.99 + 0.01 = 1.0.
// threshold   = 0.5 × 1.0          = 0.5.
func TestWeightedCommittedIndexFloorWeight(t *testing.T) {
	mc := MajorityConfig{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}
	w := mapWeightConfig{
		1: 0.2475,
		2: 0.2475,
		3: 0.2475,
		4: 0.2475,
		5: 0.01,
	}

	t.Run("committed index with floor-weight node present", func(t *testing.T) {
		// All five nodes have acked.
		//
		// Acked indices: 1→10, 2→10, 3→8, 4→5, 5→5.
		//
		// Walk descending by acked index:
		//   node1 (w=0.2475, idx=10): cumWeight = 0.2475    →  0.2475 > 0.5?  No
		//   node2 (w=0.2475, idx=10): cumWeight = 0.4950    →  0.4950 > 0.5?  No
		//   node3 (w=0.2475, idx=8):  cumWeight = 0.7425    →  0.7425 > 0.5?  Yes → committed index = 8
		//
		// Node 5's floor weight (0.01) does not affect the outcome here
		// because quorum is reached at idx=8 before reaching idx=5.
		l := mapAckIndexer{
			1: 10,
			2: 10,
			3: 8,
			4: 5,
			5: 5,
		}
		got := mc.WeightedCommittedIndex(l, w)
		require.Equal(t, Index(8), got,
			"floor-weight config: three equal-weight nodes (cum=0.7425) exceed threshold at idx=8 → Index(8)")
	})

	t.Run("floor-weight node counted in denominator but cannot satisfy quorum alone", func(t *testing.T) {
		// Only node 5 (weight=0.01) has acked, at the highest possible index (100).
		// Nodes 1-4 have NOT acked (absent from the acked map).
		//
		// totalWeight = 1.0 (all five configured voters counted in denominator).
		// threshold   = 0.5.
		//
		// Acked entries contributed to numerator: node5 only.
		//   node5 (w=0.01, idx=100): cumWeight = 0.01  →  0.01 > 0.5?  No.
		//
		// Cumulative weight never exceeds threshold → Index(0).
		//
		// This confirms: node5 IS included in totalWeight (denominator), but its
		// near-zero weight (0.01) cannot satisfy quorum even at the highest index.
		// The high acked index of the floor node is irrelevant if its weight is
		// too small to tip the scales.
		l := mapAckIndexer{
			// nodes 1-4 deliberately omitted — have not reported acked indices
			5: 100,
		}
		got := mc.WeightedCommittedIndex(l, w)
		require.Equal(t, Index(0), got,
			"floor-weight node (w=0.01) acked at idx=100 but weight < threshold (0.5): cannot satisfy quorum → Index(0)")
	})
}
