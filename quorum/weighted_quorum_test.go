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
