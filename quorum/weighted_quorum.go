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

import "slices"

// WeightedCommittedIndex computes the highest log index X such that the sum of
// weights of all voters whose acked index >= X is >= 0.5 * (sum of all weights).
//
// weights maps voter ID to its static weight. acked maps voter ID to the
// highest log index that voter has acknowledged. Only voters present in both
// maps are considered. If totalWeight is 0 (or no voters are present in both
// maps), Index(0) is returned.
//
// This is a standalone skeleton function and is not yet wired into
// MajorityConfig or CommittedIndex.
func WeightedCommittedIndex(weights map[uint64]float64, acked map[uint64]Index) Index {
	type entry struct {
		idx    Index
		weight float64
	}

	// Collect (index, weight) pairs for voters present in both maps.
	entries := make([]entry, 0, len(weights))
	var totalWeight float64
	for id, w := range weights {
		if idx, ok := acked[id]; ok {
			entries = append(entries, entry{idx: idx, weight: w})
			totalWeight += w
		}
	}

	if totalWeight == 0 {
		return Index(0)
	}

	// Sort descending by index so we walk from the highest acknowledged index
	// down to the lowest.
	slices.SortFunc(entries, func(a, b entry) int {
		// Descending: b before a.
		if b.idx > a.idx {
			return 1
		}
		if b.idx < a.idx {
			return -1
		}
		return 0
	})

	// Walk down accumulating weight. The first index at which cumulative weight
	// reaches >= 0.5 * totalWeight is the weighted committed index.
	threshold := 0.5 * totalWeight
	var cumWeight float64
	for _, e := range entries {
		cumWeight += e.weight
		if cumWeight >= threshold {
			return e.idx
		}
	}

	// Should not be reached if totalWeight > 0 and entries is non-empty.
	return Index(0)
}
