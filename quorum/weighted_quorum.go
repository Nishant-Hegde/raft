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
	"math"
	"slices"
)

// WeightedCommittedIndex computes the highest log index X such that the sum of
// weights of all voters whose acked index >= X is > 0.5 * (sum of all weights),
// i.e. a strict majority of total weight. Using strictly-greater-than matches
// the n/2+1 unweighted majority threshold for all cluster sizes n.
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

	// Walk down accumulating weight. The first index at which cumulative
	// weight exceeds 0.5 * totalWeight is the weighted committed index.
	// Using strictly-greater-than (not >=) ensures weighted quorum with equal
	// weights matches the n/2+1 unweighted majority for all cluster sizes.
	threshold := 0.5 * totalWeight
	var cumWeight float64
	for _, e := range entries {
		cumWeight += e.weight
		if cumWeight > threshold {
			return e.idx
		}
	}

	// Should not be reached if totalWeight > 0 and entries is non-empty.
	return Index(0)
}

// WeightedCommittedIndex computes the committed index using per-voter weights
// instead of a simple majority count. It returns the highest index X such that
// the sum of weights of voters whose acked index >= X is > 0.5 * totalWeight
// (strictly greater than half), where totalWeight is the sum of weights of ALL
// voters in c (not only those that have acked something).
//
// Using strictly-greater-than (not >=) ensures that weighted quorum with equal
// unit weights produces results identical to the n/2+1 unweighted majority
// for all cluster sizes, including n=2 where both nodes must agree.
//
// Voters in c with no weight entry in w are treated as having weight 1.0
// (graceful default, matching unweighted majority behavior). Only voters that
// have reported an acked index via l contribute to the numerator; voters with
// no acked index are counted in totalWeight but skipped in the numerator,
// making it harder (not easier) to reach quorum when not all peers have
// reported in. If no voter has acked anything, or totalWeight is 0, Index(0)
// is returned.
func (c MajorityConfig) WeightedCommittedIndex(l AckedIndexer, w WeightedConfig) Index {
	// An empty config imposes no restriction on the commit index — return
	// MaxUint64 so that JointConfig.WeightedCommittedIndex takes min(idx0,
	// idx1) correctly when one half is nil/empty. This mirrors the identical
	// fast-path in MajorityConfig.CommittedIndex.
	if len(c) == 0 {
		return math.MaxUint64
	}

	type entry struct {
		idx    Index
		weight float64
	}

	// totalWeight covers every voter in c, regardless of whether they have
	// acked. This is the correct denominator for the quorum fraction.
	var totalWeight float64
	entries := make([]entry, 0, len(c))

	for id := range c {
		// Determine this voter's weight; default to 1.0 if not in w.
		weight := 1.0
		if wv, ok := w.Weight(id); ok {
			weight = wv
		}
		totalWeight += weight // always counted

		// Only voters that have acked contribute to the numerator.
		if idx, ok := l.AckedIndex(id); ok {
			entries = append(entries, entry{idx: idx, weight: weight})
		}
	}

	if totalWeight == 0 || len(entries) == 0 {
		return Index(0)
	}

	// Sort descending by index so we walk from the highest acked index down.
	slices.SortFunc(entries, func(a, b entry) int {
		if b.idx > a.idx {
			return 1
		}
		if b.idx < a.idx {
			return -1
		}
		return 0
	})

	// Walk down accumulating weight. Return the first index at which cumulative
	// acknowledging weight strictly exceeds 0.5 * totalWeight. Using > (not >=)
	// matches n/2+1 unweighted majority for all cluster sizes.
	threshold := 0.5 * totalWeight
	var cumWeight float64
	for _, e := range entries {
		cumWeight += e.weight
		if cumWeight > threshold {
			return e.idx
		}
	}

	// Cumulative weight never reached threshold (e.g. acking voters together
	// hold less than half of all configured weight).
	return Index(0)
}

// WeightedVoteResult takes a mapping of voters to yes/no votes and a
// WeightedConfig, and returns a VoteResult using weight sums instead of
// node counts. The rules are:
//   - VoteWon:     Σ(weight of yes-voters) > 0.5 * Σ(weight of all voters in c)
//                  (strictly greater than, matching n/2+1 majority for equal weights)
//   - VoteLost:    Σ(weight of yes-voters) + Σ(weight of not-yet-voted voters)
//                  <= 0.5 * Σ(weight of all voters in c)  [cannot reach threshold]
//   - VotePending: otherwise (threshold not yet reached but still reachable)
//
// Voters in c absent from w default to weight 1.0.
// Empty config (len(c)==0) returns VoteWon by convention, matching VoteResult.
func (c MajorityConfig) WeightedVoteResult(votes map[uint64]bool, w WeightedConfig) VoteResult {
	if len(c) == 0 {
		// By convention, the election on an empty config wins. This plays well
		// with joint quorums where one half is empty.
		return VoteWon
	}

	// Sum totalWeight over all voters in c, defaulting to 1.0 if absent from w.
	var totalWeight float64
	for id := range c {
		weight := 1.0
		if wv, ok := w.Weight(id); ok {
			weight = wv
		}
		totalWeight += weight
	}

	threshold := 0.5 * totalWeight

	// Walk voters: accumulate yesWeight for yes-votes, missingWeight for
	// voters not yet present in the votes map (not voted yet).
	var yesWeight float64
	var missingWeight float64
	for id := range c {
		weight := 1.0
		if wv, ok := w.Weight(id); ok {
			weight = wv
		}
		v, ok := votes[id]
		if !ok {
			missingWeight += weight
			continue
		}
		if v {
			yesWeight += weight
		}
	}

	if yesWeight > threshold {
		return VoteWon
	}
	if yesWeight+missingWeight >= threshold {
		return VotePending
	}
	return VoteLost
}

