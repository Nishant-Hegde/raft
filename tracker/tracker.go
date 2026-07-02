// Copyright 2019 The etcd Authors
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
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strings"

	"go.etcd.io/raft/v3/quorum"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Config reflects the configuration tracked in a ProgressTracker.
type Config struct {
	Voters quorum.JointConfig
	// AutoLeave is true if the configuration is joint and a transition to the
	// incoming configuration should be carried out automatically by Raft when
	// this is possible. If false, the configuration will be joint until the
	// application initiates the transition manually.
	AutoLeave bool
	// Learners is a set of IDs corresponding to the learners active in the
	// current configuration.
	//
	// Invariant: Learners and Voters does not intersect, i.e. if a peer is in
	// either half of the joint config, it can't be a learner; if it is a
	// learner it can't be in either half of the joint config. This invariant
	// simplifies the implementation since it allows peers to have clarity about
	// its current role without taking into account joint consensus.
	Learners map[uint64]struct{}
	// When we turn a voter into a learner during a joint consensus transition,
	// we cannot add the learner directly when entering the joint state. This is
	// because this would violate the invariant that the intersection of
	// voters and learners is empty. For example, assume a Voter is removed and
	// immediately re-added as a learner (or in other words, it is demoted):
	//
	// Initially, the configuration will be
	//
	//   voters:   {1 2 3}
	//   learners: {}
	//
	// and we want to demote 3. Entering the joint configuration, we naively get
	//
	//   voters:   {1 2} & {1 2 3}
	//   learners: {3}
	//
	// but this violates the invariant (3 is both voter and learner). Instead,
	// we get
	//
	//   voters:   {1 2} & {1 2 3}
	//   learners: {}
	//   next_learners: {3}
	//
	// Where 3 is now still purely a voter, but we are remembering the intention
	// to make it a learner upon transitioning into the final configuration:
	//
	//   voters:   {1 2}
	//   learners: {3}
	//   next_learners: {}
	//
	// Note that next_learners is not used while adding a learner that is not
	// also a voter in the joint config. In this case, the learner is added
	// right away when entering the joint configuration, so that it is caught up
	// as soon as possible.
	LearnersNext map[uint64]struct{}
}

func (c Config) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "voters=%s", c.Voters)
	if c.Learners != nil {
		fmt.Fprintf(&buf, " learners=%s", quorum.MajorityConfig(c.Learners).String())
	}
	if c.LearnersNext != nil {
		fmt.Fprintf(&buf, " learners_next=%s", quorum.MajorityConfig(c.LearnersNext).String())
	}
	if c.AutoLeave {
		fmt.Fprint(&buf, " autoleave")
	}
	return buf.String()
}

// Clone returns a copy of the Config that shares no memory with the original.
func (c *Config) Clone() Config {
	clone := func(m map[uint64]struct{}) map[uint64]struct{} {
		if m == nil {
			return nil
		}
		mm := make(map[uint64]struct{}, len(m))
		for k := range m {
			mm[k] = struct{}{}
		}
		return mm
	}
	return Config{
		Voters:       quorum.JointConfig{clone(c.Voters[0]), clone(c.Voters[1])},
		Learners:     clone(c.Learners),
		LearnersNext: clone(c.LearnersNext),
	}
}

// ProgressTracker tracks the currently active configuration and the information
// known about the nodes and learners in it. In particular, it tracks the match
// index for each peer which in turn allows reasoning about the committed index.
type ProgressTracker struct {
	Config

	Progress ProgressMap

	Votes map[uint64]bool

	// Weight assigns a static weight to each voter, keyed by voter ID. If nil
	// or a voter is absent, that voter defaults to weight 1.0 (equivalent to
	// unweighted majority behavior).
	Weight map[uint64]float64

	CurrentEpoch uint64
	EpochStates  map[uint64]*EpochState

	MaxInflight      int
	MaxInflightBytes uint64
}

type EpochState struct {
	Weights     map[uint64]float64
	MaxAppended uint64
	Acks        map[uint64]bool
}

// MakeProgressTracker initializes a ProgressTracker.
func MakeProgressTracker(maxInflight int, maxBytes uint64) ProgressTracker {
	p := ProgressTracker{
		MaxInflight:      maxInflight,
		MaxInflightBytes: maxBytes,
		EpochStates: map[uint64]*EpochState{
			0: {Weights: make(map[uint64]float64)},
		},
		Config: Config{
			Voters: quorum.JointConfig{
				quorum.MajorityConfig{},
				nil, // only populated when used
			},
			Learners:     nil, // only populated when used
			LearnersNext: nil, // only populated when used
		},
		Votes:    map[uint64]bool{},
		Progress: map[uint64]*Progress{},
	}
	return p
}

// ConfState returns a ConfState representing the active configuration.
func (p *ProgressTracker) ConfState() *pb.ConfState {
	return &pb.ConfState{
		Voters:         p.Voters[0].Slice(),
		VotersOutgoing: p.Voters[1].Slice(),
		Learners:       quorum.MajorityConfig(p.Learners).Slice(),
		LearnersNext:   quorum.MajorityConfig(p.LearnersNext).Slice(),
		AutoLeave:      new(p.AutoLeave),
	}
}

// IsSingleton returns true if (and only if) there is only one voting member
// (i.e. the leader) in the current configuration.
func (p *ProgressTracker) IsSingleton() bool {
	return len(p.Voters[0]) == 1 && len(p.Voters[1]) == 0
}

type matchAckIndexer map[uint64]*Progress

var _ quorum.AckedIndexer = matchAckIndexer(nil)

// AckedIndex implements IndexLookuper.
func (l matchAckIndexer) AckedIndex(id uint64) (quorum.Index, bool) {
	pr, ok := l[id]
	if !ok {
		return 0, false
	}
	return quorum.Index(pr.Match), true
}

// trackerWeightConfig adapts a map[uint64]float64 to the quorum.WeightedConfig
// interface. Voters absent from the map return (0, false), causing
// MajorityConfig.WeightedCommittedIndex to default them to weight 1.0.
type trackerWeightConfig map[uint64]float64

var _ quorum.WeightedConfig = trackerWeightConfig(nil)

func (m trackerWeightConfig) Weight(id uint64) (float64, bool) {
	w, ok := m[id]
	return w, ok
}

// Committed returns the largest log index known to be committed based on what
// the voting members of the group have acknowledged. When ProgressTracker.Weight
// is set, per-voter weights are used; absent or nil Weight maps default every
// voter to 1.0, producing results identical to the old unweighted path.
func (p *ProgressTracker) Committed() uint64 {
	return uint64(p.Voters.WeightedCommittedIndex(
		matchAckIndexer(p.Progress), trackerWeightConfig(p.Weight)))
}

// Visit invokes the supplied closure for all tracked progresses in stable order.
func (p *ProgressTracker) Visit(f func(id uint64, pr *Progress)) {
	n := len(p.Progress)
	// We need to sort the IDs and don't want to allocate since this is hot code.
	// The optimization here mirrors that in `(MajorityConfig).CommittedIndex`,
	// see there for details.
	var sl [7]uint64
	var ids []uint64
	if len(sl) >= n {
		ids = sl[:n]
	} else {
		ids = make([]uint64, n)
	}
	for id := range p.Progress {
		n--
		ids[n] = id
	}
	slices.Sort(ids)
	for _, id := range ids {
		f(id, p.Progress[id])
	}
}

// QuorumActive returns true if the quorum is active from the view of the local
// raft state machine. Otherwise, it returns false.
func (p *ProgressTracker) QuorumActive() bool {
	votes := map[uint64]bool{}
	p.Visit(func(id uint64, pr *Progress) {
		if pr.IsLearner {
			return
		}
		votes[id] = pr.RecentActive
	})

	return p.Voters.WeightedVoteResult(votes, trackerWeightConfig(p.Weight)) == quorum.VoteWon
}

// VoterNodes returns a sorted slice of voters.
func (p *ProgressTracker) VoterNodes() []uint64 {
	m := p.Voters.IDs()
	nodes := make([]uint64, 0, len(m))
	for id := range m {
		nodes = append(nodes, id)
	}
	slices.Sort(nodes)
	return nodes
}

// LearnerNodes returns a sorted slice of learners.
func (p *ProgressTracker) LearnerNodes() []uint64 {
	if len(p.Learners) == 0 {
		return nil
	}
	nodes := make([]uint64, 0, len(p.Learners))
	for id := range p.Learners {
		nodes = append(nodes, id)
	}
	slices.Sort(nodes)
	return nodes
}

// ResetVotes prepares for a new round of vote counting via recordVote.
func (p *ProgressTracker) ResetVotes() {
	p.Votes = map[uint64]bool{}
}

// RecordVote records that the node with the given id voted for this Raft
// instance if v == true (and declined it otherwise).
func (p *ProgressTracker) RecordVote(id uint64, v bool) {
	_, ok := p.Votes[id]
	if !ok {
		p.Votes[id] = v
	}
}

// TallyVotes returns the number of granted and rejected Votes, and whether the
// election outcome is known.
func (p *ProgressTracker) TallyVotes() (granted int, rejected int, _ quorum.VoteResult) {
	// Make sure to populate granted/rejected correctly even if the Votes slice
	// contains members no longer part of the configuration. This doesn't really
	// matter in the way the numbers are used (they're informational), but might
	// as well get it right.
	for id, pr := range p.Progress {
		if pr.IsLearner {
			continue
		}
		v, voted := p.Votes[id]
		if !voted {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	result := p.Voters.WeightedVoteResult(p.Votes, trackerWeightConfig(p.Weight))
	return granted, rejected, result
}

const EWAlpha = 0.2

// Epsilon is the weight floor guard.
// The formula is: 0.05 * (total_weight / n) = 0.05, given the sum==n normalization.
// The default is 0.05 (capped below 1.0 defensively).
// Why the floor exists: a node whose weight collapses to ~0 becomes invisible to quorums
// and unable to recover, since low weight -> negligible contribution even after latency improves.
// How the clamp/renormalize interaction is resolved: remainder renormalization to a fixed point.
// Floored nodes are clamped exactly to Epsilon, and only the unfloored remainder is renormalized
// to (n - sum(floored)). This repeats until no new nodes fall below the floor.
const Epsilon = 0.05

// UpdateEWAWeight updates the EWA weight for the voter identified by id using
// the supplied storage-write latency sample (in nanoseconds). The formula is:
//
//	wi <- EWAlpha*(1/latencyMs) + (1-EWAlpha)*wi
//
// If latencyNs <= 0 (no real sample yet), the update acts as a mathematical
// no-op (wRaw = wPrev) rather than skipping the function or clamping to a
// tiny floor, ensuring a single code path. Voters absent from the Weight map
// start with wi=1 (uniform initial weight).
//
// After updating the individual weight, all voter weights are re-normalized so
// that their sum equals n (the number of voters in the current joint config).
// This keeps the total weight equal to n regardless of the mix of latencies,
// which in turn preserves the "more than half the weight" quorum threshold
// semantics implemented by WeightedCommittedIndex and WeightedVoteResult.
func (p *ProgressTracker) UpdateEWAWeight(id uint64, latencyNs int64) {
	// Lazily allocate the Weight map on first use.
	if p.Weight == nil {
		p.Weight = make(map[uint64]float64)
	}

	// Prior weight defaults to 1.0 (uniform) if this voter has never been seen.
	wPrev, ok := p.Weight[id]
	if !ok {
		wPrev = 1.0
	}

	// EWA update:
	// If there's no real sample (<= 0), the natural formula output is to preserve wPrev.
	// NOTE: this means any caller that never populates StorageWriteLatencyNs (e.g. tests
	// using bare MemoryStorage with SimulatedFsyncLatency left at 0, or manually-set
	// static weights via direct trk.Weight assignment) will see EWA act as a complete
	// no-op on every update — not because static weights are special-cased, but because
	// EWA never receives a nonzero latency signal to act on. Static weights only persist
	// in such tests as a side effect of this; if real latency data starts flowing for
	// a given voter, EWA will begin overwriting its weight on the next update.
	// Otherwise, convert ns to ms so 1/latency produces a meaningful magnitude.
	wRaw := wPrev
	if latencyNs > 0 {
		latencyMs := float64(latencyNs) / 1_000_000.0
		wRaw = EWAlpha*(1.0/latencyMs) + (1-EWAlpha)*wPrev
	}
	p.Weight[id] = wRaw

	// Normalize: collect all voter IDs (union of both halves of joint config),
	// fill in weight 1.0 for voters not yet in the map, then scale so sum == n.
	voterIDs := p.Voters.IDs()
	n := float64(len(voterIDs))
	if n == 0 {
		return
	}

	// Compute raw sum, defaulting absent voters to 1.0.
	var total float64
	for vid := range voterIDs {
		w, exists := p.Weight[vid]
		if !exists {
			w = 1.0
		}
		total += w
	}
	if total == 0 {
		return
	}

	// Scale factor so that sum of all voter weights == n.
	scale := n / total
	for vid := range voterIDs {
		w, exists := p.Weight[vid]
		if !exists {
			w = 1.0
		}
		p.Weight[vid] = w * scale
	}

	// Apply weight floor ε-guard iteratively.
	if latencyNs <= 0 {
		return
	}

	eps := Epsilon
	if eps >= 1.0 {
		eps = 0.99
	}

	floored := make(map[uint64]bool)
	for {
		newlyFloored := false
		var unflooredRawSum float64

		for vid := range voterIDs {
			if !floored[vid] && p.Weight[vid] < eps {
				floored[vid] = true
				p.Weight[vid] = eps
				newlyFloored = true
			}
			if !floored[vid] {
				unflooredRawSum += p.Weight[vid]
			}
		}

		if !newlyFloored {
			break
		}

		if len(floored) == int(n) {
			// All-floored fallback: uniform 1.0
			for vid := range voterIDs {
				p.Weight[vid] = 1.0
			}
			break
		}

		flooredTotal := float64(len(floored)) * eps
		targetUnflooredSum := n - flooredTotal

		if unflooredRawSum > 0 {
			remainderScale := targetUnflooredSum / unflooredRawSum
			for vid := range voterIDs {
				if !floored[vid] {
					p.Weight[vid] *= remainderScale
				}
			}
		}
	}
}

// SnapshotEpoch increments CurrentEpoch if weights differ from the last broadcast,
// and records the new weights under CurrentEpoch with maxAppended.
// It is called once per broadcast cycle (e.g., bcastAppend) to avoid churn.
func (p *ProgressTracker) SnapshotEpoch(maxAppended uint64, leaderID uint64) {
	if p.EpochStates == nil {
		p.EpochStates = make(map[uint64]*EpochState)
	}

	currState, exists := p.EpochStates[p.CurrentEpoch]
	changed := !exists
	if exists {
		if len(p.Weight) != len(currState.Weights) {
			changed = true
		} else {
			for id, w := range p.Weight {
				// Exact comparison: any weight drift creates a new epoch snapshot. This
				// is intentional — epochs are per-broadcast-round snapshots, bounded by
				// CleanupEpochs; we do not coalesce near-identical weight vectors.
				if currState.Weights[id] != w {
					changed = true
					break
				}
			}
		}
	}

	if changed && p.CurrentEpoch == 0 {
		// Optimization/Test-compat: If we are in Epoch 0 and the new weights are purely uniform (1.0),
		// we don't need to increment the epoch because it behaves exactly like the default unweighted state.
		allUniform := true
		for _, w := range p.Weight {
			if w != 1.0 {
				allUniform = false
				break
			}
		}
		if allUniform {
			changed = false
		}
	}

	if changed {
		p.CurrentEpoch++
		newWeights := make(map[uint64]float64, len(p.Weight))
		for k, v := range p.Weight {
			newWeights[k] = v
		}
		p.EpochStates[p.CurrentEpoch] = &EpochState{
			Weights:     newWeights,
			MaxAppended: maxAppended,
			Acks:        map[uint64]bool{leaderID: true}, // leader implicitly acks
		}
	} else if exists {
		currState.MaxAppended = maxAppended
	}
}

// HasQuorum returns true if the gathered acks form a quorum under this epoch's weights.
func (s *EpochState) HasQuorum(voters quorum.JointConfig) bool {
	return voters.WeightedVoteResult(s.Acks, trackerWeightConfig(s.Weights)) == quorum.VoteWon
}

func (p *ProgressTracker) CleanupEpochs(committed uint64) {
	for ep, state := range p.EpochStates {
		if ep == 0 {
			continue // Never delete the default unweighted Epoch 0
		}
		if state.MaxAppended <= committed {
			delete(p.EpochStates, ep)
		}
	}
}

// EncodeEpochContext packs the epoch and weights into a byte slice.
func EncodeEpochContext(epoch uint64, weights map[uint64]float64) []byte {
	if epoch == 0 {
		return nil
	}
	n := len(weights)
	ctx := make([]byte, 10+n*16)
	binary.LittleEndian.PutUint64(ctx[0:8], epoch)
	binary.LittleEndian.PutUint16(ctx[8:10], uint16(n))
	offset := 10
	for id, w := range weights {
		binary.LittleEndian.PutUint64(ctx[offset:offset+8], id)
		binary.LittleEndian.PutUint64(ctx[offset+8:offset+16], math.Float64bits(w))
		offset += 16
	}
	return ctx
}

// DecodeEpochContext extracts the epoch from a payload encoded by EncodeEpochContext.
func DecodeEpochContext(ctx []byte) (epoch uint64, ok bool) {
	if len(ctx) < 10 {
		return 0, false
	}
	return binary.LittleEndian.Uint64(ctx[:8]), true
}
