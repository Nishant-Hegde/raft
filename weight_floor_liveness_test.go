package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
)

// TestWeightFloorLiveness verifies that when a node's weight is driven to the 
// epsilon floor (0.05), the remaining healthy nodes hold enough weight (4.95)
// to satisfy the >2.5 quorum threshold, meaning the cluster can still commit
// entries even if the floored node is completely disconnected (simulating a 
// severe sustained latency spike / partition).
//
// We use manual weights here to isolate the Raft liveness check from the 
// wall-clock sleeps required by SimulatedFsyncLatency and to avoid EWA 
// ordering-drift flakiness on strict margins.
func TestWeightFloorLiveness(t *testing.T) {
	// 1. Build a 5-node cluster
	nt := newNetwork(nil, nil, nil, nil, nil) // nodes 1..5

	// 2. Elect node 2 as leader (so node 1 is a follower that we can isolate)
	nt.send(&pb.Message{
		From: new(uint64(2)),
		To:   new(uint64(2)),
		Type: pb.MsgHup.Enum(),
	})
	leader := nt.peers[2].(*raft)
	require.Equal(t, StateLeader, leader.state)

	// 3. Manually set the floored weights on the leader.
	// This simulates the steady-state after a sustained 20x+ spike on Node 1.
	leader.trk.Weight = map[uint64]float64{
		1: 0.05,   // Floored
		2: 1.2375, // Remaining 4.95 shared equally
		3: 1.2375,
		4: 1.2375,
		5: 1.2375,
	}

	initialCommitted := leader.raftLog.committed

	// 4. Completely isolate Node 1 from the network.
	// If it was just slow (80ms), it would eventually reply. Cutting it proves
	// we do not need its vote at all to commit.
	nt.isolate(1)

	// 5. Propose a new entry
	nt.send(&pb.Message{
		From:    new(uint64(2)),
		To:      new(uint64(2)),
		Type:    pb.MsgProp.Enum(),
		Entries: []*pb.Entry{{Data: []byte("test-liveness")}},
	})

	// 6. Assert that the entry was committed despite Node 1's absence.
	// Nodes 2,3,4,5 hold 4 * 1.2375 = 4.95 > 2.5 weight.
	assert.Greater(t, leader.raftLog.committed, initialCommitted, 
		"Cluster failed to commit entry without the floored node")
	
	// Ensure all healthy followers also committed
	for i := uint64(2); i <= 5; i++ {
		r := nt.peers[i].(*raft)
		assert.Equal(t, leader.raftLog.committed, r.raftLog.committed,
			"Healthy follower %d did not commit", i)
	}
}
