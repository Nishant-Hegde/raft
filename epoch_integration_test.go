package raft

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

func TestEpochIntegrationStragglerAck(t *testing.T) {
	// Setup a leader node with 3 peers (1, 2, 3). Node 1 is the leader.
	storage := NewMemoryStorage()
	storage.snapshot.Metadata.ConfState.Voters = []uint64{1, 2, 3}
	c := &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
	}
	r := newRaft(c)
	r.becomeFollower(1, None)
	r.becomeCandidate()
	r.becomeLeader()

	// Initial configuration
	// Set non-uniform weights to trigger epoch increment beyond 0
	r.trk.Weight = map[uint64]float64{1: 1.5, 2: 1.0, 3: 1.0}

	// 1. First broadcast cycle (Epoch 1)
	// We append two entries so that a straggler ack for the first entry
	// does not fully commit the entire epoch, preventing CleanupEpochs from deleting it.
	r.appendEntry(&pb.Entry{Data: []byte("foo1")}, &pb.Entry{Data: []byte("foo2")})
	r.bcastAppend()

	epoch1 := r.trk.CurrentEpoch
	t.Logf("Broadcast 1 -> Epoch: %d", epoch1)
	if epoch1 != 1 {
		t.Fatalf("expected epoch 1, got %d", epoch1)
	}

	// 2. Change weights (simulate EWA update)
	r.trk.Weight[2] = 2.0
	r.trk.Weight[3] = 0.5

	// 3. Second broadcast cycle (Epoch 2)
	r.appendEntry(&pb.Entry{Data: []byte("bar")})
	r.bcastAppend()

	epoch2 := r.trk.CurrentEpoch
	t.Logf("Broadcast 2 (weights changed) -> Epoch: %d", epoch2)
	if epoch2 != 2 {
		t.Fatalf("expected epoch 2, got %d", epoch2)
	}

	// 4. Simulate a straggler ack from Node 2 for Epoch 1
	// Encode context for Epoch 1 manually to mimic the old message Node 2 is replying to
	oldCtx := tracker.EncodeEpochContext(epoch1, r.trk.EpochStates[epoch1].Weights)

	// We manually target the first appended entry (index 2).
	// Because MaxAppended for Epoch 1 is 3 (due to foo2), acking index 2 will
	// not cause CleanupEpochs to delete Epoch 1, allowing us to inspect its Acks.
	from2 := uint64(2)
	to1 := uint64(1)
	idxToAck := uint64(2)

	ackMsg := &pb.Message{
		From:    &from2,
		To:      &to1,
		Type:    pb.MsgAppResp.Enum(),
		Index:   &idxToAck,
		Context: oldCtx,
	}

	// Process the straggler ack
	err := stepLeader(r, ackMsg)
	if err != nil {
		t.Fatalf("stepLeader failed: %v", err)
	}

	// 5. Verify the bookkeeping
	state1 := r.trk.EpochStates[epoch1]
	if !state1.Acks[2] {
		t.Fatalf("expected Node 2 to be recorded in Epoch 1 Acks")
	}
	t.Logf("Epoch 1 Acks: %v", state1.Acks)

	// Node 1 (leader) implicitly acks its own epoch, so Acks should contain 1 and 2
	hasQuorumEpoch1 := state1.HasQuorum(r.trk.Voters)
	t.Logf("Epoch 1 HasQuorum: %v (Leader + Node 2 = 1.0 + 1.0 = 2.0/3.0)", hasQuorumEpoch1)
	if !hasQuorumEpoch1 {
		t.Fatalf("expected Epoch 1 to have quorum with node 1 and 2 (weights 1.0 and 1.0 out of 3.0)")
	}

	// Check that Epoch 2 does NOT have Node 2's ack
	state2 := r.trk.EpochStates[epoch2]
	if state2.Acks[2] {
		t.Fatalf("Node 2 should NOT be in Epoch 2 Acks")
	}
	t.Logf("Epoch 2 Acks: %v", state2.Acks)

	hasQuorumEpoch2 := state2.HasQuorum(r.trk.Voters)
	t.Logf("Epoch 2 HasQuorum: %v (Leader only = 1.0 / 3.5)", hasQuorumEpoch2)
	if hasQuorumEpoch2 {
		t.Fatalf("expected Epoch 2 to NOT have quorum yet")
	}
}

func TestEpochFollowerEcho(t *testing.T) {
	storage := NewMemoryStorage()
	storage.snapshot.Metadata.ConfState.Voters = []uint64{1, 2, 3}
	c := &Config{
		ID:              2, // follower
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
	}
	r := newRaft(c)
	r.becomeFollower(1, 1)

	// Construct a real MsgApp with a Context payload
	mockContext := []byte("mock-epoch-context-1234")

	from1 := uint64(1)
	to2 := uint64(2)
	zero := uint64(0)

	one := uint64(1)

	msgApp := pb.Message{
		From:    &from1,
		To:      &to2,
		Type:    pb.MsgApp.Enum(),
		Index:   &zero,
		LogTerm: &zero,
		Entries: []*pb.Entry{{Index: &one, Term: &one}},
		Commit:  &zero,
		Context: mockContext,
	}

	// Process the MsgApp
	r.Step(&msgApp)

	// Capture the resulting MsgAppResp
	msgs := r.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	resp := msgs[0]
	if resp.GetType() != pb.MsgAppResp {
		t.Fatalf("expected MsgAppResp, got %v", resp.GetType())
	}

	if string(resp.GetContext()) != string(mockContext) {
		t.Fatalf("expected echoed context %q, got %q", mockContext, resp.GetContext())
	}
}
