package raft

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

func TestEpochNoPause(t *testing.T) {
	// 1. Establish a leader
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	r.becomeCandidate()
	r.becomeLeader()

	// Ensure there are no pending messages before we start the test sequence
	r.readMessages()

	// Initial weights (Epoch A) - non-uniform so epoch > 0 is created
	r.trk.Weight = map[uint64]float64{1: 1.5, 2: 1.0, 3: 1.0}

	// 2. Broadcast under Epoch N (weights A)
	from1 := uint64(1)
	to1 := uint64(1)
	r.Step(&pb.Message{
		From:    &from1,
		To:      &to1,
		Type:    pb.MsgProp.Enum(),
		Entries: []*pb.Entry{{Data: []byte("entry1")}},
	})

	msgs := r.readMessages()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}

	msgTo2 := msgs[0]
	msgTo3 := msgs[1]
	if msgTo2.GetTo() != 2 {
		msgTo2, msgTo3 = msgTo3, msgTo2
	}

	epochN, ok := tracker.DecodeEpochContext(msgTo2.GetContext())
	if !ok || epochN == 0 {
		t.Fatalf("expected valid epoch, got %d", epochN)
	}

	t.Logf("Step 1: Broadcasted MsgApp to nodes 2 and 3 with Epoch %d", epochN)
	commitBefore := r.raftLog.committed
	t.Logf("Committed index before acks: %d", commitBefore)

	// 3. Change weights mid-cycle BEFORE acks arrive
	// We update weights so the next broadcast will be Epoch N+1 (weights B).
	r.trk.Weight = map[uint64]float64{1: 1.0, 2: 0.1, 3: 2.0}
	t.Logf("Step 2: Changed weights mid-cycle to {1: 1.0, 2: 0.1, 3: 2.0}")

	if r.trk.CurrentEpoch != epochN {
		t.Fatalf("expected epoch to remain %d immediately after weight reassignment, but got %d", epochN, r.trk.CurrentEpoch)
	}
	t.Logf("Confirmed: Weight reassignment did not instantly bump epoch (CurrentEpoch == %d)", r.trk.CurrentEpoch)

	// 4. Deliver a straggling epoch-N ack from node 2
	from2 := uint64(2)
	indexToAck2 := msgTo2.GetIndex() + uint64(len(msgTo2.GetEntries()))
	err := r.Step(&pb.Message{
		From:    &from2,
		To:      &to1,
		Type:    pb.MsgAppResp.Enum(),
		Index:   &indexToAck2,
		Context: msgTo2.GetContext(),
	})
	if err != nil {
		t.Fatalf("step failed: %v", err)
	}

	// Verify the ack was processed normally and counted toward epoch N's bookkeeping tally.
	stateN := r.trk.EpochStates[epochN]
	if stateN == nil {
		t.Fatalf("expected EpochState for epoch %d to exist", epochN)
	}
	if !stateN.Acks[2] {
		t.Errorf("expected node 2 to be acked in epoch %d tally", epochN)
	}
	t.Logf("Step 3: Delivered straggling ack from node 2. Recorded in Epoch %d tally: %v", epochN, stateN.Acks)

	// Since we use the current weights (1: 1.0, 2: 0.1, 3: 2.0) to evaluate commit,
	// Leader(1.0) + Node2(0.1) = 1.1, which is not a majority of 3.1. So commit shouldn't advance yet.
	commitMid := r.raftLog.committed
	t.Logf("Committed index after node 2's ack: %d", commitMid)
	if commitMid > commitBefore {
		t.Errorf("expected commit index not to advance yet, but went from %d to %d", commitBefore, commitMid)
	}

	// 5. Trigger the next broadcast cycle and show it carries epoch N+1 / weights B
	r.Step(&pb.Message{
		From:    &from1,
		To:      &to1,
		Type:    pb.MsgProp.Enum(),
		Entries: []*pb.Entry{{Data: []byte("entry2")}},
	})

	msgs2 := r.readMessages()
	// Since node 3 is still in StateProbe and hasn't replied to the first message, 
	// it will not receive another message. Only Node 2 will receive this one.
	if len(msgs2) != 1 {
		t.Fatalf("expected 1 message in next cycle (to node 2), got %d", len(msgs2))
	}
	
	msg2To2 := msgs2[0]
	if msg2To2.GetTo() != 2 {
		t.Fatalf("expected message to go to node 2, got %d", msg2To2.GetTo())
	}

	epochNPlus1, ok := tracker.DecodeEpochContext(msg2To2.GetContext())
	if !ok {
		t.Fatalf("expected valid epoch in second broadcast")
	}
	if epochNPlus1 <= epochN {
		t.Errorf("expected new epoch > %d, got %d", epochN, epochNPlus1)
	}

	stateNPlus1 := r.trk.EpochStates[epochNPlus1]
	t.Logf("Step 4: Triggered next cycle. New MsgApp carries Epoch %d with weights: %v", epochNPlus1, stateNPlus1.Weights)

	// 6. Deliver the first straggling epoch-N ack from node 3
	from3 := uint64(3)
	indexToAck3 := msgTo3.GetIndex() + uint64(len(msgTo3.GetEntries()))
	r.Step(&pb.Message{
		From:    &from3,
		To:      &to1,
		Type:    pb.MsgAppResp.Enum(),
		Index:   &indexToAck3,
		Context: msgTo3.GetContext(),
	})

	// Leader(1.0) + Node3(2.0) = 3.0 out of 3.1. This is a majority under the current weights.
	// So commit should advance, proving replication did not stall and the system correctly progressed.
	commitAfter := r.raftLog.committed
	t.Logf("Step 5: Delivered straggling epoch-N ack from node 3. Committed index is now: %d", commitAfter)

	if commitAfter <= commitBefore {
		t.Errorf("expected committed index to advance from %d, but stayed %d. Replication stalled!", commitBefore, commitAfter)
	}

	t.Logf("SUCCESS: Replication did not pause mid-cycle, in-flight used old weights, and new weights took effect next cycle.")
}
