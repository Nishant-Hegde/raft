package tracker

import (
	"testing"
)

func TestEpochSnapshotIncrementsOnlyOnWeightChange(t *testing.T) {
	p := MakeProgressTracker(10, 1000)
	p.Weight = map[uint64]float64{1: 1.5, 2: 1.0, 3: 1.0}

	// First snapshot should create epoch 1
	p.SnapshotEpoch(5, 1)
	if p.CurrentEpoch != 1 {
		t.Fatalf("expected epoch 1, got %d", p.CurrentEpoch)
	}

	// Snapshot again with no weight change -> should not increment epoch, but should update maxAppended
	p.SnapshotEpoch(10, 1)
	if p.CurrentEpoch != 1 {
		t.Fatalf("expected epoch 1 to not increment, got %d", p.CurrentEpoch)
	}
	if p.EpochStates[1].MaxAppended != 10 {
		t.Fatalf("expected maxAppended 10, got %d", p.EpochStates[1].MaxAppended)
	}

	// Change weights and snapshot -> should increment
	p.Weight[2] = 1.2
	p.SnapshotEpoch(15, 1)
	if p.CurrentEpoch != 2 {
		t.Fatalf("expected epoch 2, got %d", p.CurrentEpoch)
	}
	if p.EpochStates[2].MaxAppended != 15 {
		t.Fatalf("expected maxAppended 15, got %d", p.EpochStates[2].MaxAppended)
	}
}

func TestEncodeDecodeEpochContext(t *testing.T) {
	epoch := uint64(42)
	weights := map[uint64]float64{
		1: 1.0,
		2: 3.14,
		3: 0.5,
	}

	encoded := EncodeEpochContext(epoch, weights)

	// Decode and verify
	decodedEpoch, ok := DecodeEpochContext(encoded)
	if !ok {
		t.Fatalf("expected successful decode")
	}
	if decodedEpoch != epoch {
		t.Fatalf("expected decoded epoch %d, got %d", epoch, decodedEpoch)
	}

	// Verify failure on malformed/short context
	_, ok = DecodeEpochContext([]byte{1, 2, 3})
	if ok {
		t.Fatalf("expected fail on short context")
	}
}
