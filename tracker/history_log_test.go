package tracker

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestEpochHistoryLoggerBasic(t *testing.T) {
	pt := MakeProgressTracker(10, 10)
	pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}}

	var logLines []string
	pt.HistoryLogger = func(epoch uint64, ct float64, maxAppended uint64, weights map[uint64]float64) {
		// Verify weights map is independent (modifying it shouldn't affect the caller, though we just read it here)
		// We format it as requested: epoch, ct, id:weight
		var ids []uint64
		for id := range weights {
			ids = append(ids, id)
		}
		slices.Sort(ids)

		var parts []string
		parts = append(parts, fmt.Sprintf("%d", epoch))
		parts = append(parts, fmt.Sprintf("%.3f", ct))
		parts = append(parts, fmt.Sprintf("%d", maxAppended))
		for _, id := range ids {
			parts = append(parts, fmt.Sprintf("%d:%.3f", id, weights[id]))
		}
		logLines = append(logLines, strings.Join(parts, ","))
	}

	// 1. Initial snapshot (epoch 1) due to non-uniform weights
	pt.Weight = map[uint64]float64{1: 1.0, 2: 1.0, 3: 0.5}
	pt.SnapshotEpoch(10, 1)

	// 2. Another epoch due to weight change
	pt.Weight = map[uint64]float64{1: 1.0, 2: 0.2, 3: 0.5}
	pt.SnapshotEpoch(20, 1)

	// 3. No change -> no epoch
	pt.SnapshotEpoch(30, 1)

	if len(logLines) != 2 {
		t.Fatalf("Expected 2 log lines, got %d", len(logLines))
	}

	expected1 := "1,1.250,10,1:1.000,2:1.000,3:0.500"
	if logLines[0] != expected1 {
		t.Errorf("Line 1 mismatch: expected %q, got %q", expected1, logLines[0])
	}

	expected2 := "2,0.850,20,1:1.000,2:0.200,3:0.500"
	if logLines[1] != expected2 {
		t.Errorf("Line 2 mismatch: expected %q, got %q", expected2, logLines[1])
	}
}

func TestEpochHistoryLoggerDisabledAllocs(t *testing.T) {
	pt := MakeProgressTracker(10, 10)
	pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}}
	// intentionally nil logger
	pt.HistoryLogger = nil
	pt.Weight = map[uint64]float64{1: 1.0, 2: 1.0, 3: 1.0}

	// First we do one snapshot to advance past epoch 0 so we test the steady state
	pt.Weight = map[uint64]float64{1: 1.0, 2: 0.5, 3: 1.0}
	pt.SnapshotEpoch(10, 1)

	allocs := testing.AllocsPerRun(100, func() {
		// Call SnapshotEpoch with unchanged weights. This should trigger no allocations.
		pt.SnapshotEpoch(20, 1)
	})

	if allocs > 0 {
		t.Fatalf("Expected 0 allocations on disabled/steady path, got %f", allocs)
	}
}

func TestEpochHistoryLoggerMultiEpoch(t *testing.T) {
	pt := MakeProgressTracker(5, 5)
	pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

	var epochs []uint64
	pt.HistoryLogger = func(epoch uint64, ct float64, maxAppended uint64, weights map[uint64]float64) {
		if len(epochs) > 0 && epoch <= epochs[len(epochs)-1] {
			t.Fatalf("Epoch did not strictly increase: previous %d, got %d", epochs[len(epochs)-1], epoch)
		}
		epochs = append(epochs, epoch)
	}

	// 1. Initialize all nodes to 4ms
	for i := uint64(1); i <= 5; i++ {
		pt.UpdateEWAWeight(i, 4_000_000)
	}
	pt.SnapshotEpoch(10, 1)

	// 2. Drop node 1 to the floor with extreme 400ms spike
	for round := 1; round <= 100; round++ {
		for i := uint64(2); i <= 5; i++ {
			pt.UpdateEWAWeight(i, 4_000_000)
		}
		pt.UpdateEWAWeight(1, 400_000_000)
		pt.SnapshotEpoch(uint64(10+round), 1)
	}

	if len(epochs) < 2 {
		t.Fatalf("Expected multiple epochs to be logged, got %d", len(epochs))
	}

	// Ensure they are strictly monotonically increasing
	for i := 1; i < len(epochs); i++ {
		if epochs[i] <= epochs[i-1] {
			t.Fatalf("Epoch sequence is not monotonically increasing: %v", epochs)
		}
	}
}
