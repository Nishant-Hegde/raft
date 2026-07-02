package tracker

import (
	"math"
	"testing"
)

func TestWeightFloor(t *testing.T) {
	// (a) extreme-latency node clamped to exactly ε, sum==n, all >= ε
	t.Run("extreme-latency clamped", func(t *testing.T) {
		pt := MakeProgressTracker(10, 10)
		pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

		// Set weights manually to simulate before normalization
		pt.Weight = map[uint64]float64{
			1: 0.001, // Extreme latency
			2: 1.0,
			3: 1.0,
			4: 1.0,
			5: 1.0,
		}

		// Update node 1 with huge latency
		pt.UpdateEWAWeight(1, 1_000_000_000_000) // 1000s latency

		var sum float64
		for _, w := range pt.Weight {
			sum += w
			if w < Epsilon {
				t.Errorf("Weight broke floor invariant: %f < %f", w, Epsilon)
			}
		}

		if math.Abs(sum-5.0) > 1e-9 {
			t.Errorf("Sum invariant broken: expected 5.0, got %f", sum)
		}

		if math.Abs(pt.Weight[1]-Epsilon) > 1e-9 {
			t.Errorf("Node 1 was not clamped to exactly Epsilon, got: %f", pt.Weight[1])
		}

		t.Logf("Clamped weights: %v", pt.Weight)
		t.Logf("Sum: %f", sum)
	})

	// (b) healthy vector unchanged (floor dormant)
	t.Run("healthy vector unchanged", func(t *testing.T) {
		pt := MakeProgressTracker(10, 10)
		pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

		pt.Weight = map[uint64]float64{
			1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0,
		}

		pt.UpdateEWAWeight(1, 10_000_000) // 10ms

		for id, w := range pt.Weight {
			if w < Epsilon {
				t.Errorf("Node %d weight %f < %f", id, w, Epsilon)
			}
		}
		t.Logf("Healthy weights: %v", pt.Weight)
	})

	// (c) no-op path untouched
	t.Run("no-op path", func(t *testing.T) {
		pt := MakeProgressTracker(10, 10)
		pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

		// Map empty initially
		pt.UpdateEWAWeight(1, 0)
		if pt.Weight[1] != 1.0 {
			t.Errorf("Expected weight map to populate 1.0 on no-op with empty map, got %v", pt.Weight[1])
		}

		// Map with values, no real sample
		pt.Weight = map[uint64]float64{1: 0.01, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0}
		pt.UpdateEWAWeight(1, 0)
		// Should not have clamped 0.01 to 0.05
		if pt.Weight[1] > 0.013 {
			t.Errorf("No-op path modified weight incorrectly: %f", pt.Weight[1])
		}
		t.Logf("No-op weights: %v", pt.Weight)
	})

	// (d) recovery: floored node climbs back above ε when latency improves
	t.Run("recovery", func(t *testing.T) {
		pt := MakeProgressTracker(10, 10)
		pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

		pt.Weight = map[uint64]float64{
			1: 0.01, // Initially bad
			2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0,
		}

		// Improve latency
		pt.UpdateEWAWeight(1, 1_000_000) // 1ms -> high weight

		if pt.Weight[1] <= Epsilon {
			t.Errorf("Node 1 failed to recover: %f <= %f", pt.Weight[1], Epsilon)
		}
		t.Logf("Recovered weights: %v", pt.Weight)
	})
}
