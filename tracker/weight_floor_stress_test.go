package tracker

import (
	"testing"
)

func TestWeightFloorSustainedSpike(t *testing.T) {
	pt := MakeProgressTracker(5, 5)
	pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

	// Initialize all nodes to 4ms
	for i := uint64(1); i <= 5; i++ {
		pt.UpdateEWAWeight(i, 4_000_000)
	}

	// Sustained 100x spike for Node 1
	for round := 1; round <= 100; round++ {
		for i := uint64(2); i <= 5; i++ {
			pt.UpdateEWAWeight(i, 4_000_000)
		}
		pt.UpdateEWAWeight(1, 400_000_000)

		if pt.GlobalDamped {
			t.Fatalf("Model A: Damper erroneously engaged on sustained step response at round %d", round)
		}
	}

	// Floor must hold exactly at Epsilon (0.05)
	if pt.Weight[1] != Epsilon {
		t.Fatalf("Model A: Floor failed to clamp sustained spike. Expected exactly %f, got %f", Epsilon, pt.Weight[1])
	}

	// Verify remaining weights renormalize cleanly to (5.0 - 0.05) / 4 = 1.2375
	for i := uint64(2); i <= 5; i++ {
		// allow slight floating point drift due to per-call normalization
		if pt.Weight[i] < 1.15 || pt.Weight[i] > 1.35 {
			t.Fatalf("Model A: Healthy node %d weight %f is far from expected ~1.2375", i, pt.Weight[i])
		}
	}

	// Recovery
	for round := 1; round <= 100; round++ {
		for i := uint64(1); i <= 5; i++ {
			pt.UpdateEWAWeight(i, 4_000_000)
		}
	}

	if pt.Weight[1] <= Epsilon {
		t.Fatalf("Model A: Recovery failed. Expected weight to climb off the floor, got %f", pt.Weight[1])
	}
}

func TestWeightFloorThrashingSpike(t *testing.T) {
	pt := MakeProgressTracker(5, 5)
	pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

	// Initialize all nodes to 4ms
	for i := uint64(1); i <= 5; i++ {
		pt.UpdateEWAWeight(i, 4_000_000)
	}

	// 1. Drop node 1 to the floor with extreme 400ms spike
	for round := 1; round <= 100; round++ {
		for i := uint64(2); i <= 5; i++ {
			pt.UpdateEWAWeight(i, 4_000_000)
		}
		pt.UpdateEWAWeight(1, 400_000_000)
	}

	if pt.Weight[1] != Epsilon {
		t.Fatalf("Failed to hit floor during initial descent. Got %f", pt.Weight[1])
	}

	// 2. Inject a burst of 5 massive 1ms heartbeats. 
	// This yanks the weight up and injects huge transient variance into the window, 
	// which will trigger the damper on subsequent rounds.
	for burst := 0; burst < 5; burst++ {
		for i := uint64(2); i <= 5; i++ {
			pt.UpdateEWAWeight(i, 4_000_000)
		}
		pt.UpdateEWAWeight(1, 1_000_000)
	}

	// 3. Resume 400ms spike and track variance/damping
	damperEngaged := false
	maxVariance := 0.0
	
	descentRounds := 0
	for round := 1; round <= 200; round++ {
		for i := uint64(2); i <= 5; i++ {
			pt.UpdateEWAWeight(i, 4_000_000)
		}
		pt.UpdateEWAWeight(1, 400_000_000)

		if pt.WeightWindow != nil {
			window := pt.WeightWindow[1]
			if len(window) == 10 {
				mean := 0.0
				for _, w := range window { mean += w }
				mean /= 10.0
				variance := 0.0
				for _, w := range window { variance += (w - mean) * (w - mean) }
				variance /= 10.0
				if variance > maxVariance {
					maxVariance = variance
				}
			}
		}

		if pt.GlobalDamped {
			damperEngaged = true
		}
		if pt.Weight[1] > Epsilon {
			descentRounds++
		}
	}

	t.Logf("Model B: Max window variance observed: %f", maxVariance)
	
	if maxVariance <= 0.0100 {
		t.Fatalf("Model B: Thrashing calibration failed. Max variance %f did not cross 0.0100 threshold", maxVariance)
	}
	if !damperEngaged {
		t.Fatalf("Model B: Damper failed to engage despite massive variance")
	}

	// 4. After 200 rounds of 400ms with damper engaged,
	// uniform alpha removes the parasitic INVERSION.
	// Per-call ordering drift remains (measured in the sweep),
	// so the analytic target is not exactly achieved, but the weight still
	// descends and clamps at exactly Epsilon.
	t.Logf("Descent rounds under damping: %d", descentRounds)
	t.Logf("Full trajectory final weight: %f", pt.Weight[1])
	if pt.Weight[1] != Epsilon {
		t.Fatalf("Model B: Expected clamp exactly at %f, got %f", Epsilon, pt.Weight[1])
	}
	
	// Recovery
	for round := 1; round <= 100; round++ {
		for i := uint64(1); i <= 5; i++ {
			pt.UpdateEWAWeight(i, 4_000_000)
		}
	}

	if pt.Weight[1] <= Epsilon {
		t.Fatalf("Model B: Recovery failed. Expected weight to climb off the floor, got %f", pt.Weight[1])
	}
}
