package tracker

import (
	"math"
	"math/rand"
	"testing"
)

func TestJitterDamping(t *testing.T) {
	// a) Steady input
	t.Run("Steady input", func(t *testing.T) {
		pt := MakeProgressTracker(10, 10)
		ptUndamped := MakeProgressTracker(10, 10)
		pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}}
		ptUndamped.Voters[0] = map[uint64]struct{}{1: {}, 2: {}}

		for i := 1; i <= 50; i++ {
			pt.UpdateEWAWeight(1, 4_000_000)
			pt.UpdateEWAWeight(2, 4_000_000)

			ptUndamped.UpdateEWAWeight(1, 4_000_000)
			ptUndamped.UpdateEWAWeight(2, 4_000_000)
			// Forcefully prevent damping by clearing the window
			if ptUndamped.WeightWindow != nil {
				ptUndamped.WeightWindow[1] = nil
				ptUndamped.WeightWindow[2] = nil
			}

			if pt.Damped != nil && pt.Damped[1] {
				t.Fatalf("Damper erroneously engaged on steady input at round %d", i)
			}

			if pt.Weight[1] != ptUndamped.Weight[1] || pt.Weight[2] != ptUndamped.Weight[2] {
				t.Fatalf("Trajectory diverged at round %d! pt.Weight[1]=%f, ptUndamped.Weight[1]=%f", i, pt.Weight[1], ptUndamped.Weight[1])
			}
		}
	})

	// b) Jittery input
	t.Run("Jitter engages damper and lowers stddev", func(t *testing.T) {
		ptDamped := MakeProgressTracker(10, 10)
		ptUndamped := MakeProgressTracker(10, 10)
		ptDamped.Voters[0] = map[uint64]struct{}{1: {}, 2: {}}
		ptUndamped.Voters[0] = map[uint64]struct{}{1: {}, 2: {}}

		rng := rand.New(rand.NewSource(42))
		var jitterLatencies [200]int64
		for i := 0; i < 200; i++ {
			// Extreme jitter (0.5ms to 10.5ms) to trigger the 0.0100 variance threshold
			latencyMs := 0.5 + rng.Float64()*10.0
			jitterLatencies[i] = int64(latencyMs * 1_000_000.0)
		}

		var undampedWeights []float64
		var dampedWeights []float64

		damperEngaged := false

		for i := 0; i < 200; i++ {
			lat := jitterLatencies[i]
			
			// Damped tracker
			ptDamped.UpdateEWAWeight(1, lat)
			ptDamped.UpdateEWAWeight(2, 2_000_000)
			
			if ptDamped.Damped != nil && ptDamped.Damped[1] {
				damperEngaged = true
				ptDamped.DampCooldown[1] = 200 // Force it to stay engaged to measure pure damped stddev
			}

			// Undamped tracker (force clear cooldown)
			ptUndamped.UpdateEWAWeight(1, lat)
			ptUndamped.UpdateEWAWeight(2, 2_000_000)
			if ptUndamped.DampCooldown != nil {
				ptUndamped.DampCooldown[1] = 0 // bypass damper
			}

			if i >= 150 {
				dampedWeights = append(dampedWeights, ptDamped.Weight[1])
				undampedWeights = append(undampedWeights, ptUndamped.Weight[1])
			}
		}

		if !damperEngaged {
			t.Fatal("Damper failed to engage under heavy jitter")
		}

		// Calculate stddev
		calcStdDev := func(weights []float64) float64 {
			m := 0.0
			for _, w := range weights { m += w }
			m /= float64(len(weights))
			v := 0.0
			for _, w := range weights { v += (w - m)*(w - m) }
			return math.Sqrt(v / float64(len(weights)))
		}

		dampedStdDev := calcStdDev(dampedWeights)
		undampedStdDev := calcStdDev(undampedWeights)

		t.Logf("Damped StdDev: %f", dampedStdDev)
		t.Logf("Undamped StdDev: %f", undampedStdDev)

		if dampedStdDev >= undampedStdDev {
			t.Fatalf("Damped stddev (%f) was not lower than undamped stddev (%f)", dampedStdDev, undampedStdDev)
		}
	})

	// c) Recovery
	t.Run("Recovery after quiet period", func(t *testing.T) {
		pt := MakeProgressTracker(10, 10)
		pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}}

		// 1. Induce damping
		rng := rand.New(rand.NewSource(42))
		damperEngaged := false
		for i := 0; i < 200; i++ {
			// Extreme jitter
			latencyMs := 0.5 + rng.Float64()*10.0
			pt.UpdateEWAWeight(1, int64(latencyMs * 1_000_000.0))
			pt.UpdateEWAWeight(2, 2_000_000)
			if pt.Damped != nil && pt.Damped[1] {
				damperEngaged = true
			}
		}

		if !damperEngaged {
			t.Fatal("Expected damper to be engaged")
		}

		// 2. Quiet period
		recovered := false
		for i := 0; i < 100; i++ {
			pt.UpdateEWAWeight(1, 8_000_000)
			pt.UpdateEWAWeight(2, 2_000_000)
			if !pt.Damped[1] {
				recovered = true
				break
			}
		}

		if !recovered {
			t.Fatal("Damper failed to disengage after quiet period")
		}
	})

	// d) Flapping guard
	t.Run("Flapping guard", func(t *testing.T) {
		pt := MakeProgressTracker(10, 10)
		pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}}

		transitions := 0
		lastState := false

		rng := rand.New(rand.NewSource(99))
		for i := 0; i < 200; i++ {
			// Randomly switch between extreme jitter (trigger) and steady (decay) every 15 rounds
			isJittery := (i/15)%2 == 0
			var latencyMs float64
			if isJittery {
				latencyMs = 0.5 + rng.Float64()*10.0
			} else {
				latencyMs = 8.0
			}
			pt.UpdateEWAWeight(1, int64(latencyMs*1_000_000.0))
			pt.UpdateEWAWeight(2, 2_000_000)

			if pt.Damped != nil {
				currState := pt.Damped[1]
				if currState != lastState {
					t.Logf("Transition at round %d: %v -> %v", i, lastState, currState)
					transitions++
					lastState = currState
				}
			}
		}

		t.Logf("Flapping transitions: %d", transitions)
		if transitions == 0 {
			t.Fatal("Flapping test failed to induce any transitions (placebo test)")
		}
		if transitions > 10 {
			t.Fatalf("Damper flapped too much! Transitions: %d", transitions)
		}
	})

	// e) No-op path
	t.Run("No-op path leaves damper untouched", func(t *testing.T) {
		pt := MakeProgressTracker(10, 10)
		pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}}
		
		pt.WeightWindow = make(map[uint64][]float64)
		pt.DampCooldown = make(map[uint64]int)
		pt.Damped = make(map[uint64]bool)
		
		pt.DampCooldown[1] = 10
		pt.Damped[1] = true
		pt.WeightWindow[1] = []float64{0.1, 0.9, 0.1, 0.9, 0.1, 0.9, 0.1, 0.9, 0.1, 0.9}

		pt.UpdateEWAWeight(1, 0)
		
		if pt.DampCooldown[1] != 10 {
			t.Fatalf("No-op path modified cooldown! Expected 10, got %d", pt.DampCooldown[1])
		}
		if !pt.Damped[1] {
			t.Fatal("No-op path cleared damped flag!")
		}
	})
}
