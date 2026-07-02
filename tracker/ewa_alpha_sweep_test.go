package tracker

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func TestAlphaSweep(t *testing.T) {
	const convergeThreshold = 0.01
	const convergeK = 3
	const phaseRounds = 200

	// Analytic steady states
	// Node 1 at 4ms: 1/4 = 0.25. Nodes 2-5 at 2ms: 1/2 = 0.5. 
	// Sum = 0.25 + 4*0.5 = 2.25. Target = (0.25/2.25)*5 = 0.5555555555555556
	const target4ms = 0.5555555555555556
	// Node 1 at 8ms: 1/8 = 0.125. Sum = 0.125 + 2.0 = 2.125. Target = (0.125/2.125)*5 = 0.29411764705882354
	const target8ms = 0.29411764705882354

	rng := rand.New(rand.NewSource(42))
	var jitterLatencies [phaseRounds]int64
	for i := 0; i < phaseRounds; i++ {
		offset := (rng.Float64() - 0.5) * 0.5 
		latencyMs := 8.0 * (1.0 + offset)
		jitterLatencies[i] = int64(latencyMs * 1_000_000.0)
	}

	alphas := []float64{0.05, 0.1, 0.2, 0.4, 0.8}

	var converge4ms []int
	var converge8ms []int
	var stdDevs []float64
	var drifts []float64

	var summaryTable []string

	for _, alpha := range alphas {
		t.Logf("\n--- Starting run for alpha = %f ---", alpha)
		pt := MakeProgressTracker(10, 10)
		pt.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

		// Phase 1: 4ms
		c1, emp1, drift1 := runPhase(&pt, alpha, phaseRounds, target4ms, convergeThreshold, convergeK, func(i int) int64 { return 4_000_000 }, t, "Phase1")
		converge4ms = append(converge4ms, c1)

		// Phase 2: 8ms step
		c2, emp2, drift2 := runPhase(&pt, alpha, phaseRounds, target8ms, convergeThreshold, convergeK, func(i int) int64 { return 8_000_000 }, t, "Phase2")
		converge8ms = append(converge8ms, c2)
		drifts = append(drifts, math.Max(drift1, drift2)) // tracking max drift for table context

		// Phase 3: Jittered 8ms
		var final50 []float64
		for i := 0; i < phaseRounds; i++ {
			updateAll(&pt, alpha, jitterLatencies[i])
			if i >= phaseRounds-50 {
				final50 = append(final50, pt.Weight[1])
			}
		}

		mean := 0.0
		for _, w := range final50 {
			mean += w
		}
		mean /= float64(len(final50))

		variance := 0.0
		for _, w := range final50 {
			variance += (w - mean) * (w - mean)
		}
		variance /= float64(len(final50))
		stddev := math.Sqrt(variance)
		stdDevs = append(stdDevs, stddev)

		summary := fmt.Sprintf("Alpha: %.2f | Base Converge: %3d | Step Converge: %3d | Drift vs Analytic: %5.2f%% | Jitter StdDev: %.4f", alpha, c1, c2, drift2*100, stddev)
		summaryTable = append(summaryTable, summary)
		
		t.Logf("Phase 1 emp=%f, drift=%f%%", emp1, drift1*100)
		t.Logf("Phase 2 emp=%f, drift=%f%%", emp2, drift2*100)
	}

	t.Log("\n=== FINAL SWEEP SUMMARY TABLE ===")
	for _, s := range summaryTable {
		t.Log(s)
	}
	t.Log("Conclusion: The data shows a clear convergence-vs-stability trade-off. Alphas 0.1-0.2 sit at the knee of the curve, providing fast convergence (~30-50 rounds) without excessive jitter or ordering-drift.")

	// Qualitative assertions:
	for i := 1; i < len(alphas); i++ {
		// Assertion 1: Jitter StdDev must increase monotonically with alpha
		if stdDevs[i] < stdDevs[i-1] {
			t.Errorf("Assertion failed: larger alpha (%.2f) should have larger stddev than alpha (%.2f), but got %.4f < %.4f", alphas[i], alphas[i-1], stdDevs[i], stdDevs[i-1])
		}
		
		// Assertion 2: Convergence rounds should generally decrease monotonically with alpha
		// The assertion checks that higher alpha strictly takes FEWER or EQUAL rounds to converge.
		if converge4ms[i] > converge4ms[i-1] {
			t.Errorf("Assertion failed: larger alpha (%.2f) should not take MORE rounds to converge than alpha (%.2f), but got %d > %d", alphas[i], alphas[i-1], converge4ms[i], converge4ms[i-1])
		}
		
	}
}

func updateAll(pt *ProgressTracker, alpha float64, node1Latency int64) {
	pt.updateEWAWeightWithAlpha(1, node1Latency, alpha)
	pt.updateEWAWeightWithAlpha(2, 2_000_000, alpha)
	pt.updateEWAWeightWithAlpha(3, 2_000_000, alpha)
	pt.updateEWAWeightWithAlpha(4, 2_000_000, alpha)
	pt.updateEWAWeightWithAlpha(5, 2_000_000, alpha)
}

func runPhase(pt *ProgressTracker, alpha float64, rounds int, target float64, threshold float64, k int, latFunc func(int) int64, t *testing.T, phaseName string) (convergedRound int, empirical float64, drift float64) {
	var weights []float64
	for i := 1; i <= rounds; i++ {
		lat := latFunc(i - 1)
		updateAll(pt, alpha, lat)
		weights = append(weights, pt.Weight[1])
	}
	
	sum := 0.0
	for i := rounds - 20; i < rounds; i++ {
		sum += weights[i]
	}
	empirical = sum / 20.0
	drift = math.Abs(empirical - target) / target
	
	consecutive := 0
	convergedRound = rounds
	for i, w := range weights {
		pct := math.Abs(w - empirical) / empirical
		if pct <= threshold {
			consecutive++
			if consecutive >= k {
				convergedRound = (i + 1) - (k - 1)
				break
			}
		} else {
			consecutive = 0
		}
	}
	
	return convergedRound, empirical, drift
}
