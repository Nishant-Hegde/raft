package tracker

import (
	"math"
	"math/rand"
	"testing"
)

func TestRawJitterCalibration(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	var jitterLatencies [200]int64
	for i := 0; i < 200; i++ {
		offset := (rng.Float64() - 0.5) * 0.5 
		latencyMs := 8.0 * (1.0 + offset)
		jitterLatencies[i] = int64(latencyMs * 1_000_000.0)
	}

	alpha := 0.2
	wRaw := 0.125 // starts at analytic target for 8ms

	var final50 []float64
	for i := 0; i < 200; i++ {
		latencyMs := float64(jitterLatencies[i]) / 1_000_000.0
		wRaw = alpha*(1.0/latencyMs) + (1-alpha)*wRaw
		if i >= 150 {
			final50 = append(final50, wRaw)
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
	
	t.Logf("Raw StdDev: %f", stddev)
}
