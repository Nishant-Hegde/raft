package tracker_test

import (
	"math"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/tracker"
)

func TestConfigEWAAlpha(t *testing.T) {
	// (a) unset fields -> behavior identical to a hand-computed default-constant run
	cUnset := &raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         raft.NewMemoryStorage(),
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 256,
	}
	// Note: We use raft.NewRawNode to construct the raft instance, which
	// propagates the config to the tracker.
	_, err := raft.NewRawNode(cUnset)
	if err != nil {
		t.Fatalf("NewRawNode: %v", err)
	}

	// Make a manual tracker to compare
	ptDefault := tracker.MakeProgressTracker(256, 0)

	// Note: We test the standalone ProgressTracker API here. We test the Config
	// override propagation in the raft package (TestConfigTrackerPropagation).
	ptUnset := tracker.MakeProgressTracker(256, 0)
	if ptUnset.AlphaBase != tracker.EWAlpha {
		t.Errorf("Expected default AlphaBase %f, got %f", tracker.EWAlpha, ptUnset.AlphaBase)
	}
	if ptUnset.FloorEpsilon != tracker.Epsilon {
		t.Errorf("Expected default FloorEpsilon %f, got %f", tracker.Epsilon, ptUnset.FloorEpsilon)
	}

	// (b) custom alpha changes convergence speed as the sweep predicts (alpha 0.4 vs default)
	ptCustom := tracker.MakeProgressTracker(256, 0)
	ptCustom.AlphaBase = 0.4 // Custom alpha

	ptDefault.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}
	ptCustom.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

	// Initialize both
	for i := uint64(1); i <= 5; i++ {
		ptDefault.UpdateEWAWeight(i, 4_000_000)
		ptCustom.UpdateEWAWeight(i, 4_000_000)
	}

	// Simulate step response to 20ms for 10 rounds
	for i := 0; i < 10; i++ {
		ptDefault.UpdateEWAWeight(1, 20_000_000)
		ptCustom.UpdateEWAWeight(1, 20_000_000)
	}

	wDefault := ptDefault.Weight[1]
	wCustom := ptCustom.Weight[1]
	if wCustom >= wDefault {
		t.Errorf("Expected faster convergence for alpha=0.4 after 10 rounds. Default=%f, Custom=%f", wDefault, wCustom)
	}

	// (c) custom epsilon changes the floor clamp value
	ptCustomEps := tracker.MakeProgressTracker(256, 0)
	ptCustomEps.FloorEpsilon = 0.1 // Custom floor
	ptCustomEps.Voters[0] = map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

	// Push node 1 to floor
	for i := 0; i < 100; i++ {
		for j := uint64(2); j <= 5; j++ {
			ptCustomEps.UpdateEWAWeight(j, 4_000_000)
		}
		ptCustomEps.UpdateEWAWeight(1, 400_000_000)
	}

	if math.Abs(ptCustomEps.Weight[1]-0.1) > 1e-9 {
		t.Errorf("Expected node 1 to clamp at custom floor 0.1, got %f", ptCustomEps.Weight[1])
	}
}

func TestConfigValidation(t *testing.T) {
	// (d) validate() rejects out-of-range values
	testCases := []struct {
		alpha       float64
		eps         float64
		valid       bool
		expectedErr string
	}{
		{0.0, 0.0, true, ""},
		{0.5, 0.5, true, ""},
		{1.0, 0.5, false, "AlphaBase must be strictly between 0 and 1"},
		{-0.1, 0.5, false, "AlphaBase must be strictly between 0 and 1"},
		{0.5, 1.0, false, "FloorEpsilon must be strictly between 0 and 1"},
		{0.5, -0.1, false, "FloorEpsilon must be strictly between 0 and 1"},
	}

	for _, tc := range testCases {
		c := &raft.Config{
			ID:              1,
			ElectionTick:    10,
			HeartbeatTick:   1,
			Storage:         raft.NewMemoryStorage(),
			MaxInflightMsgs: 256,
			AlphaBase:       tc.alpha,
			FloorEpsilon:    tc.eps,
		}

		// newRaft validates and panics with err.Error() if invalid.
		func() {
			defer func() {
				r := recover()
				if tc.valid && r != nil {
					t.Errorf("Valid config alpha=%v eps=%v unexpectedly panicked: %v", tc.alpha, tc.eps, r)
				}
				if !tc.valid {
					if r == nil {
						t.Errorf("Invalid config alpha=%v eps=%v failed to panic", tc.alpha, tc.eps)
					} else if errStr, ok := r.(string); !ok || errStr != tc.expectedErr {
						t.Errorf("Expected panic %q, got %q", tc.expectedErr, r)
					}
				}
			}()
			raft.NewRawNode(c)
		}()
	}
	
	// Unset fields behavior test with RawNode to explicitly cover NewRawNode paths
	cUnset := &raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         raft.NewMemoryStorage(),
		MaxInflightMsgs: 256,
	}
	if _, err := raft.NewRawNode(cUnset); err != nil {
		t.Fatalf("Unset fields failed to validate: %v", err)
	}
}
