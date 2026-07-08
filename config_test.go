package raft

import (
	"testing"
)

func TestConfigTrackerPropagation(t *testing.T) {
	c := &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         NewMemoryStorage(),
		MaxInflightMsgs: 256,
		AlphaBase:       0.33,
		FloorEpsilon:    0.11,
	}

	r := newRaft(c)
	if r.trk.AlphaBase != 0.33 {
		t.Errorf("Expected tracker AlphaBase to be 0.33, got %f", r.trk.AlphaBase)
	}
	if r.trk.FloorEpsilon != 0.11 {
		t.Errorf("Expected tracker FloorEpsilon to be 0.11, got %f", r.trk.FloorEpsilon)
	}
}
