// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// storage_latency_test.go exercises the SimulatedFsyncLatency /
// LastFsyncLatencyNs feature added to MemoryStorage in Week 3 Task 1.
package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
)

// TestSimulatedFsyncLatencyDisabled verifies that when SimulatedFsyncLatency
// is unset (zero), Append() records no latency.
func TestSimulatedFsyncLatencyDisabled(t *testing.T) {
	ms := NewMemoryStorage()
	// SimulatedFsyncLatency defaults to 0 — intentionally not set.

	require.NoError(t, ms.Append(index(1).terms(1)))

	assert.Equal(t, int64(0), ms.LastFsyncLatencyNs,
		"LastFsyncLatencyNs must be 0 when SimulatedFsyncLatency is unset")
}

// TestSimulatedFsyncLatencyEnabled verifies that when SimulatedFsyncLatency is
// 10 ms, Append() sleeps for at least that duration and records a latency
// of at least 8_000_000 ns.
//
// Tolerance: we check >= 8 ms (not 10 ms) because OS schedulers may wake the
// goroutine slightly early. In practice time.Sleep always sleeps at least
// the requested duration on Linux/Windows, but the >= 8 ms guard makes the
// test robust against extreme CI scheduler jitter without being meaningless.
func TestSimulatedFsyncLatencyEnabled(t *testing.T) {
	ms := NewMemoryStorage()
	ms.SimulatedFsyncLatency = 10 * time.Millisecond

	before := time.Now()
	require.NoError(t, ms.Append(index(1).terms(1)))
	elapsed := time.Since(before)

	const minNs = int64(8_000_000) // 8 ms in nanoseconds
	assert.GreaterOrEqual(t, ms.LastFsyncLatencyNs, minNs,
		"LastFsyncLatencyNs must be >= 8 ms; got %d ns (wall elapsed %s)",
		ms.LastFsyncLatencyNs, elapsed)
}

// TestSimulatedFsyncLatencyEmptyAppend verifies that calling Append() with a
// nil/empty slice hits the pre-lock early return and leaves LastFsyncLatencyNs
// unchanged at 0.
func TestSimulatedFsyncLatencyEmptyAppend(t *testing.T) {
	ms := NewMemoryStorage()
	// Even if someone had previously set a non-zero latency, the
	// len==0 path returns before acquiring the lock and must not
	// touch LastFsyncLatencyNs.
	ms.LastFsyncLatencyNs = 0 // explicit for clarity

	require.NoError(t, ms.Append(nil))

	assert.Equal(t, int64(0), ms.LastFsyncLatencyNs,
		"LastFsyncLatencyNs must stay 0 when Append() is called with nil entries")
}

// TestStorageAppendRespMsgLatencyWiring verifies the end-to-end wiring:
// a MemoryStorage with SimulatedFsyncLatency set will cause
// newStorageAppendRespMsg to produce a *pb.Message whose
// StorageWriteLatencyNs matches LastFsyncLatencyNs (via LatencyReporter).
//
// Design note: MemoryStorage.Append() is called by the *application's* storage
// goroutine after it drains a Ready, not by the raft state machine itself.
// becomeLeader/becomeCandidate write to the unstable in-memory log only.
// We therefore call ms.Append(unstableEntries) explicitly here to simulate the
// storage goroutine, which is the call that sets LastFsyncLatencyNs.
func TestStorageAppendRespMsgLatencyWiring(t *testing.T) {
	const delay = 10 * time.Millisecond

	// Build a minimal single-node raft instance backed by a MemoryStorage
	// that has a simulated fsync delay.
	ms := newTestMemoryStorage(withPeers(1))
	ms.SimulatedFsyncLatency = delay
	r := newTestRaft(1, 10, 1, ms)

	// Drive to leader to produce unstable entries in the raft log.
	r.becomeCandidate()
	r.becomeLeader()

	// Simulate the application's storage goroutine: drain unstable entries
	// and call ms.Append(). This is what actually triggers the simulated
	// fsync delay and sets ms.LastFsyncLatencyNs.
	unstable := r.raftLog.nextUnstableEnts()
	require.NotEmpty(t, unstable, "precondition: leader must have unstable entries (no-op)")
	require.NoError(t, ms.Append(unstable))

	// Confirm latency was recorded.
	wantNs := ms.LastFsyncLatencyNs
	assert.GreaterOrEqual(t, wantNs, int64(delay/2),
		"precondition: LastFsyncLatencyNs must be >= half the simulated delay after Append()")

	// Construct a minimal Ready mirroring what acceptReady would build.
	rd := Ready{
		Entries: unstable,
	}

	// Call the function under test.
	msg := newStorageAppendRespMsg(r, rd)

	// Verify message type.
	assert.Equal(t, pb.MsgStorageAppendResp, msg.GetType(),
		"message type must be MsgStorageAppendResp")

	// Verify StorageWriteLatencyNs is non-nil and equals LastFsyncLatencyNs.
	if assert.NotNil(t, msg.StorageWriteLatencyNs,
		"StorageWriteLatencyNs must be populated when storage implements LatencyReporter") {
		assert.Equal(t, wantNs, msg.GetStorageWriteLatencyNs(),
			"StorageWriteLatencyNs must equal LastFsyncLatencyNs reported by LatencyReporter")
	}
}
