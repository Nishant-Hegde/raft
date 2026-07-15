package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type InstrumentedStorage struct {
	mu               sync.Mutex
	ms               *raft.MemoryStorage
	walDir           string
	nodeID           string
	extraDelayMs     int
	jitterPct        int
	rng              *rand.Rand
	lastWriteLatency int64
}

func hashSeed(seed string) int64 {
	h := fnv.New64a()
	h.Write([]byte(seed))
	return int64(h.Sum64())
}

func NewInstrumentedStorage(nodeID, walDir string, extraDelayMs int, jitterPct int, seed string, backend *raft.MemoryStorage) *InstrumentedStorage {
	return &InstrumentedStorage{
		ms:           backend,
		walDir:       walDir,
		nodeID:       nodeID,
		extraDelayMs: extraDelayMs,
		jitterPct:    jitterPct,
		rng:          rand.New(rand.NewSource(hashSeed(seed))),
	}
}

func (s *InstrumentedStorage) realFsync(data []byte) error {
	path := fmt.Sprintf("%s/wal-%d.tmp", s.walDir, time.Now().UnixNano())
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer os.Remove(path)
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	// Apply artificial delay if set (simulates slow disk via tc netem equivalent)
	if s.extraDelayMs > 0 {
		if s.jitterPct > 0 {
			pct := float64(s.jitterPct) / 100.0
			u := (s.rng.Float64() * 2.0) - 1.0 // [-1.0, 1.0)
			factor := 1.0 + (u * pct)
			sleepDur := time.Duration(float64(s.extraDelayMs) * factor * float64(time.Millisecond))
			time.Sleep(sleepDur)
		} else {
			time.Sleep(time.Duration(s.extraDelayMs) * time.Millisecond)
		}
	}
	return nil
}

func (s *InstrumentedStorage) Append(entries []*raftpb.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data := []byte(fmt.Sprintf("%v", entries))

	start := time.Now()
	if err := s.realFsync(data); err != nil {
		return err
	}
	duration := time.Since(start).Nanoseconds()
	s.lastWriteLatency = duration
	fsyncDuration.Observe(float64(duration))
	log.Printf("[STAGE 1] FollowerID: %s, LastWriteLatencyNs: %d, LastWriteLatencyMs: %f\n", s.nodeID, duration, float64(duration)/1_000_000.0)

	return s.ms.Append(entries)
}

func (s *InstrumentedStorage) SetHardState(st *raftpb.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ms.SetHardState(st)
}

func (s *InstrumentedStorage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	return s.ms.InitialState()
}

func (s *InstrumentedStorage) Entries(lo, hi, maxSize uint64) ([]*raftpb.Entry, error) {
	return s.ms.Entries(lo, hi, maxSize)
}

func (s *InstrumentedStorage) Term(i uint64) (uint64, error) {
	return s.ms.Term(i)
}

func (s *InstrumentedStorage) LastIndex() (uint64, error) {
	return s.ms.LastIndex()
}

func (s *InstrumentedStorage) FirstIndex() (uint64, error) {
	return s.ms.FirstIndex()
}

func (s *InstrumentedStorage) Snapshot() (*raftpb.Snapshot, error) {
	return s.ms.Snapshot()
}

// LastWriteLatencyNs implements raft.LatencyReporter.
// Pointer receiver is required so the type assertion
// r.raftLog.storage.(LatencyReporter) succeeds when Config.Storage
// holds *InstrumentedStorage.
func (s *InstrumentedStorage) LastWriteLatencyNs() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastWriteLatency
}
