package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type InstrumentedStorage struct {
	mu           sync.Mutex
	ms           *raft.MemoryStorage
	walDir       string
	nodeID       string
	extraDelayMs int
}

func NewInstrumentedStorage(nodeID, walDir string, extraDelayMs int) *InstrumentedStorage {
	return &InstrumentedStorage{
		ms:           raft.NewMemoryStorage(),
		walDir:       walDir,
		nodeID:       nodeID,
		extraDelayMs: extraDelayMs,
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
		time.Sleep(time.Duration(s.extraDelayMs) * time.Millisecond)
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
	fsyncDuration.Observe(float64(duration))

	return s.ms.Append(entries)
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