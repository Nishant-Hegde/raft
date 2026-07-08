package transport

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
)

func TestTransportSmoke(t *testing.T) {
	// Start two raft nodes in-process, wired over our real gRPC transport.
	// Node 1
	addr1 := "localhost:50051"
	addr2 := "localhost:50052"

	lis1, err := net.Listen("tcp", addr1)
	if err != nil {
		t.Fatalf("failed to listen on %s: %v", addr1, err)
	}
	lis2, err := net.Listen("tcp", addr2)
	if err != nil {
		t.Fatalf("failed to listen on %s: %v", addr2, err)
	}

	storage1 := raft.NewMemoryStorage()
	storage2 := raft.NewMemoryStorage()

	peers := []raft.Peer{{ID: 1}, {ID: 2}}

	c1 := &raft.Config{
		ID:                 1,
		ElectionTick:       10,
		HeartbeatTick:      1,
		Storage:            storage1,
		MaxSizePerMsg:      4096,
		MaxInflightMsgs:    256,
		AsyncStorageWrites: false, // keep it simple for the test
	}
	c2 := &raft.Config{
		ID:                 2,
		ElectionTick:       10,
		HeartbeatTick:      1,
		Storage:            storage2,
		MaxSizePerMsg:      4096,
		MaxInflightMsgs:    256,
		AsyncStorageWrites: false,
	}

	n1 := raft.StartNode(c1, peers)
	n2 := raft.StartNode(c2, peers)

	// Setup gRPC servers
	grpcServer1 := grpc.NewServer()
	RegisterRaftTransportServer(grpcServer1, NewRaftServer(n1))
	go grpcServer1.Serve(lis1)
	defer grpcServer1.Stop()

	grpcServer2 := grpc.NewServer()
	RegisterRaftTransportServer(grpcServer2, NewRaftServer(n2))
	go grpcServer2.Serve(lis2)
	defer grpcServer2.Stop()

	addrs := map[uint64]string{
		1: addr1,
		2: addr2,
	}

	pm1 := NewPeerManager(addrs, 1)
	defer pm1.Stop()

	pm2 := NewPeerManager(addrs, 2)
	defer pm2.Stop()

	// Simple loops to tick and advance
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runNode := func(n raft.Node, pm *PeerManager, id uint64, storage *raft.MemoryStorage) {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n.Tick()
			case rd := <-n.Ready():
				storage.Append(rd.Entries)
				
				for _, m := range rd.Messages {
					if m.GetTo() != id {
						pm.Send(m)
					}
				}
				
				if len(rd.CommittedEntries) > 0 {
					for _, e := range rd.CommittedEntries {
						if e.GetType() == raftpb.EntryNormal && len(e.Data) > 0 {
							log.Printf("Node %d committed entry: %s", id, string(e.Data))
						}
					}
				}

				n.Advance()
			}
		}
	}

	go runNode(n1, pm1, 1, storage1)
	go runNode(n2, pm2, 2, storage2)

	// Propose an entry to node 1
	time.Sleep(1 * time.Second) // wait for election
	log.Printf("Proposing entry to Node 1")
	err = n1.Propose(context.Background(), []byte("hello transport"))
	if err != nil {
		t.Fatalf("failed to propose: %v", err)
	}

	// Wait for commit
	time.Sleep(2 * time.Second)
	
	// Stop nodes
	n1.Stop()
	n2.Stop()
}
