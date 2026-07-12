package transport

import (
	"context"
	"log"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type RaftServer struct {
	UnimplementedRaftTransportServer
	node raft.Node
}

func NewRaftServer(n raft.Node) *RaftServer {
	return &RaftServer{node: n}
}

func (s *RaftServer) SendMessage(ctx context.Context, msg *raftpb.Message) (*Ack, error) {
	if err := s.node.Step(ctx, msg); err != nil {
		// ALL n.Step errors are log-and-ack-success — never surface raft-level 
		// rejections as gRPC errors (they would trigger sender reconnect churn).
		log.Printf("[transport] Step failed but acking success: %v", err)
	}
	return &Ack{}, nil
}
