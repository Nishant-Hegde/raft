package transport

import (
	"context"
	"log"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PeerClient struct {
	addr   string
	conn   *grpc.ClientConn
	client RaftTransportClient
	sendCh chan *raftpb.Message
	stopCh chan struct{}
}

func NewPeerClient(addr string) *PeerClient {
	pc := &PeerClient{
		addr:   addr,
		sendCh: make(chan *raftpb.Message, 256),
		stopCh: make(chan struct{}),
	}
	go pc.run()
	return pc
}

func (pc *PeerClient) Stop() {
	close(pc.stopCh)
}

func (pc *PeerClient) Send(ctx context.Context, msg *raftpb.Message) error {
	select {
	case pc.sendCh <- msg:
		return nil
	default:
		log.Printf("[transport] dropped message to %s: send channel full", pc.addr)
		return nil // raft tolerates dropped messages
	}
}

func (pc *PeerClient) run() {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(pc.addr, opts...)
	if err != nil {
		log.Printf("[transport] failed to create client for %s: %v", pc.addr, err)
		return
	}
	pc.conn = conn
	pc.client = NewRaftTransportClient(conn)

	defer conn.Close()

	for {
		select {
		case <-pc.stopCh:
			return
		case msg := <-pc.sendCh:
			_, err := pc.client.SendMessage(context.Background(), msg)
			if err != nil {
				log.Printf("[transport] send to node%d failed: %v", msg.GetTo(), err)
			}
		}
	}
}
