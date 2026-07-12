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
	cli    RaftTransportClient
	sendCh chan *raftpb.Message
	stopCh chan struct{}
}

func NewPeerClient(addr string) *PeerClient {
	pc := &PeerClient{
		addr:   addr,
		sendCh: make(chan *raftpb.Message, 256),
		stopCh: make(chan struct{}),
	}
	
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[transport] failed to dial %s: %v", addr, err)
	} else {
		pc.conn = conn
		pc.cli = NewRaftTransportClient(conn)
	}
	
	go pc.run()
	return pc
}

func (pc *PeerClient) run() {
	for {
		select {
		case msg := <-pc.sendCh:
			if pc.cli == nil {
				// Failed dial, log and continue
				continue
			}
			if _, err := pc.cli.SendMessage(context.Background(), msg); err != nil {
				log.Printf("[transport] send to %s failed: %v", pc.addr, err)
			}
		case <-pc.stopCh:
			return
		}
	}
}

func (pc *PeerClient) Send(ctx context.Context, msg *raftpb.Message) {
	select {
	case pc.sendCh <- msg:
	default:
		log.Printf("[transport] dropped message to %s: send channel full", pc.addr)
	}
}

func (pc *PeerClient) Stop() {
	close(pc.stopCh)
	if pc.conn != nil {
		pc.conn.Close()
	}
}
