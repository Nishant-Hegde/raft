package transport

import (
"context"
"log"

"go.etcd.io/raft/v3/raftpb"
"google.golang.org/grpc"
"google.golang.org/grpc/credentials/insecure"
)

type PeerClient struct {
addr string
conn *grpc.ClientConn
cli  RaftTransportClient
}

func NewPeerClient(addr string) *PeerClient {
conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil {
log.Printf("[transport] failed to dial %s: %v", addr, err)
return &PeerClient{addr: addr}
}
return &PeerClient{
addr: addr,
conn: conn,
cli:  NewRaftTransportClient(conn),
}
}

func (pc *PeerClient) Send(ctx context.Context, msg *raftpb.Message) {
if pc.cli == nil {
return
}
if _, err := pc.cli.SendMessage(ctx, msg); err != nil {
log.Printf("[transport] send to %s failed: %v", pc.addr, err)
}
}

func (pc *PeerClient) Stop() {
if pc.conn != nil {
pc.conn.Close()
}
}
