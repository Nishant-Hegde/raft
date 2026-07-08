package transport

import (
	"context"
	"go.etcd.io/raft/v3/raftpb"
)

type PeerManager struct {
	peers map[uint64]*PeerClient
}

func NewPeerManager(addrs map[uint64]string, myID uint64) *PeerManager {
	pm := &PeerManager{
		peers: make(map[uint64]*PeerClient),
	}
	for id, addr := range addrs {
		if id != myID {
			pm.peers[id] = NewPeerClient(addr)
		}
	}
	return pm
}

func (pm *PeerManager) Send(msg *raftpb.Message) {
	if pc, ok := pm.peers[msg.GetTo()]; ok {
		pc.Send(context.Background(), msg)
	}
}

func (pm *PeerManager) Stop() {
	for _, pc := range pm.peers {
		pc.Stop()
	}
}
