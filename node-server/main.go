package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3/node-server/transport"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	nodeID       = flag.String("id", "node1", "Node ID")
	grpcPort     = flag.String("grpc-port", "50051", "gRPC port")
	metricsPort  = flag.String("metrics-port", "9090", "Prometheus metrics port")
	walDir       = flag.String("wal-dir", "/wal", "WAL directory (tmpfs)")
	extraDelayMs = flag.Int("extra-delay-ms", 0, "Extra artificial delay in ms added to each fsync (set by injector)")

	fsyncDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "fsync_duration_ns",
		Help:    "Duration of real fsync calls in nanoseconds",
		Buckets: prometheus.ExponentialBuckets(1000, 2, 20),
	})

	var raftWeight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
        Name: "wr_raft_weight",
        Help: "Current WR-Raft EWA weight for this node",
    }, []string{"peer_id"})
)
var nodeIDMap = map[string]uint64{
	"node1": 1,
	"node2": 2,
	"node3": 3,
	"node4": 4,
	"node5": 5,
}

func init() {
	prometheus.MustRegister(fsyncDuration)
	prometheus.MustRegister(raftWeight)
}

func main() {
	flag.Parse()
	myID, ok := nodeIDMap[*nodeID]
	if !ok {
		log.Fatalf("unknown node id: %s", *nodeID)
	}

	backend := raft.NewMemoryStorage()
    storage := NewInstrumentedStorage(*nodeID, *walDir, *extraDelayMs, backend)

	c := &raft.Config{
		ID:              myID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	peers := []raft.Peer{
		{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5},
	}

	n := raft.StartNode(c, peers)
	peerAddrs := map[uint64]string{
		1: "node1:50051",
		2: "node2:50052",
		3: "node3:50053",
		4: "node4:50054",
		5: "node5:50055",
	}

	pm := transport.NewPeerManager(peerAddrs, myID)
	defer pm.Stop()

	// Start the gRPC server so other nodes can send messages to us
	grpcAddr := fmt.Sprintf(":%s", *grpcPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", grpcAddr, err)
	}
	grpcServer := grpc.NewServer()
	transport.RegisterRaftTransportServer(grpcServer, transport.NewRaftServer(n))
	go func() {
		log.Printf("gRPC server listening on %s", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("grpc server failed: %v", err)
		}
	}()
	log.Printf("Raft node started with ID %d", myID)
	log.Printf("Starting node %s | gRPC: %s | Metrics: %s | WAL: %s",
		*nodeID, *grpcPort, *metricsPort, *walDir)

	// Run the real Raft ticker + Ready() loop in the background,
	// so it doesn't block the metrics HTTP server below.
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				n.Tick()
			case rd := <-n.Ready():
				// 1. Persist hard state + unstable entries to storage
				if !raft.IsEmptyHardState(rd.HardState) {
					if err := storage.SetHardState(rd.HardState); err != nil {
						log.Printf("failed to set hard state: %v", err)
					}
				}
				if len(rd.Entries) > 0 {
					if err := storage.Append(rd.Entries); err != nil {
						log.Printf("failed to append entries: %v", err)
					}
				}

				// 2. Send outgoing messages to peers
				for _, m := range rd.Messages {
					pm.Send(m)
				}

				// 3. Apply committed entries
				for _, entry := range rd.CommittedEntries {
                    switch entry.GetType() {
                    case raftpb.EntryNormal:
                        if len(entry.Data) == 0 {
                            continue
                        }
                        // TODO: apply entry.Data to the state machine
                        log.Printf("applied normal entry: %d bytes", len(entry.Data))
                    case raftpb.EntryConfChange:
                        var cc raftpb.ConfChange
                        if err := proto.Unmarshal(entry.Data, &cc); err != nil {
                            log.Printf("failed to unmarshal ConfChange: %v", err)
                            continue
                        }
                        n.ApplyConfChange(&cc)
                        log.Printf("applied conf change: %+v", cc)
                    }
                }

				// 4. Tell raft this Ready batch is fully processed
				n.Advance()
			}
		}
	}()

	// Expose Prometheus metrics at /metrics
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "node=%s grpc=%s wal=%s\n", *nodeID, *grpcPort, *walDir)
	})
	http.HandleFunc("/propose", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := n.Propose(ctx, body); err != nil {
			http.Error(w, fmt.Sprintf("propose failed: %v", err), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "proposed %d bytes\n", len(body))
	})

	addr := fmt.Sprintf(":%s", *metricsPort)
	log.Printf("Metrics available at http://localhost%s/metrics", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}