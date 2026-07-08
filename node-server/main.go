package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
    "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	nodeID      = flag.String("id", "node1", "Node ID")
	grpcPort    = flag.String("grpc-port", "50051", "gRPC port")
	metricsPort = flag.String("metrics-port", "9090", "Prometheus metrics port")
	walDir      = flag.String("wal-dir", "/wal", "WAL directory (tmpfs)")
	extraDelayMs = flag.Int("extra-delay-ms", 0, "Extra artificial delay in ms added to each fsync (set by injector)")

	fsyncDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "fsync_duration_ns",
		Help:    "Duration of real fsync calls in nanoseconds",
		Buckets: prometheus.ExponentialBuckets(1000, 2, 20),
	})
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
}

func main() {
	flag.Parse()
	myID, ok := nodeIDMap[*nodeID]
	if !ok {
		log.Fatalf("unknown node id: %s", *nodeID)
	}

	storage := raft.NewMemoryStorage()

	c := &raft.Config{
		ID:                 myID,
		ElectionTick:       10,
		HeartbeatTick:      1,
		Storage:            storage,
		MaxSizePerMsg:      4096,
		MaxInflightMsgs:    256,
		AsyncStorageWrites: true,
	}

	peers := []raft.Peer{
		{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5},
	}

	n := raft.StartNode(c, peers)
	log.Printf("Raft node started with ID %d", myID)
	log.Printf("Starting node %s | gRPC: %s | Metrics: %s | WAL: %s",
		*nodeID, *grpcPort, *metricsPort, *walDir)

	// Run the real Raft ticker + Ready() loop in the background,
	// so it doesn't block the metrics HTTP server below.
	go func() {
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    toAppend := make(chan *raftpb.Message, 256)
    toApply := make(chan *raftpb.Message, 256)

    go func() {
        for m := range toAppend {
            log.Printf("[append-thread] got %d entries", len(m.GetEntries()))
        }
    }()
    go func() {
        for m := range toApply {
            log.Printf("[apply-thread] got %d committed entries", len(m.GetEntries()))
        }
    }()

    for {
        select {
        case <-ticker.C:
            n.Tick()
        case rd := <-n.Ready():
            for _, m := range rd.Messages {
                switch m.GetTo() {
                case raft.LocalAppendThread:
                    toAppend <- m
                case raft.LocalApplyThread:
                    toApply <- m
                default:
                    log.Printf("[network] would send msg to node %d (not wired yet)", m.GetTo())
                }
            }
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

	addr := fmt.Sprintf(":%s", *metricsPort)
	log.Printf("Metrics available at http://localhost%s/metrics", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}