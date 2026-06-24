package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

func init() {
	prometheus.MustRegister(fsyncDuration)
}

func main() {
	flag.Parse()
	log.Printf("Starting node %s | gRPC: %s | Metrics: %s | WAL: %s",
		*nodeID, *grpcPort, *metricsPort, *walDir)

	// Create instrumented storage — this measures real fsync latency
	storage := NewInstrumentedStorage(*nodeID, *walDir, *extraDelayMs)

	// Simulate Raft log appends (entries arriving from leader)
	go func() {
		entryIndex := uint64(1)
		for {
			// Simulate a log entry arriving every 100ms
			idx := entryIndex
            term := uint64(1)
            etype := raftpb.EntryNormal
            entry := &raftpb.Entry{
                Index: &idx,
                Term:  &term,
                Type:  &etype,
                Data:  []byte(fmt.Sprintf("write-op-%d-from-%s", entryIndex, *nodeID)),
            }

			    if err := storage.Append([]*raftpb.Entry{entry}); err != nil {
				log.Printf("Append error: %v", err)
			}

			entryIndex++
			time.Sleep(100 * time.Millisecond)
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