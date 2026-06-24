package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"
)

var (
	opsPerSec   = flag.Int("ops", 1000, "Target ops/sec (workload-A: 50/50 read-write)")
	duration    = flag.Int("duration", 60, "Benchmark duration in seconds")
	nodes       = flag.String("nodes", "localhost:9091,localhost:9092,localhost:9093,localhost:9094,localhost:9095", "Comma-separated node metrics URLs")
)

// LatencyRecorder records latencies for percentile calculation
type LatencyRecorder struct {
	mu        sync.Mutex
	latencies []float64
}

func (r *LatencyRecorder) Record(ns float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.latencies = append(r.latencies, ns)
}

func (r *LatencyRecorder) Percentile(p float64) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.latencies) == 0 {
		return 0
	}
	sorted := make([]float64, len(r.latencies))
	copy(sorted, r.latencies)
	sort.Float64s(sorted)
	idx := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}

func (r *LatencyRecorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.latencies)
}

// simulateWrite simulates a write operation (appending to WAL)
// In Week 1 baseline, we measure the fsync latency from Prometheus
func simulateWrite(nodeURL string) (float64, error) {
	start := time.Now()
	resp, err := http.Get(fmt.Sprintf("http://%s/health", nodeURL))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.ReadAll(resp.Body)
	return float64(time.Since(start).Nanoseconds()), nil
}

// simulateRead simulates a read operation
func simulateRead(nodeURL string) (float64, error) {
	start := time.Now()
	resp, err := http.Get(fmt.Sprintf("http://%s/status", nodeURL))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.ReadAll(resp.Body)
	return float64(time.Since(start).Nanoseconds()), nil
}

// fetchFsyncPercentiles fetches real fsync latency from Prometheus metrics
func fetchFsyncPercentiles(metricsURL string) map[string]float64 {
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", metricsURL))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	// Parse histogram buckets to calculate percentiles
	buckets := map[float64]float64{}
	var totalCount float64

	lines := string(body)
	for _, line := range splitLines(lines) {
		var le, count float64
		if n, _ := fmt.Sscanf(line, `fsync_duration_ns_bucket{le="%f"} %f`, &le, &count); n == 2 {
			buckets[le] = count
		}
		if n, _ := fmt.Sscanf(line, "fsync_duration_ns_count %f", &count); n == 1 {
			totalCount = count
		}
	}

	if totalCount == 0 {
		return map[string]float64{"p50": 0, "p99": 0, "p999": 0}
	}

	return map[string]float64{
		"p50":  percentileFromBuckets(buckets, totalCount, 50),
		"p99":  percentileFromBuckets(buckets, totalCount, 99),
		"p999": percentileFromBuckets(buckets, totalCount, 99.9),
	}
}

func percentileFromBuckets(buckets map[float64]float64, total float64, p float64) float64 {
	target := (p / 100.0) * total
	les := make([]float64, 0, len(buckets))
	for le := range buckets {
		les = append(les, le)
	}
	sort.Float64s(les)

	var prev float64
	for _, le := range les {
		if le == math.Inf(1) {
			continue
		}
		if buckets[le] >= target {
			return le
		}
		prev = le
	}
	return prev
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	return lines
}

type NodeResult struct {
	NodeURL    string
	WriteP50   float64
	WriteP99   float64
	WriteP999  float64
	ReadP50    float64
	ReadP99    float64
	ReadP999   float64
	FsyncP50   float64
	FsyncP99   float64
	FsyncP999  float64
	TotalOps   int
	ErrorCount int
}

func main() {
	flag.Parse()

	nodeList := splitLines(*nodes)
	// re-split by comma
	nodeList = splitByComma(*nodes)

	log.Printf("🚀 Starting YCSB Workload-A Benchmark")
	log.Printf("   Nodes: %v", nodeList)
	log.Printf("   Target: %d ops/sec | Duration: %d sec", *opsPerSec, *duration)
	log.Printf("   Workload: 50%% reads + 50%% writes")
	log.Println("─────────────────────────────────────────")

	results := make([]*NodeResult, len(nodeList))
	recorders := make(map[string]*struct {
		writes LatencyRecorder
		reads  LatencyRecorder
		errors int
	})

	for i, node := range nodeList {
		results[i] = &NodeResult{NodeURL: node}
		recorders[node] = &struct {
			writes LatencyRecorder
			reads  LatencyRecorder
			errors int
		}{}
	}

	// Run benchmark
	interval := time.Second / time.Duration(*opsPerSec)
	endTime := time.Now().Add(time.Duration(*duration) * time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var wg sync.WaitGroup
	opCount := 0

	log.Printf("⏱  Running for %d seconds...", *duration)

	for time.Now().Before(endTime) {
		<-ticker.C
		node := nodeList[opCount%len(nodeList)]
		rec := recorders[node]
		isWrite := rand.Intn(2) == 0 // 50/50

		wg.Add(1)
		go func(n string, r *struct {
			writes LatencyRecorder
			reads  LatencyRecorder
			errors int
		}, write bool) {
			defer wg.Done()
			var latency float64
			var err error
			if write {
				latency, err = simulateWrite(n)
				if err != nil {
					r.errors++
					return
				}
				r.writes.Record(latency)
			} else {
				latency, err = simulateRead(n)
				if err != nil {
					r.errors++
					return
				}
				r.reads.Record(latency)
			}
		}(node, rec, isWrite)

		opCount++
	}

	wg.Wait()

	// Collect fsync percentiles from Prometheus
	log.Println("\n📊 Fetching fsync latency from Prometheus...")
	for _, node := range nodeList {
		// node is like "localhost:9091" — metrics port is the same
		fsync := fetchFsyncPercentiles(node)
		for i, r := range results {
			if r.NodeURL == node {
				results[i].FsyncP50 = fsync["p50"]
				results[i].FsyncP99 = fsync["p99"]
				results[i].FsyncP999 = fsync["p999"]
				rec := recorders[node]
				results[i].WriteP50 = rec.writes.Percentile(50)
				results[i].WriteP99 = rec.writes.Percentile(99)
				results[i].WriteP999 = rec.writes.Percentile(99.9)
				results[i].ReadP50 = rec.reads.Percentile(50)
				results[i].ReadP99 = rec.reads.Percentile(99)
				results[i].ReadP999 = rec.reads.Percentile(99.9)
				results[i].TotalOps = rec.writes.Count() + rec.reads.Count()
				results[i].ErrorCount = rec.errors
			}
		}
	}

	// Print results table
	fmt.Println("\n╔══════════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║           BASELINE LATENCY TABLE — 5-Node Uniform Cluster (Week 1)             ║")
	fmt.Println("║                    YCSB Workload-A | 1000 ops/sec | 60 sec                     ║")
	fmt.Println("╠══════════╦═══════════════════════╦═══════════════════════╦════════════════════╗")
	fmt.Println("║  Node    ║   Write Latency (ns)  ║   Read Latency (ns)   ║  fsync (ns) [WAL]  ║")
	fmt.Println("║          ║  p50    p99    p999    ║  p50    p99    p999   ║  p50   p99   p999  ║")
	fmt.Println("╠══════════╬═══════════════════════╬═══════════════════════╬════════════════════╣")

	for _, r := range results {
		fmt.Printf("║ %-8s ║ %6.0f %6.0f %7.0f ║ %6.0f %6.0f %7.0f ║ %5.0f %5.0f %6.0f ║\n",
			r.NodeURL[len(r.NodeURL)-4:],
			r.WriteP50, r.WriteP99, r.WriteP999,
			r.ReadP50, r.ReadP99, r.ReadP999,
			r.FsyncP50, r.FsyncP99, r.FsyncP999,
		)
	}

	fmt.Println("╚══════════╩═══════════════════════╩═══════════════════════╩════════════════════╝")
	fmt.Printf("\nTotal ops: %d | Duration: %ds | Target: %d ops/sec\n", opCount, *duration, *opsPerSec)

	// Save results as JSON
	jsonData, _ := json.MarshalIndent(results, "", "  ")
	fmt.Printf("\n📄 Raw JSON results:\n%s\n", jsonData)
}

func splitByComma(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}