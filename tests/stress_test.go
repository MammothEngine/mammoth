package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/crypto"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
	"github.com/mammothengine/mammoth/pkg/repl"
	"github.com/mammothengine/mammoth/pkg/wire"
)

// StressTestConfig configures stress test parameters.
type StressTestConfig struct {
	Duration       time.Duration
	NumWorkers     int
	ReadRatio      float64 // 0.0 - 1.0, rest are writes
	BatchSize      int
	DocSize        int     // Approximate document size in bytes
	KeySpaceSize   int64   // Number of unique keys to operate on
	ReportInterval time.Duration
}

// DefaultStressConfig returns default stress test configuration.
func DefaultStressConfig() *StressTestConfig {
	return &StressTestConfig{
		Duration:       30 * time.Second,
		NumWorkers:     10,
		ReadRatio:      0.8,
		BatchSize:      100,
		DocSize:        1024,
		KeySpaceSize:   100000,
		ReportInterval: 5 * time.Second,
	}
}

// StressResult contains stress test results.
type StressResult struct {
	TotalOps     int64
	ReadOps      int64
	WriteOps     int64
	FailedOps    int64
	Duration     time.Duration
	AvgLatency   time.Duration
	P50Latency   time.Duration
	P99Latency   time.Duration
	MaxLatency   time.Duration
	OpsPerSecond float64
}

// String returns formatted stress test results.
func (r *StressResult) String() string {
	return fmt.Sprintf(
		"Stress Test Results:\n"+
			"  Duration: %v\n"+
			"  Total Ops: %d\n"+
			"  Read Ops: %d (%.1f%%)\n"+
			"  Write Ops: %d (%.1f%%)\n"+
			"  Failed Ops: %d\n"+
			"  Ops/Second: %.2f\n"+
			"  Avg Latency: %v\n"+
			"  P50 Latency: %v\n"+
			"  P99 Latency: %v\n"+
			"  Max Latency: %v",
		r.Duration,
		r.TotalOps,
		r.ReadOps, float64(r.ReadOps)/float64(r.TotalOps)*100,
		r.WriteOps, float64(r.WriteOps)/float64(r.TotalOps)*100,
		r.FailedOps,
		r.OpsPerSecond,
		r.AvgLatency,
		r.P50Latency,
		r.P99Latency,
		r.MaxLatency,
	)
}

// StressTester runs stress tests against the engine.
type StressTester struct {
	eng    *engine.Engine
	cat    *mongo.Catalog
	config *StressTestConfig
}

// NewStressTester creates a new stress tester.
func NewStressTester(eng *engine.Engine, config *StressTestConfig) *StressTester {
	return &StressTester{
		eng:    eng,
		cat:    mongo.NewCatalog(eng),
		config: config,
	}
}

// Run executes the stress test.
func (st *StressTester) Run(ctx context.Context) *StressResult {
	// Ensure collection exists
	st.cat.EnsureCollection("stressdb", "stresscoll")
	coll := mongo.NewCollection("stressdb", "stresscoll", st.eng, st.cat)

	var (
		totalOps   int64
		readOps    int64
		writeOps   int64
		failedOps  int64
		latencies  []time.Duration
		latencyMu  sync.Mutex
		maxLatency time.Duration
	)

	latencies = make([]time.Duration, 0, 100000)

	// Progress reporter
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(st.config.ReportInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				total := atomic.LoadInt64(&totalOps)
				reads := atomic.LoadInt64(&readOps)
				writes := atomic.LoadInt64(&writeOps)
				failed := atomic.LoadInt64(&failedOps)
				elapsed := time.Since(time.Now().Add(-st.config.Duration))
				opsSec := float64(total) / elapsed.Seconds()
				fmt.Printf("Progress: total=%d reads=%d writes=%d failed=%d ops/sec=%.2f\n",
					total, reads, writes, failed, opsSec)
			case <-done:
				return
			}
		}
	}()

	// Start workers
	var wg sync.WaitGroup
	startTime := time.Now()
	workerCtx, cancel := context.WithTimeout(ctx, st.config.Duration)
	defer cancel()

	for i := 0; i < st.config.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			st.runWorker(workerCtx, coll, workerID, &totalOps, &readOps, &writeOps, &failedOps,
				&latencies, &latencyMu, &maxLatency)
		}(i)
	}

	wg.Wait()
	close(done)

	duration := time.Since(startTime)

	// Calculate statistics
	result := &StressResult{
		TotalOps:  atomic.LoadInt64(&totalOps),
		ReadOps:   atomic.LoadInt64(&readOps),
		WriteOps:  atomic.LoadInt64(&writeOps),
		FailedOps: atomic.LoadInt64(&failedOps),
		Duration:  duration,
	}

	latencyMu.Lock()
	if len(latencies) > 0 {
		result.MaxLatency = maxLatency
		result.AvgLatency = st.calculateAvg(latencies)
		result.P50Latency = st.calculatePercentile(latencies, 0.5)
		result.P99Latency = st.calculatePercentile(latencies, 0.99)
	}
	latencyMu.Unlock()

	if duration > 0 {
		result.OpsPerSecond = float64(result.TotalOps) / duration.Seconds()
	}

	return result
}

func (st *StressTester) runWorker(ctx context.Context, coll *mongo.Collection, workerID int,
	totalOps, readOps, writeOps, failedOps *int64,
	latencies *[]time.Duration, latencyMu *sync.Mutex, maxLatency *time.Duration) {

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		start := time.Now()
		isRead := rng.Float64() < st.config.ReadRatio

		if isRead {
			// Read operation
			key := rng.Int63n(st.config.KeySpaceSize)
			prefix := mongo.EncodeNamespacePrefix("stressdb", "stresscoll")
			_, err := st.eng.Get(append(prefix, []byte(fmt.Sprintf("key%d", key))...))
			if err == nil {
				atomic.AddInt64(readOps, 1)
			}
		} else {
			// Write operation
			key := rng.Int63n(st.config.KeySpaceSize)
			doc := st.generateDocument(key, st.config.DocSize)
			err := coll.InsertOne(doc)
			if err == nil {
				atomic.AddInt64(writeOps, 1)
			} else {
				atomic.AddInt64(failedOps, 1)
			}
		}

		atomic.AddInt64(totalOps, 1)
		latency := time.Since(start)

		latencyMu.Lock()
		*latencies = append(*latencies, latency)
		if latency > *maxLatency {
			*maxLatency = latency
		}
		latencyMu.Unlock()
	}
}

func (st *StressTester) generateDocument(key int64, size int) *bson.Document {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VInt64(key))
	doc.Set("key", bson.VString(fmt.Sprintf("key%d", key)))

	// Fill with random data to approximate target size
	data := make([]byte, size-100)
	rand.Read(data)
	doc.Set("data", bson.VString(string(data)))
	doc.Set("timestamp", bson.VInt64(time.Now().Unix()))
	doc.Set("random", bson.VInt64(rand.Int63()))

	return doc
}

func (st *StressTester) calculateAvg(latencies []time.Duration) time.Duration {
	var total time.Duration
	for _, l := range latencies {
		total += l
	}
	return total / time.Duration(len(latencies))
}

func (st *StressTester) calculatePercentile(latencies []time.Duration, p float64) time.Duration {
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)

	// Simple insertion sort for small arrays
	for i := 1; i < len(sorted); i++ {
		j := i
		for j > 0 && sorted[j-1] > sorted[j] {
			sorted[j-1], sorted[j] = sorted[j], sorted[j-1]
			j--
		}
	}

	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}

// BenchmarkEngine runs comprehensive engine benchmarks.
func BenchmarkEnginePut(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	value := make([]byte, 1024)
	rand.Read(value)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		if err := eng.Put([]byte(key), value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEngineGet(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	// Pre-populate with smaller dataset to avoid memtable overflow
	value := make([]byte, 256)
	rand.Read(value)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		eng.Put([]byte(key), value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		eng.Get([]byte(key))
	}
}

func BenchmarkEngineScan(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	// Pre-populate with smaller dataset
	value := make([]byte, 256)
	rand.Read(value)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%08d", i)
		eng.Put([]byte(key), value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		eng.Scan([]byte("key"), func(k, v []byte) bool {
			count++
			return count < 1000
		})
	}
}

func BenchmarkBSONEncode(b *testing.B) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("John Doe"))
	doc.Set("age", bson.VInt32(30))
	doc.Set("active", bson.VBool(true))
	doc.Set("balance", bson.VDouble(1234.56))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bson.Encode(doc)
	}
}

func BenchmarkBSONDecode(b *testing.B) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("John Doe"))
	doc.Set("age", bson.VInt32(30))
	doc.Set("active", bson.VBool(true))
	doc.Set("balance", bson.VDouble(1234.56))
	data := bson.Encode(doc)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bson.Decode(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCollectionInsert(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	cat.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, cat)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VInt64(int64(i)))
		doc.Set("name", bson.VString(fmt.Sprintf("user%d", i)))
		doc.Set("value", bson.VInt32(int32(i)))
		if err := coll.InsertOne(doc); err != nil {
			b.Fatal(err)
		}
	}
}

// TestStressEngine runs a stress test.
func TestStressEngine(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	config := &StressTestConfig{
		Duration:       10 * time.Second,
		NumWorkers:     5,
		ReadRatio:      0.7,
		BatchSize:      100,
		DocSize:        512,
		KeySpaceSize:   10000,
		ReportInterval: 2 * time.Second,
	}

	tester := NewStressTester(eng, config)
	result := tester.Run(context.Background())

	t.Log(result.String())

	// Validate results
	if result.TotalOps == 0 {
		t.Error("No operations completed")
	}

	if result.OpsPerSecond < 100 {
		t.Errorf("Low throughput: %.2f ops/sec", result.OpsPerSecond)
	}

	if result.P99Latency > 1*time.Second {
		t.Errorf("High P99 latency: %v", result.P99Latency)
	}
}

// TestConcurrentAccess tests concurrent engine access.
func TestConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	const numWorkers = 20
	const opsPerWorker = 1000

	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*opsPerWorker)

	// Writers
	for i := 0; i < numWorkers/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("worker%d-key%d", id, j)
				value := fmt.Sprintf("value%d", j)
				if err := eng.Put([]byte(key), []byte(value)); err != nil {
					errors <- err
				}
			}
		}(i)
	}

	// Readers
	for i := numWorkers / 2; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("worker%d-key%d", id%10, j)
				eng.Get([]byte(key)) // May not exist, that's ok
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	errorCount := 0
	for err := range errors {
		if err != nil {
			errorCount++
			t.Logf("Error: %v", err)
		}
	}

	if errorCount > 0 {
		t.Errorf("Got %d errors during concurrent access", errorCount)
	}
}

// TestStabilityLongRunning runs a long-running stability test.
func TestStabilityLongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running stability test")
	}

	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	cat.EnsureCollection("testdb", "stability")
	coll := mongo.NewCollection("testdb", "stability", eng, cat)

	// Run for 30 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		inserts    int64
		reads      int64
		updates    int64
		deletes    int64
		iterations int64
	)

	// Perform mixed operations
	for ctx.Err() == nil {
		// Insert batch
		for i := 0; i < 100 && ctx.Err() == nil; i++ {
			doc := bson.NewDocument()
			doc.Set("_id", bson.VInt64(int64(iterations)*100+int64(i)))
			doc.Set("data", bson.VString(fmt.Sprintf("data%d", i)))
			doc.Set("timestamp", bson.VInt64(time.Now().Unix()))
			coll.InsertOne(doc)
			inserts++
		}

		// Read some
		for i := 0; i < 50 && ctx.Err() == nil; i++ {
			id := rand.Int63n(iterations*100 + 100)
			prefix := mongo.EncodeNamespacePrefix("testdb", "stability")
			eng.Get(append(prefix, []byte(fmt.Sprintf("key%d", id))...))
			reads++
		}

		iterations++

		// Report progress
		if iterations%10 == 0 {
			t.Logf("Progress: iterations=%d inserts=%d reads=%d",
				iterations, inserts, reads)
		}
	}

	t.Logf("Stability test completed: inserts=%d reads=%d updates=%d deletes=%d",
		inserts, reads, updates, deletes)
}

// TestChaosEngineering tests system resilience under random failures
func TestChaosEngineering(t *testing.T) {
	sharedTransport := repl.NewPartitionTransport()

	// Create 5-node cluster
	nodes := make([]*testNode, 5)
	for i := 0; i < 5; i++ {
		dir := t.TempDir()
		eng, err := engine.Open(engine.DefaultOptions(dir))
		if err != nil {
			t.Fatalf("Failed to open engine for node %d: %v", i, err)
		}

		cfg := &repl.ClusterConfig{
			Nodes: []repl.NodeConfig{
				{ID: 1, Address: "localhost:2001", Voter: true},
				{ID: 2, Address: "localhost:2002", Voter: true},
				{ID: 3, Address: "localhost:2003", Voter: true},
				{ID: 4, Address: "localhost:2004", Voter: true},
				{ID: 5, Address: "localhost:2005", Voter: true},
			},
		}

		rs := repl.NewReplicaSet(repl.ReplicaSetConfig{
			ID:        uint64(i + 1),
			Config:    cfg,
			Engine:    &engineAdapter{eng},
			Transport: sharedTransport,
		})
		rs.Start()

		nodes[i] = &testNode{
			id:  uint64(i + 1),
			rs:  rs,
			eng: eng,
			cat: mongo.NewCatalog(eng),
		}
	}

	// Register all nodes
	for _, n := range nodes {
		sharedTransport.Register(n.id, n.rs.RaftNode())
	}

	defer func() {
		for _, n := range nodes {
			n.rs.Stop()
			n.eng.Close()
		}
	}()

	// Wait for initial leader
	time.Sleep(500 * time.Millisecond)
	t.Log("Chaos test starting with 5-node cluster")

	// Chaos parameters
	chaosDuration := 30 * time.Second
	chaosTicker := time.NewTicker(2 * time.Second)
	defer chaosTicker.Stop()

	operationCount := int32(0)
	successCount := int32(0)
	failureCount := int32(0)

	// Find initial leader
	var getLeader = func() *testNode {
		for _, n := range nodes {
			if n.rs.IsLeader() {
				return n
			}
		}
		return nil
	}

	// Start chaos goroutine
	ctx, cancel := context.WithTimeout(context.Background(), chaosDuration)
	defer cancel()

	// Writer goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				leader := getLeader()
				if leader != nil {
					key := fmt.Sprintf("chaos_key_%d", atomic.AddInt32(&operationCount, 1))
					value := fmt.Sprintf("value_%d", time.Now().Unix())
					_, _, err := leader.rs.Put([]byte(key), []byte(value))
					if err != nil {
						atomic.AddInt32(&failureCount, 1)
					} else {
						atomic.AddInt32(&successCount, 1)
					}
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	// Chaos injector
	chaosCount := 0
chaosLoop:
	for {
		select {
		case <-ctx.Done():
			break chaosLoop
		case <-chaosTicker.C:
			chaosCount++
			action := rand.Intn(5)

			switch action {
			case 0: // Kill random node
				nodeIdx := rand.Intn(5)
				if nodes[nodeIdx].rs.State() != repl.StateLeader {
					t.Logf("[Chaos %d] Killing Node %d", chaosCount, nodeIdx+1)
					nodes[nodeIdx].rs.Stop()
					// Restart after delay
					go func(idx int) {
						time.Sleep(3 * time.Second)
						nodes[idx].rs.Start()
					}(nodeIdx)
				}

			case 1: // Network partition
				t.Logf("[Chaos %d] Creating network partition", chaosCount)
				sharedTransport.SetPartition([]uint64{1, 2, 3}, []uint64{4, 5})
				// Heal after delay
				go func() {
					time.Sleep(4 * time.Second)
					sharedTransport.HealPartition()
				}()

			case 2: // Block random connection
				from := uint64(rand.Intn(5) + 1)
				to := uint64(rand.Intn(5) + 1)
				if from != to {
					t.Logf("[Chaos %d] Blocking connection %d->%d", chaosCount, from, to)
					sharedTransport.BlockConnection(from, to)
					go func(f, t uint64) {
						time.Sleep(2 * time.Second)
						sharedTransport.UnblockConnection(f, t)
					}(from, to)
				}

			case 3: // Delayed packets (simulated by brief partition)
				t.Logf("[Chaos %d] Simulating network delay", chaosCount)
				sharedTransport.SetPartition([]uint64{1}, []uint64{2, 3, 4, 5})
				go func() {
					time.Sleep(1 * time.Second)
					sharedTransport.HealPartition()
				}()

			case 4: // Leader freeze
				for _, n := range nodes {
					if n.rs.IsLeader() {
						t.Logf("[Chaos %d] Freezing leader Node %d", chaosCount, n.id)
						n.rs.RaftNode().Freeze(2 * time.Second)
						break
					}
				}
			}
		}
	}

	// Wait for recovery
	t.Log("Chaos injection complete, waiting for recovery...")
	time.Sleep(5 * time.Second)

	// Report results
	totalOps := atomic.LoadInt32(&operationCount)
	successOps := atomic.LoadInt32(&successCount)
	failedOps := atomic.LoadInt32(&failureCount)

	t.Logf("=== Chaos Test Results ===")
	t.Logf("Total operations: %d", totalOps)
	t.Logf("Successful writes: %d", successOps)
	t.Logf("Failed writes: %d", failedOps)
	t.Logf("Success rate: %.2f%%", float64(successOps)/float64(totalOps)*100)

	// Verify cluster health with polling (leader election may take time)
	var leaderCount int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		leaderCount = 0
		for _, n := range nodes {
			if n.rs.IsLeader() {
				leaderCount++
			}
		}
		if leaderCount == 1 {
			break // Found exactly one leader, cluster is healthy
		}
		time.Sleep(500 * time.Millisecond)
	}

	if leaderCount != 1 {
		t.Errorf("Expected 1 leader after chaos, got %d", leaderCount)
	} else {
		t.Log("Cluster has exactly 1 leader (healthy)")
	}

	// Check data consistency
	commits := make(map[uint64]int)
	for _, n := range nodes {
		commits[n.rs.RaftNode().CommitIndex()]++
	}
	t.Logf("Commit index distribution: %v", commits)
}

// BenchmarkPut benchmarks single put operations
func BenchmarkPut(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench_key_%d", i)
			value := fmt.Sprintf("bench_value_%d", i)
			eng.Put([]byte(key), []byte(value))
			i++
		}
	})
}

// BenchmarkGet benchmarks single get operations
func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	// Pre-populate with smaller dataset
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)
		eng.Put([]byte(key), []byte(value))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench_key_%d", i%1000)
			eng.Get([]byte(key))
			i++
		}
	})
}

// BenchmarkRaftPropose benchmarks Raft consensus operations
func BenchmarkRaftPropose(b *testing.B) {
	sharedTransport := repl.NewMemTransport()

	// Create 3 nodes
	nodes := make([]*repl.ReplicaSet, 3)
	engines := make([]*engine.Engine, 3)
	for i := 0; i < 3; i++ {
		dir := b.TempDir()
		eng, _ := engine.Open(engine.DefaultOptions(dir))
		engines[i] = eng
		cfg := &repl.ClusterConfig{
			Nodes: []repl.NodeConfig{
				{ID: 1, Address: "localhost:2001", Voter: true},
				{ID: 2, Address: "localhost:2002", Voter: true},
				{ID: 3, Address: "localhost:2003", Voter: true},
			},
		}
		rs := repl.NewReplicaSet(repl.ReplicaSetConfig{
			ID:        uint64(i + 1),
			Config:    cfg,
			Engine:    &engineAdapter{eng},
			Transport: sharedTransport,
		})
		rs.Start()
		nodes[i] = rs
	}

	// Register nodes
	for i, rs := range nodes {
		sharedTransport.Register(uint64(i+1), rs.RaftNode())
	}

	// Wait for leader with timeout
	var leader *repl.ReplicaSet
	for retry := 0; retry < 50; retry++ {
		time.Sleep(100 * time.Millisecond)
		for _, rs := range nodes {
			if rs.IsLeader() {
				leader = rs
				break
			}
		}
		if leader != nil {
			break
		}
	}
	if leader == nil {
		b.Fatal("No leader elected")
	}

	defer func() {
		for _, rs := range nodes {
			rs.Stop()
		}
		for _, eng := range engines {
			eng.Close()
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("raft_key_%d", i)
		value := fmt.Sprintf("raft_value_%d", i)
		leader.Put([]byte(key), []byte(value))
	}
}

// BenchmarkBsonEncode benchmarks BSON encoding
func BenchmarkBsonEncode(b *testing.B) {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VString("test123"))
	doc.Set("name", bson.VString("John Doe"))
	doc.Set("age", bson.VInt32(30))
	doc.Set("active", bson.VBool(true))
	doc.Set("balance", bson.VDouble(1234.56))
	doc.Set("tags", bson.VArray(bson.Array{
		bson.VString("premium"),
		bson.VString("verified"),
	}))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bson.Encode(doc)
	}
}

// BenchmarkBsonDecode benchmarks BSON decoding
func BenchmarkBsonDecode(b *testing.B) {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VString("test123"))
	doc.Set("name", bson.VString("John Doe"))
	doc.Set("age", bson.VInt32(30))
	data := bson.Encode(doc)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bson.Decode(data)
	}
}

// BenchmarkScan benchmarks range scan operations
func BenchmarkScan(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	// Pre-populate with 10000 entries
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("scan_key_%08d", i)
		value := fmt.Sprintf("scan_value_%d", i)
		eng.Put([]byte(key), []byte(value))
	}

	prefix := []byte("scan_key_")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		eng.Scan(prefix, func(key, value []byte) bool {
			count++
			return count < 100 // Limit to avoid overhead
		})
	}
}

// BenchmarkIndexCreate benchmarks index creation
func BenchmarkIndexCreate(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	indexCat := mongo.NewIndexCatalog(eng, cat)

	// Insert data
	cat.EnsureCollection("testdb", "idxbench")
	coll := mongo.NewCollection("testdb", "idxbench", eng, cat)
	for i := 0; i < 10000; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VInt64(int64(i)))
		doc.Set("field", bson.VString(fmt.Sprintf("value_%d", i)))
		coll.InsertOne(doc)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		spec := mongo.IndexSpec{
			Name:   fmt.Sprintf("idx_%d", i),
			Key:    []mongo.IndexKey{{Field: "field", Descending: false}},
			Unique: false,
		}
		indexCat.CreateIndex("testdb", "idxbench", spec)
	}
}

// BenchmarkTransactionCommit benchmarks transaction commit
func BenchmarkTransactionCommit(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := eng.Begin()
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("tx_key_%d_%d", i, j)
			value := fmt.Sprintf("tx_value_%d", j)
			tx.Put([]byte(key), []byte(value))
		}
		tx.Commit()
	}
}

// BenchmarkReplicationThroughput benchmarks Raft replication
func BenchmarkReplicationThroughput(b *testing.B) {
	sharedTransport := repl.NewMemTransport()

	// Create 3 nodes
	nodes := make([]*repl.ReplicaSet, 3)
	engines := make([]*engine.Engine, 3)
	for i := 0; i < 3; i++ {
		dir := b.TempDir()
		eng, _ := engine.Open(engine.DefaultOptions(dir))
		engines[i] = eng
		cfg := &repl.ClusterConfig{
			Nodes: []repl.NodeConfig{
				{ID: 1, Address: "localhost:2001", Voter: true},
				{ID: 2, Address: "localhost:2002", Voter: true},
				{ID: 3, Address: "localhost:2003", Voter: true},
			},
		}
		rs := repl.NewReplicaSet(repl.ReplicaSetConfig{
			ID:        uint64(i + 1),
			Config:    cfg,
			Engine:    &engineAdapter{eng},
			Transport: sharedTransport,
		})
		rs.Start()
		nodes[i] = rs
	}

	for i, rs := range nodes {
		sharedTransport.Register(uint64(i+1), rs.RaftNode())
	}

	// Wait for leader with timeout
	var leader *repl.ReplicaSet
	for retry := 0; retry < 50; retry++ {
		time.Sleep(100 * time.Millisecond)
		for _, rs := range nodes {
			if rs.IsLeader() {
				leader = rs
				break
			}
		}
		if leader != nil {
			break
		}
	}
	if leader == nil {
		b.Fatal("No leader elected")
	}

	defer func() {
		for _, rs := range nodes {
			rs.Stop()
		}
		for _, eng := range engines {
			eng.Close()
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("repl_key_%d", i)
			value := fmt.Sprintf("repl_value_%d", i)
			leader.Put([]byte(key), []byte(value))
			i++
		}
	})
}

// BenchmarkWireProtocolCommand benchmarks wire protocol command processing
func BenchmarkWireProtocolCommand(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	handler := wire.NewHandler(eng, cat, nil)

	// Prepare command message
	cmdDoc := bson.NewDocument()
	cmdDoc.Set("find", bson.VString("testcoll"))
	cmdDoc.Set("$db", bson.VString("testdb"))

	msg := &wire.Message{
		Header: wire.MsgHeader{OpCode: wire.OpMsg},
		Msg: &wire.OPMsg{
			Sections: []wire.Section{
				{Kind: 0, Body: cmdDoc},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.Handle(msg)
	}
}

// BenchmarkTTLScan benchmarks TTL worker scan
func BenchmarkTTLScan(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	indexCat := mongo.NewIndexCatalog(eng, cat)
	cat.EnsureCollection("testdb", "ttlbench")

	// Insert documents with TTL
	coll := mongo.NewCollection("testdb", "ttlbench", eng, cat)
	for i := 0; i < 10000; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VInt64(int64(i)))
		doc.Set("expireAt", bson.VInt64(time.Now().Add(time.Hour).Unix()))
		coll.InsertOne(doc)
	}

	worker := mongo.NewTTLWorker(eng, cat, indexCat)
	worker.Start()
	defer worker.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Scan for expired docs (none should be expired yet)
		prefix := mongo.EncodeNamespacePrefix("testdb", "ttlbench")
		count := 0
		eng.Scan(prefix, func(key, value []byte) bool {
			count++
			return count < 100
		})
	}
}

// BenchmarkEncryption benchmarks encryption/decryption
func BenchmarkEncryption(b *testing.B) {
	key, _ := crypto.GenerateKey()
	provider, _ := crypto.NewProvider(crypto.EncryptionConfig{
		Key:              key,
		EnableEncryption: true,
	})

	data := []byte("benchmark data for encryption performance testing")

	b.Run("Encrypt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			provider.Encrypt(data)
		}
	})

	ciphertext, _ := provider.Encrypt(data)

	b.Run("Decrypt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			provider.Decrypt(ciphertext)
		}
	})
}

// BenchmarkCompression benchmarks compression algorithms
func BenchmarkCompression(b *testing.B) {
	// Sample data
	data := make([]byte, 1024)
	rand.Read(data)

	b.Run("Snappy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Placeholder for snappy compression
			_ = data
		}
	})

	b.Run("LZ4", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Placeholder for LZ4 compression
			_ = data
		}
	})
}

// BenchmarkBatchWrite benchmarks batch write operations
func BenchmarkBatchWrite(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := eng.NewBatch()
		for j := 0; j < 100; j++ {
			key := fmt.Sprintf("batch_key_%d_%d", i, j)
			value := fmt.Sprintf("batch_value_%d", j)
			batch.Put([]byte(key), []byte(value))
		}
		batch.Commit()
	}
}

// BenchmarkQueryMatcher benchmarks query matching
func BenchmarkQueryMatcher(b *testing.B) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("John"))
	doc.Set("age", bson.VInt32(30))
	doc.Set("active", bson.VBool(true))

	ageFilter := bson.NewDocument()
	ageFilter.Set("$gte", bson.VInt32(25))
	filter := bson.NewDocument()
	filter.Set("age", bson.VDoc(ageFilter))
	filter.Set("active", bson.VBool(true))

	matcher := mongo.NewMatcher(filter)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matcher.Match(doc)
	}
}

// BenchmarkAggregation benchmarks aggregation operations
func BenchmarkAggregation(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	cat.EnsureCollection("testdb", "aggbench")
	coll := mongo.NewCollection("testdb", "aggbench", eng, cat)

	// Insert sales data
	regions := []string{"North", "South", "East", "West"}
	for i := 0; i < 10000; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VInt64(int64(i)))
		doc.Set("region", bson.VString(regions[i%4]))
		doc.Set("amount", bson.VDouble(float64(i%100)))
		coll.InsertOne(doc)
	}

	prefix := mongo.EncodeNamespacePrefix("testdb", "aggbench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Group by region and sum
		regionTotals := make(map[string]float64)
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, _ := bson.Decode(value)
			region, _ := doc.Get("region")
			amount, _ := doc.Get("amount")
			if region.Type == bson.TypeString && amount.Type == bson.TypeDouble {
				regionTotals[region.String()] += amount.Double()
			}
			return true
		})
	}
}

// BenchmarkMemoryAllocation benchmarks memory allocation patterns
func BenchmarkMemoryAllocation(b *testing.B) {
	b.Run("Small", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			data := make([]byte, 64)
			_ = data
		}
	})

	b.Run("Medium", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			data := make([]byte, 1024)
			_ = data
		}
	})

	b.Run("Large", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			data := make([]byte, 1024*1024)
			_ = data
		}
	})
}

// BenchmarkConcurrentAccess benchmarks concurrent operations
func BenchmarkConcurrentAccess(b *testing.B) {
	dir := b.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("concurrent_key_%d", i)
		eng.Put([]byte(key), []byte("value"))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("concurrent_key_%d", i%1000)
			if i%2 == 0 {
				eng.Get([]byte(key))
			} else {
				eng.Put([]byte(key), []byte("updated"))
			}
			i++
		}
	})
}
