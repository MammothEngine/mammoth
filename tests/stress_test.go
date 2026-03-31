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
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
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

	// Pre-populate
	value := make([]byte, 1024)
	rand.Read(value)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		eng.Put([]byte(key), value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%10000)
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

	// Pre-populate
	value := make([]byte, 256)
	rand.Read(value)
	for i := 0; i < 10000; i++ {
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
