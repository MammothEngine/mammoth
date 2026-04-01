package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mammothengine/mammoth/pkg/mammoth"
)

const fieldSize = 100

// opType enumerates the kinds of operations the benchmark can perform.
type opType int

const (
	opRead   opType = iota
	opUpdate
	opInsert
)

// benchResult holds latency data for a single completed operation.
type benchResult struct {
	kind     opType
	duration time.Duration
}

// workloadDist describes the proportion of each operation type in a workload.
type workloadDist struct {
	readPct   float64
	updatePct float64
	insertPct float64
}

func getWorkload(name string) (workloadDist, bool) {
	switch name {
	case "A":
		return workloadDist{readPct: 0.50, updatePct: 0.50}, true
	case "B":
		return workloadDist{readPct: 0.95, updatePct: 0.05}, true
	case "C":
		return workloadDist{readPct: 1.00}, true
	case "D":
		return workloadDist{readPct: 0.95, insertPct: 0.05}, true
	case "F":
		return workloadDist{readPct: 0.50, updatePct: 0.50}, true // read-modify-write counted as update
	default:
		return workloadDist{}, false
	}
}

// randomField generates a random ASCII string of the given length.
func randomField(rng *rand.Rand, length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

// makeDocument builds a YCSB-style document with the given integer key.
func makeDocument(key int, rng *rand.Rand) map[string]interface{} {
	doc := make(map[string]interface{}, 11)
	doc["key"] = key
	for i := 0; i < 10; i++ {
		doc[fmt.Sprintf("field%d", i)] = randomField(rng, fieldSize)
	}
	return doc
}

// benchCmd is the entry point for "mammoth bench".
func benchCmd(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	workloadName := fs.String("workload", "A", "workload type: A, B, C, D, F")
	records := fs.Int("records", 100000, "number of initial records to load")
	operations := fs.Int("operations", 1000000, "number of operations to execute")
	threads := fs.Int("threads", 4, "number of concurrent threads")
	fs.Parse(args)

	dist, ok := getWorkload(*workloadName)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: unknown workload %q (choose A, B, C, D, F)\n", *workloadName)
		os.Exit(1)
	}

	fmt.Printf("Workload: %s\n", *workloadName)
	fmt.Printf("Records: %d\n", *records)
	fmt.Printf("Operations: %d\n", *operations)
	fmt.Printf("Threads: %d\n", *threads)
	fmt.Println()

	// Create a temporary data directory for the benchmark database.
	dataDir, err := os.MkdirTemp("", "mammoth-bench-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dataDir)

	db, err := mammoth.Open(dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	coll, err := db.Collection("ycsb")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating collection: %v\n", err)
		os.Exit(1)
	}

	// --- Load phase ---
	loadStart := time.Now()
	loadPhase(coll, *records)
	loadElapsed := time.Since(loadStart).Seconds()
	fmt.Printf("Load phase: %.2f seconds\n", loadElapsed)

	// --- Run phase ---
	runStart := time.Now()
	results := runPhase(coll, dist, *records, *operations, *threads, *workloadName)
	runElapsed := time.Since(runStart).Seconds()
	fmt.Printf("Run phase: %.2f seconds\n", runElapsed)
	fmt.Println()

	// --- Print results ---
	printResults(*workloadName, *records, *operations, *threads, loadElapsed, runElapsed, results)
}

// loadPhase inserts the initial set of documents sequentially.
func loadPhase(coll *mammoth.Collection, records int) {
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < records; i++ {
		doc := makeDocument(i, rng)
		if _, err := coll.InsertOne(doc); err != nil {
			fmt.Fprintf(os.Stderr, "Error inserting record %d: %v\n", i, err)
			os.Exit(1)
		}
	}
}

// runPhase executes the benchmark operations using multiple goroutines.
func runPhase(coll *mammoth.Collection, dist workloadDist, records, totalOps, numThreads int, workloadName string) []benchResult {
	var wg sync.WaitGroup
	var totalReads, totalUpdates, totalInserts atomic.Int64

	// Collect results from all threads in a thread-safe manner.
	var mu sync.Mutex
	var allResults []benchResult

	opsPerThread := totalOps / numThreads
	remainder := totalOps % numThreads

	for t := 0; t < numThreads; t++ {
		threadOps := opsPerThread
		if t < remainder {
			threadOps++
		}
		wg.Add(1)

		go func(threadID, ops int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(threadID)*17 + 1))
			var localResults []benchResult
			localResults = make([]benchResult, 0, ops)

			var localReads, localUpdates, localInserts int64

			// nextInsertKey tracks the key for new inserts across threads.
			// We partition the insert key space to avoid contention.
			insertKeyBase := records + threadID

			for i := 0; i < ops; i++ {
				r := rng.Float64()
				kind := pickOp(dist, r)

				switch kind {
				case opRead:
					start := time.Now()
					key := rng.Intn(records)
					coll.FindOne(map[string]interface{}{"key": key})
					dur := time.Since(start)
					localResults = append(localResults, benchResult{kind: opRead, duration: dur})
					localReads++

				case opUpdate:
					if workloadName == "F" {
						// Read-modify-write: read the document, then update it.
						key := rng.Intn(records)
						start := time.Now()
						_, _ = coll.FindOne(map[string]interface{}{"key": key})
						coll.UpdateOne(
							map[string]interface{}{"key": key},
							map[string]interface{}{
								"$set": map[string]interface{}{
									"field0": randomField(rng, fieldSize),
								},
							},
						)
						dur := time.Since(start)
						localResults = append(localResults, benchResult{kind: opUpdate, duration: dur})
					} else {
						key := rng.Intn(records)
						start := time.Now()
						coll.UpdateOne(
							map[string]interface{}{"key": key},
							map[string]interface{}{
								"$set": map[string]interface{}{
									"field0": randomField(rng, fieldSize),
								},
							},
						)
						dur := time.Since(start)
						localResults = append(localResults, benchResult{kind: opUpdate, duration: dur})
					}
					localUpdates++

				case opInsert:
					insertKey := insertKeyBase + i*numThreads
					doc := makeDocument(insertKey, rng)
					start := time.Now()
					coll.InsertOne(doc)
					dur := time.Since(start)
					localResults = append(localResults, benchResult{kind: opInsert, duration: dur})
					localInserts++
				}
			}

			totalReads.Add(localReads)
			totalUpdates.Add(localUpdates)
			totalInserts.Add(localInserts)

			mu.Lock()
			allResults = append(allResults, localResults...)
			mu.Unlock()
		}(t, threadOps)
	}

	wg.Wait()

	// Store counts in the first three elements for reporting convenience.
	// We rely on the atomic counters already updated above, so just return results.
	// The counters are accessed directly in printResults via closure is not possible,
	// so we use package-level approach: embed in a side channel.
	// Instead, let's just pass them via a wrapper — but to keep it simple, we'll
	// recalculate from the results slice in printResults.

	return allResults
}

// pickOp chooses an operation type based on the workload distribution and a random value.
func pickOp(dist workloadDist, r float64) opType {
	if r < dist.readPct {
		return opRead
	}
	if r < dist.readPct+dist.updatePct {
		return opUpdate
	}
	return opInsert
}

// printResults formats and prints the benchmark summary.
func printResults(workloadName string, records, operations, threads int, loadSecs, runSecs float64, results []benchResult) {
	throughput := float64(operations) / runSecs

	// Compute latency percentiles in microseconds.
	latencies := make([]float64, len(results))
	for i, r := range results {
		latencies[i] = float64(r.duration.Nanoseconds()) / 1000.0
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)

	fmt.Printf("Throughput: %.0f ops/sec\n", throughput)
	fmt.Println()
	fmt.Println("Latency (microseconds):")
	fmt.Printf("  p50:  %.1f\n", p50)
	fmt.Printf("  p95:  %.1f\n", p95)
	fmt.Printf("  p99:  %.1f\n", p99)
	fmt.Println()

	// Operation breakdown.
	var reads, updates, inserts int
	for _, r := range results {
		switch r.kind {
		case opRead:
			reads++
		case opUpdate:
			updates++
		case opInsert:
			inserts++
		}
	}
	total := float64(len(results))
	fmt.Println("Operation breakdown:")
	fmt.Printf("  reads:    %d (%.1f%%)\n", reads, float64(reads)/total*100)
	fmt.Printf("  updates:  %d (%.1f%%)\n", updates, float64(updates)/total*100)
	fmt.Printf("  inserts:  %d (%.1f%%)\n", inserts, float64(inserts)/total*100)
}

// percentile returns the value at the given percentile from a sorted slice.
func percentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * pct / 100.0)
	return sorted[idx]
}
