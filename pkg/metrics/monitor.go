// Package metrics provides performance monitoring and metrics collection.
package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Monitor collects and exposes database performance metrics.
type Monitor struct {
	// Operation counters
	inserts    atomic.Int64
	updates    atomic.Int64
	deletes    atomic.Int64
	finds      atomic.Int64
	aggregations atomic.Int64

	// Query metrics
	queriesTotal   atomic.Int64
	queriesSlow    atomic.Int64
	queryTimeTotal atomic.Int64 // nanoseconds

	// Document metrics
	docsExamined  atomic.Int64
	docsReturned  atomic.Int64
	docsInserted  atomic.Int64
	docsUpdated   atomic.Int64
	docsDeleted   atomic.Int64

	// Storage metrics
	storageSize   atomic.Int64
	indexSize     atomic.Int64

	// Slow query tracking
	slowThreshold time.Duration
	slowQueries   chan *SlowQueryEntry
	slowQueryLog  []*SlowQueryEntry
	slowLogSize   int
	slowMu        sync.RWMutex

	// Collection stats
	collectionStats map[string]*CollectionStats
	statsMu         sync.RWMutex

	// Context for shutdown
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// SlowQueryEntry represents a slow query log entry.
type SlowQueryEntry struct {
	Timestamp   time.Time       `json:"timestamp"`
	Duration    time.Duration   `json:"duration"`
	Collection  string          `json:"collection"`
	Operation   string          `json:"operation"`
	Filter      json.RawMessage `json:"filter,omitempty"`
	DocsExamined int64          `json:"docs_examined"`
	DocsReturned int64          `json:"docs_returned"`
	Plan        string          `json:"plan,omitempty"`
}

// CollectionStats tracks per-collection statistics.
type CollectionStats struct {
	Name          string
	DocumentCount atomic.Int64
	SizeBytes     atomic.Int64
	IndexSize     atomic.Int64

	// Operation counts
	Inserts atomic.Int64
	Updates atomic.Int64
	Deletes atomic.Int64
	Finds   atomic.Int64

	// Query metrics
	QueriesTotal   atomic.Int64
	QueriesSlow    atomic.Int64
	QueryTimeTotal atomic.Int64
}

// QueryMetrics holds metrics for a single query execution.
type QueryMetrics struct {
	StartTime    time.Time
	Collection   string
	Operation    string
	Filter       interface{}
	Plan         string
}

// Options configures the monitor.
type Options struct {
	SlowQueryThreshold time.Duration
	SlowQueryLogSize   int
}

// New creates a new metrics monitor.
func New(opts Options) *Monitor {
	if opts.SlowQueryThreshold == 0 {
		opts.SlowQueryThreshold = 100 * time.Millisecond
	}
	if opts.SlowQueryLogSize == 0 {
		opts.SlowQueryLogSize = 100
	}

	ctx, cancel := context.WithCancel(context.Background())
	m := &Monitor{
		slowThreshold:   opts.SlowQueryThreshold,
		slowQueries:     make(chan *SlowQueryEntry, opts.SlowQueryLogSize),
		slowQueryLog:    make([]*SlowQueryEntry, 0, opts.SlowQueryLogSize),
		slowLogSize:     opts.SlowQueryLogSize,
		collectionStats: make(map[string]*CollectionStats),
		ctx:             ctx,
		cancel:          cancel,
	}

	m.wg.Add(1)
	go m.slowQueryProcessor()

	return m
}

// Stop shuts down the monitor.
func (m *Monitor) Stop() {
	m.cancel()
	m.wg.Wait()
	close(m.slowQueries)
}

// RecordInsert records an insert operation.
func (m *Monitor) RecordInsert(collection string, count int) {
	m.inserts.Add(1)
	m.docsInserted.Add(int64(count))

	stats := m.getOrCreateCollectionStats(collection)
	stats.Inserts.Add(1)
}

// RecordUpdate records an update operation.
func (m *Monitor) RecordUpdate(collection string, count int) {
	m.updates.Add(1)
	m.docsUpdated.Add(int64(count))

	stats := m.getOrCreateCollectionStats(collection)
	stats.Updates.Add(1)
}

// RecordDelete records a delete operation.
func (m *Monitor) RecordDelete(collection string, count int) {
	m.deletes.Add(1)
	m.docsDeleted.Add(int64(count))

	stats := m.getOrCreateCollectionStats(collection)
	stats.Deletes.Add(1)
}

// RecordFind records a find/query operation.
func (m *Monitor) RecordFind(collection string, docsReturned int) {
	m.finds.Add(1)
	m.docsReturned.Add(int64(docsReturned))

	stats := m.getOrCreateCollectionStats(collection)
	stats.Finds.Add(1)
}

// StartQuery begins tracking a query.
func (m *Monitor) StartQuery(collection, operation string) *QueryMetrics {
	return &QueryMetrics{
		StartTime:  time.Now(),
		Collection: collection,
		Operation:  operation,
	}
}

// EndQuery finishes tracking a query.
func (m *Monitor) EndQuery(qm *QueryMetrics, docsExamined, docsReturned int) {
	duration := time.Since(qm.StartTime)

	m.queriesTotal.Add(1)
	m.queryTimeTotal.Add(int64(duration))
	m.docsExamined.Add(int64(docsExamined))
	m.docsReturned.Add(int64(docsReturned))

	stats := m.getOrCreateCollectionStats(qm.Collection)
	stats.QueriesTotal.Add(1)
	stats.QueryTimeTotal.Add(int64(duration))

	// Check if slow
	if duration > m.slowThreshold {
		m.queriesSlow.Add(1)
		stats.QueriesSlow.Add(1)

		entry := &SlowQueryEntry{
			Timestamp:    time.Now(),
			Duration:     duration,
			Collection:   qm.Collection,
			Operation:    qm.Operation,
			DocsExamined: int64(docsExamined),
			DocsReturned: int64(docsReturned),
			Plan:         qm.Plan,
		}

		if qm.Filter != nil {
			entry.Filter, _ = json.Marshal(qm.Filter)
		}

		select {
		case m.slowQueries <- entry:
		default:
			// Channel full, drop entry
		}
	}
}

// UpdateStorageSize updates the storage size metric.
func (m *Monitor) UpdateStorageSize(bytes int64) {
	m.storageSize.Store(bytes)
}

// UpdateIndexSize updates the index size metric.
func (m *Monitor) UpdateIndexSize(bytes int64) {
	m.indexSize.Store(bytes)
}

// UpdateCollectionSize updates collection size metrics.
func (m *Monitor) UpdateCollectionSize(collection string, docCount, sizeBytes int64) {
	stats := m.getOrCreateCollectionStats(collection)
	stats.DocumentCount.Store(docCount)
	stats.SizeBytes.Store(sizeBytes)
}

// getOrCreateCollectionStats gets or creates collection stats.
func (m *Monitor) getOrCreateCollectionStats(name string) *CollectionStats {
	m.statsMu.RLock()
	stats, ok := m.collectionStats[name]
	m.statsMu.RUnlock()

	if ok {
		return stats
	}

	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	stats, ok = m.collectionStats[name]
	if !ok {
		stats = &CollectionStats{Name: name}
		m.collectionStats[name] = stats
	}
	return stats
}

// slowQueryProcessor processes slow query entries.
func (m *Monitor) slowQueryProcessor() {
	defer m.wg.Done()

	for {
		select {
		case entry, ok := <-m.slowQueries:
			if !ok {
				return
			}
			m.slowMu.Lock()
			m.slowQueryLog = append(m.slowQueryLog, entry)
			// Keep only last N entries
			if len(m.slowQueryLog) > m.slowLogSize {
				m.slowQueryLog = m.slowQueryLog[len(m.slowQueryLog)-m.slowLogSize:]
			}
			m.slowMu.Unlock()
		case <-m.ctx.Done():
			return
		}
	}
}

// GetSlowQueries returns the slow query log.
func (m *Monitor) GetSlowQueries() []*SlowQueryEntry {
	m.slowMu.RLock()
	defer m.slowMu.RUnlock()

	result := make([]*SlowQueryEntry, len(m.slowQueryLog))
	copy(result, m.slowQueryLog)
	return result
}

// GetCollectionStats returns stats for a collection.
func (m *Monitor) GetCollectionStats(name string) *CollectionStats {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()
	return m.collectionStats[name]
}

// GetAllCollectionStats returns stats for all collections.
func (m *Monitor) GetAllCollectionStats() []*CollectionStats {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()

	result := make([]*CollectionStats, 0, len(m.collectionStats))
	for _, stats := range m.collectionStats {
		result = append(result, stats)
	}
	return result
}

// Snapshot captures current metrics snapshot.
type Snapshot struct {
	Timestamp       time.Time            `json:"timestamp"`
	Operations      OperationsSnapshot   `json:"operations"`
	Queries         QuerySnapshot        `json:"queries"`
	Documents       DocumentSnapshot     `json:"documents"`
	Storage         StorageSnapshot      `json:"storage"`
	Memory          MemorySnapshot       `json:"memory"`
	Collections     []*CollectionStats   `json:"collections,omitempty"`
}

// OperationsSnapshot holds operation counters.
type OperationsSnapshot struct {
	Inserts      int64 `json:"inserts"`
	Updates      int64 `json:"updates"`
	Deletes      int64 `json:"deletes"`
	Finds        int64 `json:"finds"`
	Aggregations int64 `json:"aggregations"`
}

// QuerySnapshot holds query metrics.
type QuerySnapshot struct {
	Total        int64         `json:"total"`
	Slow         int64         `json:"slow"`
	AvgTimeMs    float64       `json:"avg_time_ms"`
	DocsExamined int64         `json:"docs_examined"`
	DocsReturned int64         `json:"docs_returned"`
}

// DocumentSnapshot holds document metrics.
type DocumentSnapshot struct {
	Inserted int64 `json:"inserted"`
	Updated  int64 `json:"updated"`
	Deleted  int64 `json:"deleted"`
	Returned int64 `json:"returned"`
}

// StorageSnapshot holds storage metrics.
type StorageSnapshot struct {
	DataSize  int64 `json:"data_size"`
	IndexSize int64 `json:"index_size"`
}

// MemorySnapshot holds memory metrics.
type MemorySnapshot struct {
	Alloc        uint64 `json:"alloc"`
	TotalAlloc   uint64 `json:"total_alloc"`
	Sys          uint64 `json:"sys"`
	NumGC        uint32 `json:"num_gc"`
	NumGoroutine int    `json:"num_goroutine"`
}

// Snapshot captures a point-in-time snapshot of all metrics.
func (m *Monitor) Snapshot() *Snapshot {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	queriesTotal := m.queriesTotal.Load()
	var avgTimeMs float64
	if queriesTotal > 0 {
		avgTimeMs = float64(m.queryTimeTotal.Load()) / float64(queriesTotal) / 1e6
	}

	return &Snapshot{
		Timestamp: time.Now(),
		Operations: OperationsSnapshot{
			Inserts:      m.inserts.Load(),
			Updates:      m.updates.Load(),
			Deletes:      m.deletes.Load(),
			Finds:        m.finds.Load(),
			Aggregations: m.aggregations.Load(),
		},
		Queries: QuerySnapshot{
			Total:        queriesTotal,
			Slow:         m.queriesSlow.Load(),
			AvgTimeMs:    avgTimeMs,
			DocsExamined: m.docsExamined.Load(),
			DocsReturned: m.docsReturned.Load(),
		},
		Documents: DocumentSnapshot{
			Inserted: m.docsInserted.Load(),
			Updated:  m.docsUpdated.Load(),
			Deleted:  m.docsDeleted.Load(),
			Returned: m.docsReturned.Load(),
		},
		Storage: StorageSnapshot{
			DataSize:  m.storageSize.Load(),
			IndexSize: m.indexSize.Load(),
		},
		Memory: MemorySnapshot{
			Alloc:        memStats.Alloc,
			TotalAlloc:   memStats.TotalAlloc,
			Sys:          memStats.Sys,
			NumGC:        memStats.NumGC,
			NumGoroutine: runtime.NumGoroutine(),
		},
	}
}

// PrometheusFormat exports metrics in Prometheus exposition format.
func (m *Monitor) PrometheusFormat() string {
	snap := m.Snapshot()

	return fmt.Sprintf(`# HELP mammoth_operations_total Total number of operations
# TYPE mammoth_operations_total counter
mammoth_operations_total{type="insert"} %d
mammoth_operations_total{type="update"} %d
mammoth_operations_total{type="delete"} %d
mammoth_operations_total{type="find"} %d

# HELP mammoth_queries_total Total number of queries
# TYPE mammoth_queries_total counter
mammoth_queries_total %d

# HELP mammoth_queries_slow_total Total number of slow queries
# TYPE mammoth_queries_slow_total counter
mammoth_queries_slow_total %d

# HELP mammoth_docs_examined_total Total documents examined
# TYPE mammoth_docs_examined_total counter
mammoth_docs_examined_total %d

# HELP mammoth_docs_returned_total Total documents returned
# TYPE mammoth_docs_returned_total counter
mammoth_docs_returned_total %d

# HELP mammoth_storage_bytes Storage size in bytes
# TYPE mammoth_storage_bytes gauge
mammoth_storage_bytes{type="data"} %d
mammoth_storage_bytes{type="index"} %d

# HELP mammoth_memory_bytes Memory usage in bytes
# TYPE mammoth_memory_bytes gauge
mammoth_memory_bytes{type="alloc"} %d
mammoth_memory_bytes{type="sys"} %d
`,
		snap.Operations.Inserts,
		snap.Operations.Updates,
		snap.Operations.Deletes,
		snap.Operations.Finds,
		snap.Queries.Total,
		snap.Queries.Slow,
		snap.Queries.DocsExamined,
		snap.Queries.DocsReturned,
		snap.Storage.DataSize,
		snap.Storage.IndexSize,
		snap.Memory.Alloc,
		snap.Memory.Sys,
	)
}

// HTTPHandler returns an HTTP handler for metrics endpoints.
func (m *Monitor) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", m.handleMetrics)
	mux.HandleFunc("/api/snapshot", m.handleSnapshot)
	mux.HandleFunc("/api/slow-queries", m.handleSlowQueries)
	return mux
}

func (m *Monitor) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write([]byte(m.PrometheusFormat()))
}

func (m *Monitor) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	snap := m.Snapshot()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snap)
}

func (m *Monitor) handleSlowQueries(w http.ResponseWriter, r *http.Request) {
	queries := m.GetSlowQueries()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(queries)
}

// Global monitor instance.
var globalMonitor = New(Options{})

// Global returns the global monitor instance.
func Global() *Monitor {
	return globalMonitor
}

// SetGlobal sets the global monitor instance.
func SetGlobal(m *Monitor) {
	globalMonitor = m
}
