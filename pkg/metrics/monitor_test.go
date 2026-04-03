package metrics

import (
	"net/http"
	"net/http/httptest"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestMonitor_RecordOperations(t *testing.T) {
	m := New(Options{})
	defer m.Stop()

	// Record some operations
	m.RecordInsert("users", 5)
	m.RecordUpdate("users", 3)
	m.RecordDelete("users", 1)
	m.RecordFind("users", 10)

	// Check snapshot
	snap := m.Snapshot()

	if snap.Operations.Inserts != 1 {
		t.Errorf("expected 1 insert, got %d", snap.Operations.Inserts)
	}
	if snap.Operations.Updates != 1 {
		t.Errorf("expected 1 update, got %d", snap.Operations.Updates)
	}
	if snap.Operations.Deletes != 1 {
		t.Errorf("expected 1 delete, got %d", snap.Operations.Deletes)
	}
	if snap.Operations.Finds != 1 {
		t.Errorf("expected 1 find, got %d", snap.Operations.Finds)
	}

	if snap.Documents.Inserted != 5 {
		t.Errorf("expected 5 docs inserted, got %d", snap.Documents.Inserted)
	}
	if snap.Documents.Returned != 10 {
		t.Errorf("expected 10 docs returned, got %d", snap.Documents.Returned)
	}
}

func TestMonitor_QueryTracking(t *testing.T) {
	m := New(Options{
		SlowQueryThreshold: 50 * time.Millisecond,
	})
	defer m.Stop()

	// Fast query
	qm := m.StartQuery("users", "find")
	time.Sleep(1 * time.Millisecond)
	m.EndQuery(qm, 100, 10)

	// Slow query
	qm2 := m.StartQuery("users", "find")
	time.Sleep(100 * time.Millisecond)
	qm2.Filter = map[string]string{"name": "alice"}
	m.EndQuery(qm2, 500, 5)

	// Give time for slow query processor
	time.Sleep(10 * time.Millisecond)

	snap := m.Snapshot()

	if snap.Queries.Total != 2 {
		t.Errorf("expected 2 queries, got %d", snap.Queries.Total)
	}
	if snap.Queries.Slow != 1 {
		t.Errorf("expected 1 slow query, got %d", snap.Queries.Slow)
	}
	if snap.Queries.DocsExamined != 600 {
		t.Errorf("expected 600 docs examined, got %d", snap.Queries.DocsExamined)
	}
	if snap.Queries.DocsReturned != 15 {
		t.Errorf("expected 15 docs returned, got %d", snap.Queries.DocsReturned)
	}

	// Check slow query log
	slowQueries := m.GetSlowQueries()
	if len(slowQueries) != 1 {
		t.Errorf("expected 1 slow query in log, got %d", len(slowQueries))
	}
	if slowQueries[0].Collection != "users" {
		t.Errorf("expected collection 'users', got %s", slowQueries[0].Collection)
	}
}

func TestMonitor_CollectionStats(t *testing.T) {
	m := New(Options{})
	defer m.Stop()

	// Record operations on different collections
	m.RecordInsert("users", 10)
	m.RecordInsert("users", 5)
	m.RecordFind("users", 20)
	m.RecordInsert("posts", 100)
	m.RecordFind("posts", 50)

	// Update sizes
	m.UpdateCollectionSize("users", 15, 1024)
	m.UpdateCollectionSize("posts", 100, 10000)

	// Check user collection stats
	userStats := m.GetCollectionStats("users")
	if userStats == nil {
		t.Fatal("expected user stats")
	}
	if userStats.Inserts.Load() != 2 {
		t.Errorf("expected 2 user inserts, got %d", userStats.Inserts.Load())
	}
	if userStats.Finds.Load() != 1 {
		t.Errorf("expected 1 user find, got %d", userStats.Finds.Load())
	}
	if userStats.DocumentCount.Load() != 15 {
		t.Errorf("expected 15 user docs, got %d", userStats.DocumentCount.Load())
	}

	// Check all collections
	allStats := m.GetAllCollectionStats()
	if len(allStats) != 2 {
		t.Errorf("expected 2 collections, got %d", len(allStats))
	}
}

func TestMonitor_StorageMetrics(t *testing.T) {
	m := New(Options{})
	defer m.Stop()

	m.UpdateStorageSize(1024 * 1024)     // 1 MB
	m.UpdateIndexSize(256 * 1024)        // 256 KB

	snap := m.Snapshot()

	if snap.Storage.DataSize != 1024*1024 {
		t.Errorf("expected data size 1MB, got %d", snap.Storage.DataSize)
	}
	if snap.Storage.IndexSize != 256*1024 {
		t.Errorf("expected index size 256KB, got %d", snap.Storage.IndexSize)
	}
}

func TestMonitor_PrometheusFormat(t *testing.T) {
	m := New(Options{})
	defer m.Stop()

	m.RecordInsert("test", 1)
	m.RecordFind("test", 5)
	m.UpdateStorageSize(1000)
	m.UpdateIndexSize(500)

	output := m.PrometheusFormat()

	// Check for expected metrics
	expectedMetrics := []string{
		"mammoth_operations_total",
		"mammoth_queries_total",
		"mammoth_storage_bytes",
		"mammoth_memory_bytes",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(output, metric) {
			t.Errorf("expected metric %s in output", metric)
		}
	}

	// Verify format
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "mammoth_") {
			// Should have format: metric_name{labels} value
			parts := strings.Fields(line)
			if len(parts) != 2 {
				t.Errorf("invalid metric line format: %s", line)
			}
		}
	}
}

func TestMonitor_SnapshotJSON(t *testing.T) {
	m := New(Options{})
	defer m.Stop()

	m.RecordInsert("users", 1)
	m.UpdateStorageSize(100)

	snap := m.Snapshot()

	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("failed to marshal snapshot: %v", err)
	}

	var decoded Snapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal snapshot: %v", err)
	}

	if decoded.Operations.Inserts != 1 {
		t.Errorf("expected 1 insert, got %d", decoded.Operations.Inserts)
	}
	if decoded.Storage.DataSize != 100 {
		t.Errorf("expected data size 100, got %d", decoded.Storage.DataSize)
	}
}

func TestMonitor_SlowQueryLogSize(t *testing.T) {
	m := New(Options{
		SlowQueryThreshold: 1 * time.Millisecond,
		SlowQueryLogSize:   5,
	})
	defer m.Stop()

	// Add more slow queries than the log size
	for i := 0; i < 10; i++ {
		qm := m.StartQuery("users", "find")
		time.Sleep(2 * time.Millisecond)
		m.EndQuery(qm, 10, 1)
	}

	// Give time for processor
	time.Sleep(20 * time.Millisecond)

	queries := m.GetSlowQueries()
	if len(queries) > 5 {
		t.Errorf("expected max 5 slow queries, got %d", len(queries))
	}
}

func TestGlobalMonitor(t *testing.T) {
	original := Global()
	defer SetGlobal(original)

	newMonitor := New(Options{})
	defer newMonitor.Stop()

	SetGlobal(newMonitor)

	if Global() != newMonitor {
		t.Error("global monitor not set correctly")
	}
}

func TestQueryMetrics_AverageTime(t *testing.T) {
	m := New(Options{})
	defer m.Stop()

	// Execute queries with known durations
	for i := 0; i < 3; i++ {
		qm := m.StartQuery("test", "find")
		time.Sleep(10 * time.Millisecond)
		m.EndQuery(qm, 10, 1)
	}

	snap := m.Snapshot()

	// Average should be around 10ms (with some tolerance)
	if snap.Queries.AvgTimeMs < 5 || snap.Queries.AvgTimeMs > 50 {
		t.Errorf("expected avg time around 10ms, got %.2f", snap.Queries.AvgTimeMs)
	}
}

// Test HTTPHandler
func TestMonitor_HTTPHandler(t *testing.T) {
	m := New(Options{})
	handler := m.HTTPHandler()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}

	// Test /metrics endpoint
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; version=0.0.4" {
		t.Errorf("unexpected content-type: %s", ct)
	}

	// Test /api/snapshot endpoint
	req2 := httptest.NewRequest("GET", "/api/snapshot", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Errorf("expected status 200 for snapshot, got %d", rec2.Code)
	}

	// Test /api/slow-queries endpoint
	req3 := httptest.NewRequest("GET", "/api/slow-queries", nil)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusOK {
		t.Errorf("expected status 200 for slow-queries, got %d", rec3.Code)
	}
}
