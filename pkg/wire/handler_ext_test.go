package wire

import (
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/audit"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestHandler_WithMetrics(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	metrics := &HandlerMetrics{}
	result := h.WithMetrics(metrics)

	if result != h {
		t.Error("WithMetrics should return the handler")
	}
	if h.metrics != metrics {
		t.Error("metrics should be set")
	}
}

func TestHandler_WithSlowQueryProfiler(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	profiler := &SlowQueryProfiler{}
	result := h.WithSlowQueryProfiler(profiler)

	if result != h {
		t.Error("WithSlowQueryProfiler should return the handler")
	}
	if h.slowQuery != profiler {
		t.Error("slowQuery profiler should be set")
	}
}

func TestHandler_WithAudit(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Create a temporary audit logger
	al := &audit.AuditLogger{}
	result := h.WithAudit(al)

	if result != h {
		t.Error("WithAudit should return the handler")
	}
	if h.audit != al {
		t.Error("audit logger should be set")
	}
}

func TestHandler_SetConnCountFn(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	connCount := int64(42)
	fn := func() int64 { return connCount }

	h.SetConnCountFn(fn)

	if h.connCountFn == nil {
		t.Fatal("connCountFn should be set")
	}

	// Test the function
	if h.connCountFn() != connCount {
		t.Errorf("expected connCount=%d, got %d", connCount, h.connCountFn())
	}
}

func TestHandler_StartTime(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	startTime := h.StartTime()
	if startTime.IsZero() {
		t.Error("expected non-zero start time")
	}

	// Should be recent (within last minute)
	if time.Since(startTime) > time.Minute {
		t.Error("start time should be recent")
	}
}

func TestHandler_SetSessionManager(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Create a session manager
	sm := NewSessionManager()
	h.SetSessionManager(sm)

	if h.sessionMgr != sm {
		t.Error("session manager should be set")
	}
}

func TestHandler_getEngine(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	h := NewHandler(eng, cat, nil)
	defer h.Close()

	// Create session manager
	sm := NewSessionManager()
	h.SetSessionManager(sm)

	// Test without transaction
	op := h.getEngine(123)
	if op == nil {
		t.Fatal("expected non-nil engineOps")
	}

	// Put and Get should work
	key := []byte("testkey")
	val := []byte("testvalue")

	err = op.Put(key, val)
	if err != nil {
		t.Errorf("Put: %v", err)
	}

	got, err := op.Get(key)
	if err != nil {
		t.Errorf("Get: %v", err)
	}
	if string(got) != string(val) {
		t.Errorf("expected %s, got %s", val, got)
	}

	// Delete
	op.Delete(key)
}

func TestHandler_getEngine_WithTransaction(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	h := NewHandler(eng, cat, nil)
	defer h.Close()

	// Create session manager and start a transaction
	sm := NewSessionManager()
	h.SetSessionManager(sm)

	connID := uint64(456)
	dbName := "testdb"

	// Start transaction with correct signature
	sm.StartTransaction(connID, eng, dbName)

	// Test with transaction
	op := h.getEngine(connID)
	if op == nil {
		t.Fatal("expected non-nil engineOps")
	}

	// Put should work within transaction
	key := []byte("txkey")
	val := []byte("txvalue")

	err = op.Put(key, val)
	if err != nil {
		t.Errorf("Put: %v", err)
	}

	// Delete should work (not panic)
	op.Delete(key)

	// Commit transaction
	sm.CommitTransaction(connID)
}

func TestEngineWrapper_ops(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	wrapper := &engineWrapper{eng: eng}

	key := []byte("wrapperkey")
	val := []byte("wrappervalue")

	// Test Put
	err = wrapper.Put(key, val)
	if err != nil {
		t.Errorf("Put: %v", err)
	}

	// Test Get
	got, err := wrapper.Get(key)
	if err != nil {
		t.Errorf("Get: %v", err)
	}
	if string(got) != string(val) {
		t.Errorf("expected %s, got %s", val, got)
	}

	// Test Delete
	wrapper.Delete(key)
}

func TestTxWrapper_ops(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	tx := eng.Begin()
	if tx == nil {
		t.Fatal("expected non-nil transaction")
	}
	defer tx.Rollback()

	wrapper := &txWrapper{tx: tx}

	key := []byte("txwrapperkey")
	val := []byte("txwrappervalue")

	// Test Put (should not error)
	err = wrapper.Put(key, val)
	if err != nil {
		t.Errorf("Put: %v", err)
	}

	// Note: Get in a transaction may not see uncommitted writes
	// depending on implementation, so we just verify it doesn't panic
	_, _ = wrapper.Get(key)

	// Test Delete (should not panic)
	wrapper.Delete(key)
}
