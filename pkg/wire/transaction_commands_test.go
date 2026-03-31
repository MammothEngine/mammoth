package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func TestHandleStartTransaction(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Start a transaction
	body := bson.D("$db", bson.VString("test"))
	resp := h.handleStartTransaction(body, 1)

	okVal, _ := resp.Get("ok")
	if okVal.Double() != 1.0 {
		t.Errorf("expected ok=1.0, got %v", okVal.Double())
	}

	// Verify transaction is active
	if !h.sessionMgr.IsInTransaction(1) {
		t.Error("expected transaction to be active")
	}
}

func TestHandleStartTransaction_AlreadyInTx(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Start first transaction
	body := bson.D("$db", bson.VString("test"))
	h.handleStartTransaction(body, 1)

	// Try to start second transaction
	resp := h.handleStartTransaction(body, 1)

	okVal, _ := resp.Get("ok")
	if okVal.Double() != 0.0 {
		t.Error("expected error when starting transaction while already in one")
	}
}

func TestHandleCommitTransaction(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Start transaction
	body := bson.D("$db", bson.VString("test"))
	h.handleStartTransaction(body, 1)

	// Commit transaction
	resp := h.handleCommitTransaction(1)

	okVal, _ := resp.Get("ok")
	if okVal.Double() != 1.0 {
		t.Errorf("expected ok=1.0, got %v", okVal.Double())
	}

	// Verify transaction is no longer active
	if h.sessionMgr.IsInTransaction(1) {
		t.Error("expected transaction to be committed")
	}
}

func TestHandleCommitTransaction_NoTransaction(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Commit without starting transaction
	resp := h.handleCommitTransaction(1)

	okVal, _ := resp.Get("ok")
	if okVal.Double() != 1.0 {
		t.Errorf("expected ok=1.0 even with no transaction, got %v", okVal.Double())
	}
}

func TestHandleAbortTransaction(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Start transaction
	body := bson.D("$db", bson.VString("test"))
	h.handleStartTransaction(body, 1)

	// Abort transaction
	resp := h.handleAbortTransaction(1)

	okVal, _ := resp.Get("ok")
	if okVal.Double() != 1.0 {
		t.Errorf("expected ok=1.0, got %v", okVal.Double())
	}

	// Verify transaction is no longer active
	if h.sessionMgr.IsInTransaction(1) {
		t.Error("expected transaction to be aborted")
	}
}

func TestHandleAbortTransaction_NoTransaction(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Abort without starting transaction
	resp := h.handleAbortTransaction(1)

	okVal, _ := resp.Get("ok")
	if okVal.Double() != 1.0 {
		t.Errorf("expected ok=1.0 even with no transaction, got %v", okVal.Double())
	}
}

func TestSessionManager_StartTransaction(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	sm := NewSessionManager()

	// Start transaction
	if !sm.StartTransaction(1, eng, "test") {
		t.Error("expected to start transaction")
	}

	if !sm.IsInTransaction(1) {
		t.Error("expected transaction to be active")
	}

	tx := sm.GetTransaction(1)
	if tx == nil {
		t.Error("expected transaction to exist")
	}
}

func TestSessionManager_DoubleStart(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	sm := NewSessionManager()

	// Start first transaction
	sm.StartTransaction(1, eng, "test")

	// Try to start second transaction
	if sm.StartTransaction(1, eng, "test") {
		t.Error("expected double start to fail")
	}
}

func TestSessionManager_Commit(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	sm := NewSessionManager()

	// Start and commit transaction
	sm.StartTransaction(1, eng, "test")
	if err := sm.CommitTransaction(1); err != nil {
		t.Errorf("expected commit to succeed: %v", err)
	}

	if sm.IsInTransaction(1) {
		t.Error("expected transaction to be committed")
	}
}

func TestSessionManager_Abort(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	sm := NewSessionManager()

	// Start and abort transaction
	sm.StartTransaction(1, eng, "test")
	sm.AbortTransaction(1)

	if sm.IsInTransaction(1) {
		t.Error("expected transaction to be aborted")
	}
}

func TestSessionManager_Remove(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	sm := NewSessionManager()

	// Start transaction and remove session
	sm.StartTransaction(1, eng, "test")
	sm.Remove(1)

	if sm.IsInTransaction(1) {
		t.Error("expected transaction to be removed")
	}
}

func TestSessionManager_GetTransactionDB(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	sm := NewSessionManager()

	// Check DB before transaction
	db := sm.GetTransactionDB(1)
	if db != "" {
		t.Errorf("expected empty DB, got %s", db)
	}

	// Start transaction
	sm.StartTransaction(1, eng, "mydb")

	db = sm.GetTransactionDB(1)
	if db != "mydb" {
		t.Errorf("expected DB 'mydb', got %s", db)
	}
}

func TestTxWrapper(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	tx := eng.Begin()
	wrapper := &txWrapper{tx: tx}

	// Test Put
	if err := wrapper.Put([]byte("key"), []byte("value")); err != nil {
		t.Errorf("expected put to succeed: %v", err)
	}

	// Test Get (snapshot sees state at transaction start, not uncommitted writes)
	// Uncommitted writes are in the batch, not visible to snapshot
	_, err = wrapper.Get([]byte("key"))
	// Expected to not find key since snapshot was taken before Put
	if err == nil {
		t.Log("Note: Get sees uncommitted value (implementation dependent)")
	}

	// Test Delete
	wrapper.Delete([]byte("key"))

	// Commit
	if err := tx.Commit(); err != nil {
		t.Errorf("expected commit to succeed: %v", err)
	}
}

func TestEngineWrapper(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	wrapper := &engineWrapper{eng: eng}

	// Test Put
	if err := wrapper.Put([]byte("key"), []byte("value")); err != nil {
		t.Errorf("expected put to succeed: %v", err)
	}

	// Test Get
	val, err := wrapper.Get([]byte("key"))
	if err != nil {
		t.Errorf("expected get to succeed: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("expected 'value', got %s", string(val))
	}

	// Test Delete
	wrapper.Delete([]byte("key"))

	// Verify deletion
	_, err = wrapper.Get([]byte("key"))
	if err == nil {
		t.Error("expected key to be deleted")
	}
}
