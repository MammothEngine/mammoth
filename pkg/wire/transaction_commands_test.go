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
