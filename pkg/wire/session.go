package wire

import (
	"sync"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// SessionState tracks per-connection session state including transactions.
type SessionState struct {
	ConnID      uint64
	SessionID   bson.ObjectID
	Transaction *engine.Transaction
	InTx        bool
	TxDB        string // database context for the transaction
}

// SessionManager manages per-connection session state.
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[uint64]*SessionState
}

// NewSessionManager creates a new session manager.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[uint64]*SessionState),
	}
}

// GetOrCreate returns the session state for a connection.
func (sm *SessionManager) GetOrCreate(connID uint64) *SessionState {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if s, ok := sm.sessions[connID]; ok {
		return s
	}
	s := &SessionState{
		ConnID:    connID,
		SessionID: bson.NewObjectID(),
	}
	sm.sessions[connID] = s
	return s
}

// Get returns the session state for a connection.
func (sm *SessionManager) Get(connID uint64) *SessionState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sessions[connID]
}

// Remove cleans up when a connection closes.
func (sm *SessionManager) Remove(connID uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	s := sm.sessions[connID]
	if s != nil && s.Transaction != nil {
		s.Transaction.Rollback()
	}
	delete(sm.sessions, connID)
}

// StartTransaction begins a new transaction for the session.
func (sm *SessionManager) StartTransaction(connID uint64, eng *engine.Engine, db string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	s := sm.sessions[connID]
	if s == nil {
		s = &SessionState{
			ConnID:    connID,
			SessionID: bson.NewObjectID(),
		}
		sm.sessions[connID] = s
	}
	if s.InTx {
		return false // already in transaction
	}
	s.Transaction = eng.Begin()
	s.InTx = true
	s.TxDB = db
	return true
}

// CommitTransaction commits the current transaction.
func (sm *SessionManager) CommitTransaction(connID uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	s := sm.sessions[connID]
	if s == nil || !s.InTx || s.Transaction == nil {
		return nil // nothing to commit
	}
	err := s.Transaction.Commit()
	s.InTx = false
	s.TxDB = ""
	s.Transaction = nil
	return err
}

// AbortTransaction rolls back the current transaction.
func (sm *SessionManager) AbortTransaction(connID uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	s := sm.sessions[connID]
	if s == nil || !s.InTx || s.Transaction == nil {
		return
	}
	s.Transaction.Rollback()
	s.InTx = false
	s.TxDB = ""
	s.Transaction = nil
}

// IsInTransaction returns whether the connection has an active transaction.
func (sm *SessionManager) IsInTransaction(connID uint64) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s := sm.sessions[connID]
	return s != nil && s.InTx
}

// GetTransaction returns the active transaction for the connection.
func (sm *SessionManager) GetTransaction(connID uint64) *engine.Transaction {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s := sm.sessions[connID]
	if s == nil || !s.InTx {
		return nil
	}
	return s.Transaction
}

// GetTransactionDB returns the database context for the active transaction.
func (sm *SessionManager) GetTransactionDB(connID uint64) string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s := sm.sessions[connID]
	if s == nil || !s.InTx {
		return ""
	}
	return s.TxDB
}
