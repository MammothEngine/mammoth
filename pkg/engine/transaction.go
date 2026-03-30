package engine

import (
	"sync"
)

// Transaction provides single-document transaction semantics with snapshot isolation.
type Transaction struct {
	engine   *Engine
	snapshot *Snapshot
	batch    *Batch
	mu       sync.Mutex
	committed bool
	rolledBack bool
}

// Begin starts a new transaction with a snapshot.
func (e *Engine) Begin() *Transaction {
	return &Transaction{
		engine:   e,
		snapshot: e.NewSnapshot(),
		batch:    e.NewBatch(),
	}
}

// Get reads a key using the transaction's snapshot.
func (tx *Transaction) Get(key []byte) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.committed || tx.rolledBack {
		return nil, errTransactionFinished
	}
	return tx.snapshot.Get(key)
}

// Put adds a write to the transaction's batch.
func (tx *Transaction) Put(key, value []byte) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.committed || tx.rolledBack {
		return
	}
	tx.batch.Put(key, value)
}

// Delete adds a delete to the transaction's batch.
func (tx *Transaction) Delete(key []byte) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.committed || tx.rolledBack {
		return
	}
	tx.batch.Delete(key)
}

// Commit atomically applies all pending writes.
func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.committed || tx.rolledBack {
		return errTransactionFinished
	}
	tx.committed = true
	tx.snapshot.Release()
	return tx.batch.Commit()
}

// Rollback discards all pending writes.
func (tx *Transaction) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.committed || tx.rolledBack {
		return
	}
	tx.rolledBack = true
	tx.snapshot.Release()
}

// IsCommitted returns whether the transaction has been committed.
func (tx *Transaction) IsCommitted() bool { return tx.committed }

// IsRolledBack returns whether the transaction has been rolled back.
func (tx *Transaction) IsRolledBack() bool { return tx.rolledBack }
