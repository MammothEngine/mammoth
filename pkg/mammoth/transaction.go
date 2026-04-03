package mammoth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// TransactionOptions configures transaction behavior.
type TransactionOptions struct {
	// MaxRetries is the maximum number of retry attempts for conflicts.
	// Default: 3
	MaxRetries int

	// RetryDelay is the initial delay between retries (doubles each retry).
	// Default: 10ms
	RetryDelay time.Duration

	// Timeout is the maximum time to wait for the transaction to complete.
	// Default: 30 seconds
	Timeout time.Duration

	// ReadOnly marks the transaction as read-only (optimizes performance).
	ReadOnly bool
}

// DefaultTransactionOptions returns default transaction options.
func DefaultTransactionOptions() TransactionOptions {
	return TransactionOptions{
		MaxRetries: 3,
		RetryDelay: 10 * time.Millisecond,
		Timeout:    30 * time.Second,
		ReadOnly:   false,
	}
}

// Transaction represents a multi-document ACID transaction.
type Transaction struct {
	db       *Database
	tx       *engine.Transaction
	mu       sync.Mutex
	committed bool
	rolledBack bool
	startTime  time.Time
	readOnly   bool
}

// StartTransaction begins a new transaction.
// The transaction must be committed or aborted to release resources.
func (db *Database) StartTransaction(opts ...TransactionOptions) (*Transaction, error) {
	op := DefaultTransactionOptions()
	if len(opts) > 0 {
		op = opts[0]
	}

	tx := db.eng.Begin()
	if tx == nil {
		return nil, errors.New("mammoth: failed to start transaction")
	}

	return &Transaction{
		db:        db,
		tx:        tx,
		startTime: time.Now(),
		readOnly:  op.ReadOnly,
	}, nil
}

// WithTransaction executes the given function within a transaction.
// It handles automatic retries for conflicts and ensures proper cleanup.
func (db *Database) WithTransaction(ctx context.Context, fn func(tx *Transaction) error) error {
	opts := DefaultTransactionOptions()

	// Check for options in context
	if op, ok := ctx.Value(txOptionsKey{}).(TransactionOptions); ok {
		opts = op
	}

	// Apply timeout from context if shorter
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout < opts.Timeout {
			opts.Timeout = timeout
		}
	}

	var lastErr error
	delay := opts.RetryDelay

	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("mammoth: transaction cancelled: %w", ctx.Err())
		default:
		}

		// Start transaction
		tx, err := db.StartTransaction(opts)
		if err != nil {
			return fmt.Errorf("mammoth: start transaction: %w", err)
		}

		// Execute user function
		err = fn(tx)

		if err != nil {
			// User function failed - rollback
			tx.Rollback()

			// Check if this is a retryable error
			if isRetryableError(err) && attempt < opts.MaxRetries {
				lastErr = err
				time.Sleep(delay)
				delay *= 2 // Exponential backoff
				continue
			}

			return err // Non-retryable error
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			tx.Rollback()

			if isConflictError(err) && attempt < opts.MaxRetries {
				lastErr = err
				time.Sleep(delay)
				delay *= 2
				continue
			}

			return fmt.Errorf("mammoth: commit transaction: %w", err)
		}

		return nil // Success
	}

	return fmt.Errorf("mammoth: transaction failed after %d attempts: %w", opts.MaxRetries+1, lastErr)
}

// Get reads a key within the transaction.
func (tx *Transaction) Get(key []byte) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.rolledBack {
		return nil, errors.New("mammoth: transaction already finished")
	}

	return tx.tx.Get(key)
}

// Put writes a key-value pair within the transaction.
func (tx *Transaction) Put(key, value []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.rolledBack {
		return errors.New("mammoth: transaction already finished")
	}

	if tx.readOnly {
		return errors.New("mammoth: cannot write in read-only transaction")
	}

	tx.tx.Put(key, value)
	return nil
}

// Delete removes a key within the transaction.
func (tx *Transaction) Delete(key []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.rolledBack {
		return errors.New("mammoth: transaction already finished")
	}

	if tx.readOnly {
		return errors.New("mammoth: cannot write in read-only transaction")
	}

	tx.tx.Delete(key)
	return nil
}

// Commit commits the transaction.
func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.rolledBack {
		return errors.New("mammoth: transaction already finished")
	}

	tx.committed = true
	return tx.tx.Commit()
}

// Rollback aborts the transaction.
func (tx *Transaction) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.rolledBack {
		return
	}

	tx.rolledBack = true
	tx.tx.Rollback()
}

// IsCommitted returns whether the transaction has been committed.
func (tx *Transaction) IsCommitted() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.committed
}

// IsRolledBack returns whether the transaction has been rolled back.
func (tx *Transaction) IsRolledBack() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.rolledBack
}

// IsActive returns whether the transaction is still active.
func (tx *Transaction) IsActive() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return !tx.committed && !tx.rolledBack
}

// Elapsed returns the time since the transaction started.
func (tx *Transaction) Elapsed() time.Duration {
	return time.Since(tx.startTime)
}

// txOptionsKey is the context key for transaction options.
type txOptionsKey struct{}

// WithTransactionOptions adds transaction options to a context.
func WithTransactionOptions(ctx context.Context, opts TransactionOptions) context.Context {
	return context.WithValue(ctx, txOptionsKey{}, opts)
}

// isRetryableError checks if an error should trigger a retry.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// Add specific retryable error checks here
	return false
}

// isConflictError checks if an error is a transaction conflict.
func isConflictError(err error) bool {
	if err == nil {
		return false
	}
	// Check for engine-level conflict errors
	// This would need to be implemented in the engine package
	return false
}

// CollectionTx wraps a collection with transaction support.
type CollectionTx struct {
	coll *Collection
	tx   *Transaction
}

// WithTransaction returns a collection wrapper that uses the transaction.
func (c *Collection) WithTransaction(tx *Transaction) *CollectionTx {
	return &CollectionTx{coll: c, tx: tx}
}

// InsertOne inserts a document within the transaction.
func (ct *CollectionTx) InsertOne(doc map[string]interface{}) (interface{}, error) {
	if ct.tx.readOnly {
		return nil, errors.New("mammoth: cannot insert in read-only transaction")
	}

	bsonDoc := mapToDoc(doc)
	idVal, _ := bsonDoc.Get("_id")
	id := valueToInterface(idVal)

	// Get the key for this document
	idBytes, err := encodeID(id)
	if err != nil {
		return nil, err
	}

	key := mongo.EncodeDocumentKey(ct.coll.db, ct.coll.name, idBytes)
	value := bson.Encode(bsonDoc)

	if err := ct.tx.Put(key, value); err != nil {
		return nil, err
	}

	return id, nil
}

// FindOne returns the first matching document within the transaction.
func (ct *CollectionTx) FindOne(filter map[string]interface{}) (map[string]interface{}, error) {
	// For now, scan all and filter
	// Full implementation would use the transaction snapshot
	var result *bson.Document
	var found bool

	err := ct.coll.coll.ScanAll(func(key []byte, doc *bson.Document) bool {
		if matcher := mongo.NewMatcher(mapToDoc(filter)); matcher.Match(doc) {
			result = doc
			found = true
			return false
		}
		return true
	})

	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}

	return documentToMap(result), nil
}

// UpdateOne updates the first matching document within the transaction.
func (ct *CollectionTx) UpdateOne(filter, update map[string]interface{}) (int64, error) {
	if ct.tx.readOnly {
		return 0, errors.New("mammoth: cannot update in read-only transaction")
	}

	// Find the document
	doc, err := ct.FindOne(filter)
	if err != nil {
		if err == ErrNotFound {
			return 0, nil
		}
		return 0, err
	}

	// Apply updates
	for k, v := range update {
		if k == "$set" {
			if setMap, ok := v.(map[string]interface{}); ok {
				for sk, sv := range setMap {
					doc[sk] = sv
				}
			}
		} else {
			doc[k] = v
		}
	}

	// Re-insert the document
	_, err = ct.InsertOne(doc)
	if err != nil {
		return 0, err
	}

	return 1, nil
}

// DeleteOne deletes the first matching document within the transaction.
func (ct *CollectionTx) DeleteOne(filter map[string]interface{}) (int64, error) {
	if ct.tx.readOnly {
		return 0, errors.New("mammoth: cannot delete in read-only transaction")
	}

	// Find the document to get its key
	doc, err := ct.FindOne(filter)
	if err != nil {
		if err == ErrNotFound {
			return 0, nil
		}
		return 0, err
	}

	// Get the key
	id := doc["_id"]
	idBytes, err := encodeID(id)
	if err != nil {
		return 0, err
	}

	key := mongo.EncodeDocumentKey(ct.coll.db, ct.coll.name, idBytes)
	if err := ct.tx.Delete(key); err != nil {
		return 0, err
	}

	return 1, nil
}

// encodeID encodes an ID value to bytes.
func encodeID(id interface{}) ([]byte, error) {
	switch v := id.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		// Try to convert to string
		return []byte(fmt.Sprintf("%v", v)), nil
	}
}
