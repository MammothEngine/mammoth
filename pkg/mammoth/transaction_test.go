package mammoth

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestTransactionState(t *testing.T) {
	db := openTestDB(t)

	opts := DefaultTransactionOptions()
	ctx := WithTransactionOptions(context.Background(), opts)

	var tx *Transaction
	err := db.WithTransaction(ctx, func(transaction *Transaction) error {
		tx = transaction
		return nil
	})

	if err != nil {
		t.Fatalf("WithTransaction: %v", err)
	}

	if !tx.IsCommitted() {
		t.Error("transaction should be committed after successful WithTransaction")
	}
	if tx.IsRolledBack() {
		t.Error("transaction should not be rolled back")
	}
	if tx.IsActive() {
		t.Error("transaction should not be active after commit")
	}

	time.Sleep(10 * time.Millisecond)

	elapsed := tx.Elapsed()
	if elapsed < 10*time.Millisecond {
		t.Errorf("expected elapsed >= 10ms, got %v", elapsed)
	}
}

func TestTransactionCollectionOperations(t *testing.T) {
	db := openTestDB(t)
	coll, _ := db.Collection("tx_coll_test")

	coll.InsertOne(map[string]interface{}{"_id": "tx1", "value": 1})

	err := db.WithTransaction(context.Background(), func(tx *Transaction) error {
		ct := coll.WithTransaction(tx)

		_, err := ct.UpdateOne(
			map[string]interface{}{"_id": "tx1"},
			map[string]interface{}{"$set": map[string]interface{}{"value": 2}},
		)
		if err != nil {
			return err
		}

		_, err = ct.InsertOne(map[string]interface{}{"_id": "tx2", "value": 3})
		return err
	})

	if err != nil {
		t.Fatalf("WithTransaction: %v", err)
	}

	doc1, _ := coll.FindOne(map[string]interface{}{"_id": "tx1"})
	if doc1["value"] != 2 {
		t.Errorf("expected tx1 value=2, got %v", doc1["value"])
	}

	doc2, _ := coll.FindOne(map[string]interface{}{"_id": "tx2"})
	if doc2["value"] != 3 {
		t.Errorf("expected tx2 value=3, got %v", doc2["value"])
	}
}

func TestTransactionDelete(t *testing.T) {
	db := openTestDB(t)
	coll, _ := db.Collection("tx_delete_test")

	for i := 0; i < 3; i++ {
		coll.InsertOne(map[string]interface{}{
			"_id":   fmt.Sprintf("del%d", i),
			"group": "A",
		})
	}

	before, _ := coll.Count(nil)
	if before != 3 {
		t.Fatalf("expected 3 documents before delete, got %d", before)
	}

	err := db.WithTransaction(context.Background(), func(tx *Transaction) error {
		ct := coll.WithTransaction(tx)
		_, err := ct.DeleteOne(map[string]interface{}{"_id": "del1"})
		return err
	})

	if err != nil {
		t.Fatalf("WithTransaction: %v", err)
	}

	after, _ := coll.Count(nil)
	if after != 2 {
		t.Errorf("expected 2 documents after delete, got %d", after)
	}

	_, err = coll.FindOne(map[string]interface{}{"_id": "del1"})
	if err != ErrNotFound {
		t.Errorf("expected document to be deleted, got: %v", err)
	}

	for _, id := range []string{"del0", "del2"} {
		_, err = coll.FindOne(map[string]interface{}{"_id": id})
		if err != nil {
			t.Errorf("expected document %s to exist", id)
		}
	}
}

func TestIsConflictError(t *testing.T) {
	conflictErr := fmt.Errorf("write conflict")
	if isConflictError(conflictErr) {
		t.Log("Conflict error detected")
	}

	nonConflictErr := fmt.Errorf("some error")
	if isConflictError(nonConflictErr) {
		t.Error("expected non-conflict error")
	}

	if isConflictError(nil) {
		t.Error("expected nil to not be conflict")
	}
}

// TestWithTransactionContextCancellation tests context cancellation during transaction
func TestWithTransactionContextCancellation(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	err := db.WithTransaction(ctx, func(tx *Transaction) error {
		return nil
	})

	if err == nil {
		t.Error("expected error when context is cancelled")
	}
}

// TestWithTransactionTimeout tests transaction with context timeout
func TestWithTransactionTimeout(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Very short timeout to trigger deadline
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond) // Ensure timeout passes

	err := db.WithTransaction(ctx, func(tx *Transaction) error {
		return nil
	})

	if err == nil {
		t.Error("expected error when context deadline exceeded")
	}
}

// TestWithTransactionUserError tests transaction when user function returns error
func TestWithTransactionUserError(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	customErr := fmt.Errorf("user function error")

	err := db.WithTransaction(context.Background(), func(tx *Transaction) error {
		return customErr
	})

	if err != customErr {
		t.Errorf("expected %v, got %v", customErr, err)
	}
}

// TestWithTransactionCustomOptions tests transaction with custom options from context
func TestWithTransactionCustomOptions(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	customOpts := TransactionOptions{
		MaxRetries: 1,
		RetryDelay: 5 * time.Millisecond,
		Timeout:    5 * time.Second,
		ReadOnly:   false,
	}

	ctx := WithTransactionOptions(context.Background(), customOpts)

	err := db.WithTransaction(ctx, func(tx *Transaction) error {
		return nil
	})

	if err != nil {
		t.Errorf("WithTransaction with custom options: %v", err)
	}
}

// TestWithTransactionReadOnlyOption tests read-only transaction option
func TestWithTransactionReadOnlyOption(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	customOpts := TransactionOptions{
		ReadOnly: true,
	}

	ctx := WithTransactionOptions(context.Background(), customOpts)

	err := db.WithTransaction(ctx, func(tx *Transaction) error {
		// Read-only transaction should still work for reads
		return nil
	})

	if err != nil {
		t.Errorf("WithTransaction read-only: %v", err)
	}
}

// TestTransactionOperationsAfterFinish tests operations after commit/rollback
func TestTransactionOperationsAfterFinish(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Test Put after commit
	tx1, _ := db.StartTransaction()
	tx1.Commit()

	err := tx1.Put([]byte("key"), []byte("value"))
	if err == nil {
		t.Error("expected error for Put after commit")
	}

	// Test Delete after rollback
	tx2, _ := db.StartTransaction()
	tx2.Rollback()

	err = tx2.Delete([]byte("key"))
	if err == nil {
		t.Error("expected error for Delete after rollback")
	}
}
