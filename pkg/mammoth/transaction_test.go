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
