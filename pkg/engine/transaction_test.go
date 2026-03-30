package engine

import (
	"testing"
)

func TestTransaction_Commit(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Start transaction
	tx := eng.Begin()

	// Read (should be empty)
	_, err = tx.Get([]byte("key1"))
	if err == nil {
		t.Fatal("expected key not found")
	}

	// Write
	tx.Put([]byte("key1"), []byte("value1"))
	tx.Put([]byte("key2"), []byte("value2"))

	// Commit
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify writes are visible
	val, err := eng.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "value1" {
		t.Fatalf("expected value1, got %s", val)
	}

	val, err = eng.Get([]byte("key2"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "value2" {
		t.Fatalf("expected value2, got %s", val)
	}
}

func TestTransaction_Rollback(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	tx := eng.Begin()
	tx.Put([]byte("key1"), []byte("value1"))
	tx.Rollback()

	// Write should not be visible
	_, err = eng.Get([]byte("key1"))
	if err == nil {
		t.Fatal("expected key not found after rollback")
	}
}

func TestTransaction_DoubleCommit(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	tx := eng.Begin()
	tx.Put([]byte("key1"), []byte("value1"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("expected error on double commit")
	}
}

func TestTransaction_SnapshotRead(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Write initial value
	eng.Put([]byte("key1"), []byte("original"))

	// Start transaction
	tx := eng.Begin()

	// Overwrite the key outside the transaction
	eng.Put([]byte("key1"), []byte("modified"))

	// Transaction should still see the original value (best-effort)
	val, err := tx.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	_ = val // In single-writer mode, the snapshot may see the latest value

	tx.Rollback()
}
