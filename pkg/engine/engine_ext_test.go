package engine

import (
	"fmt"
	"testing"
)

func TestTransaction_Delete(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	// Begin a transaction
	tx := eng.Begin()
	if tx == nil {
		t.Fatal("expected non-nil transaction")
	}
	defer tx.Rollback()

	// Put some data
	tx.Put([]byte("key1"), []byte("value1"))
	tx.Put([]byte("key2"), []byte("value2"))

	// Delete one key
	tx.Delete([]byte("key1"))

	// Commit
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify key1 is deleted
	_, err = eng.Get([]byte("key1"))
	if err == nil {
		t.Error("expected key1 to be deleted")
	}

	// Verify key2 still exists
	val2, err := eng.Get([]byte("key2"))
	if err != nil {
		t.Errorf("expected key2 to exist: %v", err)
	}
	if string(val2) != "value2" {
		t.Errorf("expected value2, got %s", string(val2))
	}
}

func TestTransaction_Delete_AlreadyCommitted(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	tx := eng.Begin()
	tx.Put([]byte("key1"), []byte("value1"))
	tx.Commit()

	// Delete after commit should be a no-op
	tx.Delete([]byte("key2")) // should not panic

	// Verify committed data still exists
	val, err := eng.Get([]byte("key1"))
	if err != nil {
		t.Errorf("expected key1 to exist: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}
}

func TestTransaction_IsCommitted(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	tx := eng.Begin()

	// Initially not committed
	if tx.IsCommitted() {
		t.Error("expected IsCommitted() = false initially")
	}

	// After commit
	tx.Commit()
	if !tx.IsCommitted() {
		t.Error("expected IsCommitted() = true after Commit()")
	}
}

func TestTransaction_IsRolledBack(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	tx := eng.Begin()

	// Initially not rolled back
	if tx.IsRolledBack() {
		t.Error("expected IsRolledBack() = false initially")
	}

	// After rollback
	tx.Rollback()
	if !tx.IsRolledBack() {
		t.Error("expected IsRolledBack() = true after Rollback()")
	}
}

func TestSnapshot_SeqNum(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	// Get a snapshot
	snap := eng.NewSnapshot()
	if snap == nil {
		t.Fatal("expected non-nil snapshot")
	}
	defer snap.Release()

	// SeqNum should be returned (starts at 0)
	seqNum := snap.SeqNum()
	// SeqNum is a valid uint64, just verify it doesn't panic
	_ = seqNum
}

func TestEngine_Stats(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	// Get initial stats
	stats := eng.Stats()

	// Should have valid stats (SequenceNumber starts at 0)
	// Just verify the struct is populated
	_ = stats.SequenceNumber

	// Add some data
	eng.Put([]byte("key1"), []byte("value1"))
	eng.Put([]byte("key2"), []byte("value2"))
	eng.Get([]byte("key1")) // increment get count
	eng.Delete([]byte("key1"))

	// Get updated stats
	stats2 := eng.Stats()

	// Check that operations are counted
	if stats2.PutCount < 2 {
		t.Errorf("expected PutCount >= 2, got %d", stats2.PutCount)
	}
	if stats2.GetCount < 1 {
		t.Errorf("expected GetCount >= 1, got %d", stats2.GetCount)
	}
	if stats2.DeleteCount < 1 {
		t.Errorf("expected DeleteCount >= 1, got %d", stats2.DeleteCount)
	}
}

func TestSnapshot_Scan(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	// Put some data
	eng.Put([]byte("aaa"), []byte("1"))
	eng.Put([]byte("aab"), []byte("2"))
	eng.Put([]byte("abb"), []byte("3"))
	eng.Put([]byte("bbb"), []byte("4"))

	// Get a snapshot
	snap := eng.NewSnapshot()
	if snap == nil {
		t.Fatal("expected non-nil snapshot")
	}
	defer snap.Release()

	// Scan with prefix "aa"
	var count int
	err = snap.Scan([]byte("aa"), func(key, value []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Errorf("Scan error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 results for prefix 'aa', got %d", count)
	}

	// Scan with prefix "a"
	count = 0
	err = snap.Scan([]byte("a"), func(key, value []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Errorf("Scan error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 results for prefix 'a', got %d", count)
	}
}

func TestSnapshot_Scan_Released(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	// Get and immediately release a snapshot
	snap := eng.NewSnapshot()
	snap.Release()

	// Scan on released snapshot should return error
	err = snap.Scan([]byte("test"), func(key, value []byte) bool {
		return true
	})
	if err == nil {
		t.Error("expected error when scanning released snapshot")
	}
}

// Test MaybeCompact public API
func TestEngine_MaybeCompact(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	// Add some data to create memtable entries
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		val := []byte(fmt.Sprintf("value%04d", i))
		if err := eng.Put(key, val); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Trigger compaction - should not error
	if err := eng.MaybeCompact(); err != nil {
		t.Errorf("MaybeCompact: %v", err)
	}
}
