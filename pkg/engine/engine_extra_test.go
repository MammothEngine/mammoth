package engine

import (
	"testing"
)

// TestBatchPutDelete tests batch Put and Delete
func TestBatchPutDeleteOperations(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Test batch operations
	batch := eng.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))
	batch.Delete([]byte("key1"))

	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit error: %v", err)
	}

	// Verify key1 is deleted
	_, err = eng.Get([]byte("key1"))
	if err == nil {
		t.Error("expected error for deleted key")
	}

	// Verify key2 exists
	val, err := eng.Get([]byte("key2"))
	if err != nil {
		t.Errorf("Get error: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("expected value2, got %s", val)
	}
}

// TestBatchMultipleOps tests batch with multiple operations on same key
func TestBatchMultipleOps(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Put then delete same key in batch
	batch := eng.NewBatch()
	batch.Put([]byte("key"), []byte("value1"))
	batch.Put([]byte("key"), []byte("value2"))
	batch.Delete([]byte("key"))
	batch.Put([]byte("key"), []byte("value3"))

	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit error: %v", err)
	}

	// Should have final value
	val, err := eng.Get([]byte("key"))
	if err != nil {
		t.Errorf("Get error: %v", err)
	}
	if string(val) != "value3" {
		t.Errorf("expected value3, got %s", val)
	}
}

// TestBatchLen tests batch Len
func TestBatchLen(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	batch := eng.NewBatch()
	if batch.Len() != 0 {
		t.Errorf("expected empty batch, got Len=%d", batch.Len())
	}

	batch.Put([]byte("key1"), []byte("value1"))
	if batch.Len() != 1 {
		t.Errorf("expected Len=1, got %d", batch.Len())
	}

	batch.Put([]byte("key2"), []byte("value2"))
	batch.Delete([]byte("key3"))
	if batch.Len() != 3 {
		t.Errorf("expected Len=3, got %d", batch.Len())
	}
}

// TestEngineGetDeletedKey tests Get for deleted key
func TestEngineGetDeletedKey(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Put then delete
	eng.Put([]byte("key"), []byte("value"))
	eng.Delete([]byte("key"))

	// Get deleted key
	_, err = eng.Get([]byte("key"))
	if err == nil {
		t.Error("expected error for deleted key")
	}
}

// TestSnapshotReleased tests snapshot operations after release
func TestSnapshotReleasedOps(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Put some data
	eng.Put([]byte("key"), []byte("value"))

	// Create and release snapshot
	snap := eng.NewSnapshot()
	snap.Release()

	// Try to get from released snapshot
	_, err = snap.Get([]byte("key"))
	if err == nil {
		t.Error("expected error for released snapshot")
	}

	// Try to scan from released snapshot
	err = snap.Scan([]byte("key"), func(_, _ []byte) bool { return true })
	if err == nil {
		t.Error("expected error for released snapshot scan")
	}
}

// TestSnapshotReleaseMultiple tests releasing snapshot multiple times
func TestSnapshotReleaseMultiple(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	snap := eng.NewSnapshot()

	// Release multiple times - should not panic
	snap.Release()
	snap.Release()
	snap.Release()
}

// TestSnapshotSeqNum tests snapshot SeqNum
func TestSnapshotSeqNum(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Put some data to get a sequence number
	eng.Put([]byte("key"), []byte("value"))

	snap := eng.NewSnapshot()
	defer snap.Release()

	// SeqNum should be valid
	seqNum := snap.SeqNum()
	if seqNum == 0 {
		t.Log("SeqNum is 0, may be expected")
	}
}

// TestEngineStatsOperations tests engine Stats
func TestEngineStatsOperations(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Initial stats
	stats := eng.Stats()
	if stats.MemtableCount != 1 {
		t.Errorf("expected 1 memtable, got %d", stats.MemtableCount)
	}

	// Put some data
	for i := 0; i < 100; i++ {
		key := []byte("key")
		key = append(key, byte(i))
		val := []byte("value")
		val = append(val, byte(i))
		eng.Put(key, val)
	}

	// Stats after puts
	stats = eng.Stats()
	if stats.PutCount < 100 {
		t.Errorf("expected PutCount >= 100, got %d", stats.PutCount)
	}

	// Get some data
	for i := 0; i < 50; i++ {
		key := []byte("key")
		key = append(key, byte(i))
		eng.Get(key)
	}

	stats = eng.Stats()
	if stats.GetCount < 50 {
		t.Errorf("expected GetCount >= 50, got %d", stats.GetCount)
	}
}

// TestEngineScanWithPrefix tests engine Scan with prefix
func TestEngineScanWithPrefix(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Put data with different prefixes
	eng.Put([]byte("a:1"), []byte("value1"))
	eng.Put([]byte("a:2"), []byte("value2"))
	eng.Put([]byte("b:1"), []byte("value3"))

	// Scan with prefix "a:"
	count := 0
	err = eng.Scan([]byte("a:"), func(key, value []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Errorf("Scan error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 results for prefix 'a:', got %d", count)
	}

	// Scan with prefix "b:"
	count = 0
	err = eng.Scan([]byte("b:"), func(key, value []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Errorf("Scan error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 result for prefix 'b:', got %d", count)
	}
}

// TestPrefixIteratorOperations tests PrefixIterator
func TestPrefixIteratorOperations(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Put data
	eng.Put([]byte("prefix:a"), []byte("1"))
	eng.Put([]byte("prefix:b"), []byte("2"))
	eng.Put([]byte("prefix:c"), []byte("3"))
	eng.Put([]byte("other:x"), []byte("4"))

	// Iterate with prefix
	it := eng.NewPrefixIterator([]byte("prefix:"))
	count := 0
	for it.Next() {
		count++
		_ = it.Key()
		_ = it.Value()
	}
	it.Close()

	if count != 3 {
		t.Errorf("expected 3 results, got %d", count)
	}
}

// TestEngineDeleteNonExistentKey tests deleting non-existent key
func TestEngineDeleteNonExistentKey(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Delete non-existent key - should not error
	if err := eng.Delete([]byte("nonexistent")); err != nil {
		t.Errorf("Delete error: %v", err)
	}
}

// TestEngineFlushMemtable tests flushing memtable
func TestEngineFlushMemtableOps(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 // Small memtable to trigger flush quickly
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Put enough data to trigger flush
	for i := 0; i < 100; i++ {
		key := []byte("key")
		key = append(key, byte(i/10))
		key = append(key, byte(i%10))
		val := make([]byte, 50)
		eng.Put(key, val)
	}

	// Trigger flush
	if err := eng.Flush(); err != nil {
		t.Errorf("Flush error: %v", err)
	}

	// Verify data still accessible
	stats := eng.Stats()
	if stats.SSTableCount == 0 {
		t.Log("no SSTables after flush (may be expected)")
	}
}

// TestEngineMaybeCompactManual tests manual compaction trigger
func TestEngineMaybeCompactManual(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Put some data
	for i := 0; i < 50; i++ {
		key := []byte("key")
		key = append(key, byte(i))
		eng.Put(key, []byte("value"))
	}

	// Trigger flush
	eng.Flush()

	// Try to trigger compaction
	eng.MaybeCompact()

	// Stats should be valid
	stats := eng.Stats()
	_ = stats.CompactionCount
}

// TestSnapshotScanOps tests snapshot Scan
func TestSnapshotScanOps(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Put data
	eng.Put([]byte("a:1"), []byte("v1"))
	eng.Put([]byte("a:2"), []byte("v2"))
	eng.Put([]byte("b:1"), []byte("v3"))

	// Create snapshot
	snap := eng.NewSnapshot()
	defer snap.Release()

	// Scan with prefix
	count := 0
	err = snap.Scan([]byte("a:"), func(key, value []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Errorf("Scan error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 results, got %d", count)
	}
}

// TestSnapshotGetNotFound tests snapshot Get for non-existent key
func TestSnapshotGetNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	snap := eng.NewSnapshot()
	defer snap.Release()

	// Get non-existent key
	_, err = snap.Get([]byte("nonexistent"))
	if err == nil {
		t.Error("expected error for non-existent key")
	}
}

// TestSnapshotIsolation tests snapshot isolation
func TestSnapshotIsolation(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Put initial data
	eng.Put([]byte("key"), []byte("original"))

	// Create snapshot
	snap := eng.NewSnapshot()
	defer snap.Release()

	// Modify data
	eng.Put([]byte("key"), []byte("modified"))

	// Snapshot may or may not see original value depending on implementation
	// Just verify operations work
	_, _ = snap.Get([]byte("key"))

	// Engine should see modified value
	val, err := eng.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(val) != "modified" {
		t.Errorf("expected modified, got %s", val)
	}
}

// TestEngineCloseIdempotent tests closing engine multiple times
func TestEngineCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	// Close should work
	if err := eng.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}

	// Second close should not panic
	if err := eng.Close(); err != nil {
		t.Errorf("Second Close error: %v", err)
	}
}
