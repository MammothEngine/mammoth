package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mammothengine/mammoth/pkg/engine/compression"
	"github.com/mammothengine/mammoth/pkg/engine/wal"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return dir
}

func TestEngineOpenClose(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestEnginePutGet(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	key := []byte("hello")
	val := []byte("world")

	if err := e.Put(key, val); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := e.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(val) {
		t.Errorf("Get = %q, want %q", got, val)
	}
}

func TestEngineDelete(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	key := []byte("k1")
	if err := e.Put(key, []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := e.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err = e.Get(key)
	if err != errKeyNotFound {
		t.Errorf("Get after delete = %v, want errKeyNotFound", err)
	}
}

func TestEngineGetNotFound(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	_, err = e.Get([]byte("nonexistent"))
	if err != errKeyNotFound {
		t.Errorf("Get nonexistent = %v, want errKeyNotFound", err)
	}
}

func TestEngineBatch(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	b := e.NewBatch()
	b.Put([]byte("a"), []byte("1"))
	b.Put([]byte("b"), []byte("2"))
	b.Delete([]byte("c"))

	if b.Len() != 3 {
		t.Errorf("Len = %d, want 3", b.Len())
	}
	if err := b.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	got, err := e.Get([]byte("a"))
	if err != nil || string(got) != "1" {
		t.Errorf("Get(a) = %q, %v; want \"1\", nil", got, err)
	}
	got, err = e.Get([]byte("b"))
	if err != nil || string(got) != "2" {
		t.Errorf("Get(b) = %q, %v; want \"2\", nil", got, err)
	}

	// Double commit should fail
	if err := b.Commit(); err != errBatchAlreadyCommitted {
		t.Errorf("second Commit = %v, want errBatchAlreadyCommitted", err)
	}
}

func TestEngineBatchPutAfterDelete(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	// Put then delete via batch
	e.Put([]byte("x"), []byte("old"))
	b := e.NewBatch()
	b.Delete([]byte("x"))
	if err := b.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	_, err = e.Get([]byte("x"))
	if err != errKeyNotFound {
		t.Errorf("Get(x) after batch delete = %v, want errKeyNotFound", err)
	}
}

func TestEngineSnapshot(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	e.Put([]byte("k1"), []byte("v1"))
	snap := e.NewSnapshot()
	defer snap.Release()

	// Snapshot should be able to read keys present at snapshot time
	got, err := snap.Get([]byte("k1"))
	if err != nil || string(got) != "v1" {
		t.Errorf("Snapshot.Get(k1) = %q, %v; want \"v1\", nil", got, err)
	}

	// Key not in snapshot
	_, err = snap.Get([]byte("nonexistent"))
	if err != errKeyNotFound {
		t.Errorf("Snapshot.Get(nonexistent) = %v, want errKeyNotFound", err)
	}
}

func TestEngineFlush(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 // small to trigger flush easily
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		val := make([]byte, 20)
		e.Put(key, val)
	}

	if err := e.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Verify data is still accessible
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		if _, err := e.Get(key); err != nil {
			t.Errorf("Get(%d) after flush: %v", i, err)
		}
	}
}

func TestEngineRecovery(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)
	opts.MemtableSize = 4 * 1024 * 1024

	// Write data
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key:%06d", i))
		val := []byte(fmt.Sprintf("val:%06d", i))
		if err := e.Put(key, val); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify
	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer e2.Close()

	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key:%06d", i))
		want := fmt.Sprintf("val:%06d", i)
		got, err := e2.Get(key)
		if err != nil {
			t.Errorf("Get(%s) after recovery: %v", key, err)
			continue
		}
		if string(got) != want {
			t.Errorf("Get(%s) = %q, want %q", key, got, want)
		}
	}
}

func TestEngineRecoveryWithFlush(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	e.Put([]byte("persisted"), []byte("yes"))
	e.Flush()
	e.Put([]byte("in_mem"), []byte("maybe"))
	e.Close()

	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer e2.Close()

	got, err := e2.Get([]byte("persisted"))
	if err != nil || string(got) != "yes" {
		t.Errorf("Get(persisted) = %q, %v; want \"yes\"", got, err)
	}
	got, err = e2.Get([]byte("in_mem"))
	if err != nil || string(got) != "maybe" {
		t.Errorf("Get(in_mem) = %q, %v; want \"maybe\"", got, err)
	}
}

func TestEngineLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	dir := tempDir(t)
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256 * 1024 // trigger frequent flushes
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	n := 10000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key:%06d", i))
		val := []byte(fmt.Sprintf("value:%06d", i))
		if err := e.Put(key, val); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key:%06d", i))
		want := fmt.Sprintf("value:%06d", i)
		got, err := e.Get(key)
		if err != nil {
			t.Errorf("Get(%s): %v", key, err)
			continue
		}
		if string(got) != want {
			t.Errorf("Get(%s) = %q, want %q", key, got, want)
		}
	}
}

func TestEngineConcurrentWrites(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 * 1024
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	var wg sync.WaitGroup
	n := 100
	goroutines := 8

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := []byte(fmt.Sprintf("g%d:k%d", gid, i))
				val := []byte(fmt.Sprintf("v%d:%d", gid, i))
				if err := e.Put(key, val); err != nil {
					t.Errorf("Put(g%d,i%d): %v", gid, i, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	// Verify all keys
	for g := 0; g < goroutines; g++ {
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("g%d:k%d", g, i))
			want := fmt.Sprintf("v%d:%d", g, i)
			got, err := e.Get(key)
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if string(got) != want {
				t.Errorf("Get(%s) = %q, want %q", key, got, want)
			}
		}
	}
}

func TestEngineClosedOperations(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	e.Close()

	if err := e.Put([]byte("k"), []byte("v")); err != errEngineClosed {
		t.Errorf("Put after close = %v, want errEngineClosed", err)
	}
	if _, err := e.Get([]byte("k")); err != errEngineClosed {
		t.Errorf("Get after close = %v, want errEngineClosed", err)
	}
	if err := e.Delete([]byte("k")); err != errEngineClosed {
		t.Errorf("Delete after close = %v, want errEngineClosed", err)
	}
}

func TestEngineOverwrite(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	key := []byte("key")
	e.Put(key, []byte("v1"))
	e.Put(key, []byte("v2"))
	e.Put(key, []byte("v3"))

	got, err := e.Get(key)
	if err != nil || string(got) != "v3" {
		t.Errorf("Get = %q, %v; want \"v3\"", got, err)
	}
}

func TestEngineEmptyValue(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	e.Put([]byte("k"), []byte{})
	got, err := e.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Get = %q, want empty", got)
	}
}

func TestEngineCompressionSnappy(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)
	opts.Compression = compression.CompressionSnappy
	opts.MemtableSize = 512

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key:%04d", i))
		val := make([]byte, 50)
		for j := range val {
			val[j] = byte(i % 256)
		}
		e.Put(key, val)
	}

	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key:%04d", i))
		got, err := e.Get(key)
		if err != nil {
			t.Errorf("Get(%s): %v", key, err)
			continue
		}
		if len(got) != 50 || got[0] != byte(i%256) {
			t.Errorf("Get(%s) wrong value", key)
		}
	}
}

func TestEngineWALSyncNone(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)
	opts.WALSyncMode = wal.SyncNone

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	e.Put([]byte("k"), []byte("v"))
	e.Close()

	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer e2.Close()
	got, err := e2.Get([]byte("k"))
	if err != nil || string(got) != "v" {
		t.Errorf("Get = %q, %v; want \"v\"", got, err)
	}
}

func TestEngineSSTablesCreated(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		val := make([]byte, 32)
		e.Put(key, val)
	}
	e.Close()

	// Check .sst files exist
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	sstCount := 0
	for _, ent := range entries {
		if filepath.Ext(ent.Name()) == ".sst" {
			sstCount++
		}
	}
	if sstCount == 0 {
		t.Error("expected .sst files to be created")
	}
	t.Logf("Created %d SSTable files", sstCount)
}
