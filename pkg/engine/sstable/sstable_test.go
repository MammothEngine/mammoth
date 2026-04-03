package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mammothengine/mammoth/pkg/engine/compression"
)

func writeTestSSTable(t *testing.T, dir string, n int, compress compression.CompressionType) string {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("test_%d.sst", n))

	w, err := NewWriter(WriterOptions{
		Path:        path,
		BlockSize:   256,
		Compression: compress,
		ExpectedKeys: n,
	})
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key_%08d", i))
		value := []byte(fmt.Sprintf("value_%08d", i))
		if err := w.Add(key, value); err != nil {
			t.Fatalf("add: %v", err)
		}
	}

	if _, err := w.Finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}
	return path
}

func TestSSTableWriteReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := writeTestSSTable(t, dir, 100, compression.CompressionNone)

	r, err := NewReader(path, ReaderOptions{})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer r.Close()

	// Point lookups
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%08d", i))
		val, err := r.Get(key)
		if err != nil {
			t.Fatalf("get key_%08d: %v", i, err)
		}
		expected := fmt.Sprintf("value_%08d", i)
		if string(val) != expected {
			t.Fatalf("value mismatch for key_%08d: got %s", i, val)
		}
	}
}

func TestSSTableBloomFilter(t *testing.T) {
	dir := t.TempDir()
	path := writeTestSSTable(t, dir, 100, compression.CompressionNone)

	r, err := NewReader(path, ReaderOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	// Existing keys should be found
	key := []byte("key_00000042")
	if !r.MayContain(key) {
		t.Fatal("bloom should contain existing key")
	}

	// Non-existing key
	fakeKey := []byte("zzz_nonexistent_key")
	if r.MayContain(fakeKey) {
		// Bloom can have false positives, but this is unlikely for very different keys
		t.Log("bloom false positive (acceptable)")
	}
}

func TestSSTableMissingKey(t *testing.T) {
	dir := t.TempDir()
	path := writeTestSSTable(t, dir, 10, compression.CompressionNone)

	r, err := NewReader(path, ReaderOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	_, err = r.Get([]byte("missing_key"))
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestSSTableCompression(t *testing.T) {
	dir := t.TempDir()
	path := writeTestSSTable(t, dir, 100, compression.CompressionSnappy)

	r, err := NewReader(path, ReaderOptions{Compression: compression.CompressionSnappy})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%08d", i))
		val, err := r.Get(key)
		if err != nil {
			t.Fatalf("get key_%08d: %v", i, err)
		}
		expected := fmt.Sprintf("value_%08d", i)
		if string(val) != expected {
			t.Fatalf("value mismatch: got %s, want %s", val, expected)
		}
	}
}

func TestSSTableIterator(t *testing.T) {
	dir := t.TempDir()
	path := writeTestSSTable(t, dir, 50, compression.CompressionNone)

	r, err := NewReader(path, ReaderOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	it := NewIterator(r)
	it.SeekToFirst()

	count := 0
	for it.Valid() {
		count++
		it.Next()
	}

	if count != 50 {
		t.Fatalf("expected 50 entries, got %d", count)
	}
}

func TestSSTableSeek(t *testing.T) {
	dir := t.TempDir()
	path := writeTestSSTable(t, dir, 100, compression.CompressionNone)

	r, err := NewReader(path, ReaderOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	it := NewIterator(r)
	it.Seek([]byte("key_00000050"))

	if !it.Valid() {
		t.Fatal("iterator should be valid after seek")
	}
	if string(it.Key()) != "key_00000050" {
		t.Fatalf("expected key_00000050, got %s", it.Key())
	}
}

func TestSSTable100KEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.sst")

	w, err := NewWriter(WriterOptions{
		Path:        path,
		BlockSize:   4096,
		ExpectedKeys: 100000,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	for i := 0; i < 100000; i++ {
		key := []byte(fmt.Sprintf("key_%08d", i))
		value := []byte(fmt.Sprintf("val_%08d", i))
		w.Add(key, value)
	}
	w.Finish()

	r, err := NewReader(path, ReaderOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	// Spot check
	for _, i := range []int{0, 999, 50000, 99999} {
		key := []byte(fmt.Sprintf("key_%08d", i))
		val, err := r.Get(key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		expected := fmt.Sprintf("val_%08d", i)
		if string(val) != expected {
			t.Fatalf("value mismatch at %d", i)
		}
	}

	// Check file size is reasonable
	fi, _ := os.Stat(path)
	t.Logf("100K SSTable size: %d bytes", fi.Size())
}

func TestMergeIterator(t *testing.T) {
	dir := t.TempDir()

	// Create two SSTables
	path1 := filepath.Join(dir, "s1.sst")
	w1, _ := NewWriter(WriterOptions{Path: path1, ExpectedKeys: 3})
	w1.Add([]byte("a"), []byte("1"))
	w1.Add([]byte("c"), []byte("3"))
	w1.Add([]byte("e"), []byte("5"))
	w1.Finish()

	path2 := filepath.Join(dir, "s2.sst")
	w2, _ := NewWriter(WriterOptions{Path: path2, ExpectedKeys: 3})
	w2.Add([]byte("b"), []byte("2"))
	w2.Add([]byte("d"), []byte("4"))
	w2.Add([]byte("f"), []byte("6"))
	w2.Finish()

	r1, _ := NewReader(path1, ReaderOptions{})
	r2, _ := NewReader(path2, ReaderOptions{})

	it1 := NewIterator(r1)
	it2 := NewIterator(r2)

	merge := NewMergeIterator([]*Iterator{it1, it2})
	merge.SeekToFirst()

	expected := []string{"a", "b", "c", "d", "e", "f"}
	idx := 0
	for merge.Valid() {
		if idx >= len(expected) {
			t.Fatalf("too many entries")
		}
		if string(merge.Key()) != expected[idx] {
			t.Fatalf("key mismatch at %d: got %s, want %s", idx, merge.Key(), expected[idx])
		}
		merge.Next()
		idx++
	}
	if idx != len(expected) {
		t.Fatalf("expected %d entries, got %d", len(expected), idx)
	}

	r1.Close()
	r2.Close()
}

// Test Reader getter methods: Path(), Size(), Iter(), LargestKey()
func TestReader_Getters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	// Create SSTable
	w, err := NewWriter(WriterOptions{Path: path, ExpectedKeys: 3})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	w.Add([]byte("a"), []byte("1"))
	w.Add([]byte("b"), []byte("2"))
	w.Add([]byte("c"), []byte("3"))
	w.Finish()

	r, err := NewReader(path, ReaderOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	// Test Path()
	if got := r.Path(); got != path {
		t.Errorf("Path() = %q, want %q", got, path)
	}

	// Test Size() - should be non-zero
	size := r.Size()
	if size <= 0 {
		t.Errorf("Size() = %d, want > 0", size)
	}

	// Test LargestKey()
	largest := r.LargestKey()
	if string(largest) != "c" {
		t.Errorf("LargestKey() = %q, want %q", string(largest), "c")
	}

	// Test Iter() - iterates over all entries
	var count int
	err = r.Iter(func(key, value []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Errorf("Iter() error: %v", err)
	}
	if count != 3 {
		t.Errorf("Iter() visited %d entries, want 3", count)
	}
}

// Test Writer Entries() method
func TestWriter_Entries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	w, err := NewWriter(WriterOptions{Path: path, ExpectedKeys: 10})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Entries() should return 0 before adding
	if got := w.Entries(); got != 0 {
		t.Errorf("Entries() before Add = %d, want 0", got)
	}

	// Add some entries
	for i := 0; i < 5; i++ {
		w.Add([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
	}

	// Entries() should return 5
	if got := w.Entries(); got != 5 {
		t.Errorf("Entries() after Add = %d, want 5", got)
	}

	w.Finish()
}

// Test BlockReader NumEntries
func TestBlockReader_NumEntries(t *testing.T) {
	// Create a simple block with some entries
	// Block format: key-value pairs with restart points
	data := make([]byte, 0, 256)

	// Add some dummy entries
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("val%d", i))
		// Simple encoding: shared(1), keyLen(1), key, valLen(1), val
		data = append(data, 0) // shared = 0 (restart point)
		data = append(data, byte(len(key)))
		data = append(data, key...)
		data = append(data, byte(len(val)))
		data = append(data, val...)
	}

	// Add numRestarts (uint32) at the end
	numRestarts := uint32(5)
	data = append(data, byte(numRestarts))
	data = append(data, byte(numRestarts>>8))
	data = append(data, byte(numRestarts>>16))
	data = append(data, byte(numRestarts>>24))

	reader := NewBlockReader(data)
	// Note: BlockReader.NumEntries() returns numRestarts which is
	// used as an approximation of entry count
	if reader.NumEntries() != 5 {
		t.Errorf("NumEntries() = %d, want 5", reader.NumEntries())
	}
}

// Test IndexBlockBuilder Reset and Empty
func TestIndexBlockBuilder_ResetAndEmpty(t *testing.T) {
	b := NewIndexBlockBuilder()

	// Initially should be empty
	if !b.Empty() {
		t.Error("Empty() should return true for new builder")
	}

	// Add an entry
	b.Add([]byte("key1"), 100, 50)

	// Should not be empty now
	if b.Empty() {
		t.Error("Empty() should return false after adding entry")
	}

	// Reset the builder
	b.Reset()

	// Should be empty again
	if !b.Empty() {
		t.Error("Empty() should return true after Reset()")
	}
}

// Test Iterator Value() method
func TestIterator_Value(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	// Create SSTable with known values
	w, err := NewWriter(WriterOptions{Path: path, ExpectedKeys: 3})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	w.Add([]byte("a"), []byte("value_a"))
	w.Add([]byte("b"), []byte("value_b"))
	w.Add([]byte("c"), []byte("value_c"))
	w.Finish()

	r, err := NewReader(path, ReaderOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	it := NewIterator(r)
	it.SeekToFirst()

	// Verify Value() returns correct values
	expected := map[string]string{
		"a": "value_a",
		"b": "value_b",
		"c": "value_c",
	}

	found := make(map[string]string)
	for it.Valid() {
		key := string(it.Key())
		val := string(it.Value())
		found[key] = val
		it.Next()
	}

	for k, v := range expected {
		if found[k] != v {
			t.Errorf("Value for %s = %q, want %q", k, found[k], v)
		}
	}
}

// Test Iterator Value() when not valid
func TestIterator_ValueNotValid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	w, err := NewWriter(WriterOptions{Path: path, ExpectedKeys: 1})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	w.Add([]byte("key"), []byte("value"))
	w.Finish()

	r, err := NewReader(path, ReaderOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	it := NewIterator(r)

	// Value() before SeekToFirst should return nil
	val := it.Value()
	if val != nil {
		t.Errorf("Value() before SeekToFirst = %v, want nil", val)
	}

	// Key() before SeekToFirst should also return nil
	key := it.Key()
	if key != nil {
		t.Errorf("Key() before SeekToFirst = %v, want nil", key)
	}
}

// Test MergeIterator Value() method
func TestMergeIterator_Value(t *testing.T) {
	dir := t.TempDir()

	// Create two SSTables
	path1 := filepath.Join(dir, "s1.sst")
	w1, _ := NewWriter(WriterOptions{Path: path1, ExpectedKeys: 2})
	w1.Add([]byte("a"), []byte("val_a"))
	w1.Add([]byte("c"), []byte("val_c"))
	w1.Finish()

	path2 := filepath.Join(dir, "s2.sst")
	w2, _ := NewWriter(WriterOptions{Path: path2, ExpectedKeys: 2})
	w2.Add([]byte("b"), []byte("val_b"))
	w2.Add([]byte("d"), []byte("val_d"))
	w2.Finish()

	r1, _ := NewReader(path1, ReaderOptions{})
	r2, _ := NewReader(path2, ReaderOptions{})
	defer r1.Close()
	defer r2.Close()

	it1 := NewIterator(r1)
	it2 := NewIterator(r2)

	merge := NewMergeIterator([]*Iterator{it1, it2})
	merge.SeekToFirst()

	// Verify Value() returns correct values
	expected := map[string]string{
		"a": "val_a",
		"b": "val_b",
		"c": "val_c",
		"d": "val_d",
	}

	found := make(map[string]string)
	for merge.Valid() {
		key := string(merge.Key())
		val := string(merge.Value())
		found[key] = val
		merge.Next()
	}

	for k, v := range expected {
		if found[k] != v {
			t.Errorf("MergeIterator Value for %s = %q, want %q", k, found[k], v)
		}
	}
}
