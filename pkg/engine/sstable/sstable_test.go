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
