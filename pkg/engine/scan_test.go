package engine

import (
	"testing"
)

func TestEngineScanPrefix(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	// Insert 50 keys with prefix "col1:" and 30 with prefix "col2:"
	for i := 0; i < 50; i++ {
		key := []byte("col1:" + padInt(i, 3))
		val := []byte("val1")
		if err := e.Put(key, val); err != nil {
			t.Fatalf("Put col1:%d: %v", i, err)
		}
	}
	for i := 0; i < 30; i++ {
		key := []byte("col2:" + padInt(i, 3))
		val := []byte("val2")
		if err := e.Put(key, val); err != nil {
			t.Fatalf("Put col2:%d: %v", i, err)
		}
	}

	// Scan col1: should return exactly 50
	var count int
	err = e.Scan([]byte("col1:"), func(key, value []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 50 {
		t.Errorf("Scan col1: count = %d, want 50", count)
	}

	// Scan col2: should return exactly 30
	count = 0
	err = e.Scan([]byte("col2:"), func(key, value []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 30 {
		t.Errorf("Scan col2: count = %d, want 30", count)
	}
}

func TestEngineScanSorted(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	keys := []string{"col:b", "col:a", "col:d", "col:c", "col:e"}
	for _, k := range keys {
		if err := e.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	var result []string
	e.Scan([]byte("col:"), func(key, value []byte) bool {
		result = append(result, string(key))
		return true
	})

	expected := []string{"col:a", "col:b", "col:c", "col:d", "col:e"}
	if len(result) != len(expected) {
		t.Fatalf("Scan result count = %d, want %d", len(result), len(expected))
	}
	for i, k := range result {
		if k != expected[i] {
			t.Errorf("result[%d] = %q, want %q", i, k, expected[i])
		}
	}
}

func TestEngineScanWithDelete(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	e.Put([]byte("col:1"), []byte("v1"))
	e.Put([]byte("col:2"), []byte("v2"))
	e.Put([]byte("col:3"), []byte("v3"))
	e.Delete([]byte("col:2"))

	var result []string
	e.Scan([]byte("col:"), func(key, value []byte) bool {
		result = append(result, string(key))
		return true
	})

	if len(result) != 2 {
		t.Errorf("Scan after delete: got %d keys, want 2: %v", len(result), result)
	}
}

func TestEngineScanStopEarly(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	for i := 0; i < 100; i++ {
		e.Put([]byte("k:"+padInt(i, 3)), []byte("v"))
	}

	var count int
	e.Scan([]byte("k:"), func(key, value []byte) bool {
		count++
		return count < 5 // stop after 5
	})
	if count != 5 {
		t.Errorf("Scan stop early: got %d, want 5", count)
	}
}

func TestEnginePrefixIterator(t *testing.T) {
	dir := tempDir(t)
	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	for i := 0; i < 20; i++ {
		e.Put([]byte("item:"+padInt(i, 3)), []byte("val"))
	}

	it := e.NewPrefixIterator([]byte("item:"))
	defer it.Close()

	var count int
	for it.Next() {
		count++
		key := it.Key()
		if len(key) == 0 {
			t.Error("iterator returned empty key")
		}
	}
	if count != 20 {
		t.Errorf("PrefixIterator count = %d, want 20", count)
	}
	if it.Err() != nil {
		t.Errorf("PrefixIterator.Err() = %v", it.Err())
	}
}

func TestEngineScanAfterFlush(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	// Write enough to trigger flushes
	for i := 0; i < 100; i++ {
		e.Put([]byte("col:"+padInt(i, 3)), []byte("val"))
	}
	e.Flush()

	var count int
	err = e.Scan([]byte("col:"), func(key, value []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 100 {
		t.Errorf("Scan after flush: count = %d, want 100", count)
	}
}

func padInt(v, width int) string {
	s := ""
	n := v
	for i := 0; i < width; i++ {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
