package memtable

import (
	"bytes"
	"fmt"
	"testing"
)

func TestIteratorEmptyList(t *testing.T) {
	sl := NewSkipList(1 << 20)
	it := sl.NewIterator()

	it.SeekToFirst()
	if it.Valid() {
		t.Fatal("iterator should not be valid on empty list")
	}

	it.SeekToLast()
	if it.Valid() {
		t.Fatal("iterator should not be valid on empty list")
	}

	it.Seek([]byte("anything"))
	if it.Valid() {
		t.Fatal("iterator should not be valid on empty list seek")
	}
}

func TestIteratorSeekToFirst(t *testing.T) {
	sl := NewSkipList(1 << 20)
	sl.Put([]byte("b"), []byte("2"))
	sl.Put([]byte("a"), []byte("1"))
	sl.Put([]byte("c"), []byte("3"))

	it := sl.NewIterator()
	it.SeekToFirst()

	if !it.Valid() {
		t.Fatal("expected valid iterator")
	}
	if !bytes.Equal(it.Key(), []byte("a")) {
		t.Fatalf("expected key 'a', got %q", it.Key())
	}
	if !bytes.Equal(it.Value(), []byte("1")) {
		t.Fatalf("expected value '1', got %q", it.Value())
	}
}

func TestIteratorSeekToLast(t *testing.T) {
	sl := NewSkipList(1 << 20)
	sl.Put([]byte("b"), []byte("2"))
	sl.Put([]byte("a"), []byte("1"))
	sl.Put([]byte("c"), []byte("3"))

	it := sl.NewIterator()
	it.SeekToLast()

	if !it.Valid() {
		t.Fatal("expected valid iterator")
	}
	if !bytes.Equal(it.Key(), []byte("c")) {
		t.Fatalf("expected key 'c', got %q", it.Key())
	}
	if !bytes.Equal(it.Value(), []byte("3")) {
		t.Fatalf("expected value '3', got %q", it.Value())
	}
}

func TestIteratorNext(t *testing.T) {
	sl := NewSkipList(1 << 20)
	sl.Put([]byte("c"), []byte("3"))
	sl.Put([]byte("a"), []byte("1"))
	sl.Put([]byte("b"), []byte("2"))
	sl.Put([]byte("d"), []byte("4"))

	expected := []struct {
		key, value string
	}{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
		{"d", "4"},
	}

	it := sl.NewIterator()
	it.SeekToFirst()

	for i, exp := range expected {
		if !it.Valid() {
			t.Fatalf("expected valid at position %d", i)
		}
		if !bytes.Equal(it.Key(), []byte(exp.key)) {
			t.Fatalf("position %d: expected key %q, got %q", i, exp.key, it.Key())
		}
		if !bytes.Equal(it.Value(), []byte(exp.value)) {
			t.Fatalf("position %d: expected value %q, got %q", i, exp.value, it.Value())
		}
		it.Next()
	}

	if it.Valid() {
		t.Fatal("expected iterator to be invalid after all entries")
	}
}

func TestIteratorPrev(t *testing.T) {
	sl := NewSkipList(1 << 20)
	sl.Put([]byte("c"), []byte("3"))
	sl.Put([]byte("a"), []byte("1"))
	sl.Put([]byte("b"), []byte("2"))

	it := sl.NewIterator()
	it.SeekToLast()

	expected := []string{"c", "b", "a"}
	for i, exp := range expected {
		if !it.Valid() {
			t.Fatalf("expected valid at position %d", i)
		}
		if !bytes.Equal(it.Key(), []byte(exp)) {
			t.Fatalf("position %d: expected key %q, got %q", i, exp, it.Key())
		}
		it.Prev()
	}

	if it.Valid() {
		t.Fatal("expected iterator to be invalid after all entries")
	}
}

func TestIteratorSeek(t *testing.T) {
	sl := NewSkipList(1 << 20)
	sl.Put([]byte("apple"), []byte("1"))
	sl.Put([]byte("banana"), []byte("2"))
	sl.Put([]byte("cherry"), []byte("3"))
	sl.Put([]byte("durian"), []byte("4"))

	tests := []struct {
		seek    string
		wantKey string
		valid   bool
	}{
		{"apple", "apple", true},   // exact match
		{"apricot", "banana", true}, // between apple and banana
		{"blueberry", "cherry", true},
		{"zzz", "", false},          // after all keys
		{"a", "apple", true},        // before all keys
		{"banana", "banana", true},  // exact match
	}

	for _, tt := range tests {
		it := sl.NewIterator()
		it.Seek([]byte(tt.seek))
		if tt.valid {
			if !it.Valid() {
				t.Fatalf("Seek(%q): expected valid iterator", tt.seek)
			}
			if !bytes.Equal(it.Key(), []byte(tt.wantKey)) {
				t.Fatalf("Seek(%q): expected key %q, got %q", tt.seek, tt.wantKey, it.Key())
			}
		} else {
			if it.Valid() {
				t.Fatalf("Seek(%q): expected invalid iterator, got key %q", tt.seek, it.Key())
			}
		}
	}
}

func TestIteratorSeekForPrev(t *testing.T) {
	sl := NewSkipList(1 << 20)
	sl.Put([]byte("apple"), []byte("1"))
	sl.Put([]byte("banana"), []byte("2"))
	sl.Put([]byte("cherry"), []byte("3"))

	tests := []struct {
		seek    string
		wantKey string
		valid   bool
	}{
		{"cherry", "cherry", true},
		{"coconut", "cherry", true},
		{"blueberry", "banana", true},
		{"a", "", false},
		{"banana", "banana", true},
	}

	for _, tt := range tests {
		it := sl.NewIterator()
		it.SeekForPrev([]byte(tt.seek))
		if tt.valid {
			if !it.Valid() {
				t.Fatalf("SeekForPrev(%q): expected valid iterator", tt.seek)
			}
			if !bytes.Equal(it.Key(), []byte(tt.wantKey)) {
				t.Fatalf("SeekForPrev(%q): expected key %q, got %q", tt.seek, tt.wantKey, it.Key())
			}
		} else {
			if it.Valid() {
				t.Fatalf("SeekForPrev(%q): expected invalid iterator, got key %q", tt.seek, it.Key())
			}
		}
	}
}

func TestIteratorForwardAndBackward(t *testing.T) {
	sl := NewSkipList(1 << 20)

	const n = 100
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		val := []byte(fmt.Sprintf("val_%04d", i))
		sl.Put(key, val)
	}

	// Forward iteration.
	it := sl.NewIterator()
	it.SeekToFirst()
	count := 0
	for it.Valid() {
		count++
		it.Next()
	}
	if count != n {
		t.Fatalf("forward: expected %d entries, got %d", n, count)
	}

	// Backward iteration.
	it.SeekToLast()
	count = 0
	for it.Valid() {
		count++
		it.Prev()
	}
	if count != n {
		t.Fatalf("backward: expected %d entries, got %d", n, count)
	}
}

func TestIteratorTombstone(t *testing.T) {
	sl := NewSkipList(1 << 20)
	sl.Put([]byte("keep"), []byte("yes"))
	sl.Put([]byte("delete_me"), []byte("temporary"))
	sl.Delete([]byte("delete_me"))
	sl.Put([]byte("also_keep"), []byte("yes"))

	// Get should not return tombstoned key.
	_, ok := sl.Get([]byte("delete_me"))
	if ok {
		t.Fatal("expected tombstoned key to be invisible to Get")
	}

	// Iterator should still see the tombstoned key (the raw entry exists).
	it := sl.NewIterator()
	it.SeekToFirst()
	found := false
	for it.Valid() {
		if string(it.Key()) == "delete_me" {
			found = true
			// Value() should return nil for tombstones.
			if it.Value() != nil {
				t.Fatal("expected nil value for tombstone in iterator")
			}
		}
		it.Next()
	}
	if !found {
		t.Fatal("expected to find tombstoned key during iteration")
	}
}
