package memtable

import (
	"bytes"
	"fmt"
	"testing"
)

func TestMemtableBasic(t *testing.T) {
	m := NewMemtable(1 << 20)

	m.Put([]byte("key1"), []byte("val1"), 1)
	m.Put([]byte("key2"), []byte("val2"), 2)
	m.Put([]byte("key3"), []byte("val3"), 3)

	val, ok := m.Get([]byte("key1"))
	if !ok || !bytes.Equal(val, []byte("val1")) {
		t.Fatalf("expected (val1, true), got (%q, %v)", val, ok)
	}

	if m.SeqNum() != 3 {
		t.Fatalf("expected seqNum 3, got %d", m.SeqNum())
	}
}

func TestMemtableDelete(t *testing.T) {
	m := NewMemtable(1 << 20)

	m.Put([]byte("key1"), []byte("val1"), 1)
	m.Delete([]byte("key1"), 2)

	_, ok := m.Get([]byte("key1"))
	if ok {
		t.Fatal("expected key1 to be deleted")
	}

	if m.SeqNum() != 2 {
		t.Fatalf("expected seqNum 2, got %d", m.SeqNum())
	}
}

func TestMemtableReadOnly(t *testing.T) {
	m := NewMemtable(1 << 20)
	m.Put([]byte("key"), []byte("val"), 1)

	if m.IsReadOnly() {
		t.Fatal("expected memtable to be writable")
	}

	m.SetReadOnly()
	if !m.IsReadOnly() {
		t.Fatal("expected memtable to be read-only")
	}

	// Reads should still work.
	val, ok := m.Get([]byte("key"))
	if !ok || !bytes.Equal(val, []byte("val")) {
		t.Fatalf("expected to read from read-only memtable")
	}

	// Writes should panic.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on Put to read-only memtable")
		}
	}()
	m.Put([]byte("key2"), []byte("val2"), 2)
}

func TestMemtableIterator(t *testing.T) {
	m := NewMemtable(1 << 20)
	m.Put([]byte("c"), []byte("3"), 3)
	m.Put([]byte("a"), []byte("1"), 1)
	m.Put([]byte("b"), []byte("2"), 2)

	expected := []string{"a", "b", "c"}
	it := m.NewIterator()
	it.SeekToFirst()

	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		it.Next()
	}

	if len(keys) != len(expected) {
		t.Fatalf("expected %d keys, got %d", len(expected), len(keys))
	}
	for i, k := range expected {
		if keys[i] != k {
			t.Fatalf("position %d: expected %q, got %q", i, k, keys[i])
		}
	}
}

func TestMemtableApproximateSize(t *testing.T) {
	m := NewMemtable(1 << 20)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		val := []byte(fmt.Sprintf("val_%04d", i))
		m.Put(key, val, uint64(i))
	}

	size := m.ApproximateSize()
	if size <= 0 {
		t.Fatalf("expected positive approximate size, got %d", size)
	}
	t.Logf("Approximate size for 100 entries: %d bytes", size)
}

func TestMemtableManagerLifecycle(t *testing.T) {
	mgr := NewMemtableManager(1 << 20)

	active := mgr.ActiveMemtable()
	if active == nil {
		t.Fatal("expected non-nil active memtable")
	}
	if active.IsReadOnly() {
		t.Fatal("active memtable should be writable")
	}

	// Write to active.
	active.Put([]byte("key1"), []byte("val1"), 1)

	// No immutable memtables yet.
	if mgr.ImmutableCount() != 0 {
		t.Fatalf("expected 0 immutable, got %d", mgr.ImmutableCount())
	}

	// Rotate.
	rotated := mgr.Rotate()
	if rotated != active {
		t.Fatal("rotated memtable should be the old active")
	}
	if !rotated.IsReadOnly() {
		t.Fatal("rotated memtable should be read-only")
	}

	// New active should be different.
	newActive := mgr.ActiveMemtable()
	if newActive == active {
		t.Fatal("expected new active memtable after rotation")
	}

	// Immutable queue should have 1 entry.
	if mgr.ImmutableCount() != 1 {
		t.Fatalf("expected 1 immutable, got %d", mgr.ImmutableCount())
	}

	imms := mgr.ImmutableMemtables()
	if len(imms) != 1 || imms[0] != rotated {
		t.Fatal("immutable list should contain the rotated memtable")
	}
}

func TestMemtableManagerRemoveImmutable(t *testing.T) {
	mgr := NewMemtableManager(1 << 20)

	mgr.ActiveMemtable().Put([]byte("k1"), []byte("v1"), 1)
	imm1 := mgr.Rotate()

	mgr.ActiveMemtable().Put([]byte("k2"), []byte("v2"), 2)
	imm2 := mgr.Rotate()

	if mgr.ImmutableCount() != 2 {
		t.Fatalf("expected 2 immutable, got %d", mgr.ImmutableCount())
	}

	// Remove the first immutable (simulates flush completion).
	mgr.RemoveImmutable(imm1)

	if mgr.ImmutableCount() != 1 {
		t.Fatalf("expected 1 immutable after removal, got %d", mgr.ImmutableCount())
	}

	imms := mgr.ImmutableMemtables()
	if len(imms) != 1 || imms[0] != imm2 {
		t.Fatal("remaining immutable should be imm2")
	}

	// Remove the second.
	mgr.RemoveImmutable(imm2)
	if mgr.ImmutableCount() != 0 {
		t.Fatalf("expected 0 immutable, got %d", mgr.ImmutableCount())
	}
}

func TestMemtableManagerRotation(t *testing.T) {
	mgr := NewMemtableManager(4 << 20)
	mgr.SetRotateSize(512) // Small threshold for testing.

	// Write enough data to trigger rotation.
	active := mgr.ActiveMemtable()
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		val := []byte(fmt.Sprintf("val_%04d", i))
		active.Put(key, val, uint64(i))
	}

	rotated, ok := mgr.MaybeRotate()
	if !ok {
		t.Fatal("expected rotation to trigger")
	}
	if rotated == nil {
		t.Fatal("expected non-nil rotated memtable")
	}
	if !rotated.IsReadOnly() {
		t.Fatal("rotated memtable should be read-only")
	}
}

func TestMemtableManagerMaybeRotateNoTrigger(t *testing.T) {
	mgr := NewMemtableManager(4 << 20)
	mgr.SetRotateSize(1 << 30) // Very large threshold.

	mgr.ActiveMemtable().Put([]byte("k"), []byte("v"), 1)

	rotated, ok := mgr.MaybeRotate()
	if ok {
		t.Fatal("did not expect rotation to trigger")
	}
	if rotated != nil {
		t.Fatal("expected nil rotated memtable")
	}
}

func TestMemtableManagerMultipleRotations(t *testing.T) {
	mgr := NewMemtableManager(4 << 20)

	// Simulate multiple rotations.
	for r := 0; r < 5; r++ {
		active := mgr.ActiveMemtable()
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("r%d_key_%02d", r, i))
			val := []byte(fmt.Sprintf("r%d_val_%02d", r, i))
			active.Put(key, val, uint64(r*100+i))
		}
		mgr.Rotate()
	}

	if mgr.ImmutableCount() != 5 {
		t.Fatalf("expected 5 immutable, got %d", mgr.ImmutableCount())
	}

	// Flush them all.
	for _, imm := range mgr.ImmutableMemtables() {
		mgr.RemoveImmutable(imm)
	}
	if mgr.ImmutableCount() != 0 {
		t.Fatalf("expected 0 immutable after flush, got %d", mgr.ImmutableCount())
	}
}

func TestMemtableSeqNumMonotonic(t *testing.T) {
	m := NewMemtable(1 << 20)

	// Out-of-order sequence numbers.
	m.Put([]byte("k1"), []byte("v1"), 10)
	if m.SeqNum() != 10 {
		t.Fatalf("expected seqNum 10, got %d", m.SeqNum())
	}

	m.Put([]byte("k2"), []byte("v2"), 5)
	// SeqNum should stay at 10 (5 < 10).
	if m.SeqNum() != 10 {
		t.Fatalf("expected seqNum to remain 10, got %d", m.SeqNum())
	}

	m.Put([]byte("k3"), []byte("v3"), 20)
	if m.SeqNum() != 20 {
		t.Fatalf("expected seqNum 20, got %d", m.SeqNum())
	}
}

// Test Memtable SkipList() getter
func TestMemtable_SkipList(t *testing.T) {
	m := NewMemtable(1 << 20)

	// SkipList() should return the underlying skip list
	sl := m.SkipList()
	if sl == nil {
		t.Fatal("SkipList() returned nil")
	}

	// We can use the skip list to add entries directly
	// (though normally this is done through Memtable.Put)
	sl.Put([]byte("test"), []byte("value"))

	// Verify the data exists
	val, ok := sl.Get([]byte("test"))
	if !ok || !bytes.Equal(val, []byte("value")) {
		t.Errorf("expected to find value in skip list")
	}
}
