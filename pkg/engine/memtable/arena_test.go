package memtable

import (
	"bytes"
	"testing"
)

func TestArenaAllocate(t *testing.T) {
	a := NewArena(1024)

	off1 := a.allocate(10)
	if off1 == nullOffset {
		t.Fatal("expected valid offset")
	}
	// First allocation should start at offset 0.
	if off1 != 0 {
		t.Fatalf("expected first offset to be 0, got %d", off1)
	}

	off2 := a.allocate(20)
	if off2 == nullOffset {
		t.Fatal("expected valid offset")
	}
	if off2 <= off1 {
		t.Fatalf("expected offset %d > %d", off2, off1)
	}
	if a.Size() != 30 {
		t.Fatalf("expected size 30, got %d", a.Size())
	}
}

func TestArenaAllocateOutOfBounds(t *testing.T) {
	a := NewArena(16)

	off := a.allocate(16)
	if off == nullOffset {
		t.Fatal("first allocation should succeed")
	}

	off2 := a.allocate(1)
	if off2 != nullOffset {
		t.Fatal("expected allocation to fail when arena is full")
	}
}

func TestArenaReset(t *testing.T) {
	a := NewArena(1024)

	a.allocate(100)
	if a.Size() != 100 {
		t.Fatalf("expected size 100, got %d", a.Size())
	}

	a.Reset()
	if a.Size() != 0 {
		t.Fatalf("expected size 0 after reset, got %d", a.Size())
	}

	// Should be able to allocate again.
	off := a.allocate(200)
	if off == nullOffset {
		t.Fatal("expected allocation to succeed after reset")
	}
}

func TestArenaNodeAllocation(t *testing.T) {
	a := NewArena(4096)

	key := []byte("hello")
	value := []byte("world")
	height := 5

	off := a.allocateNode(key, value, height)
	if off == nullOffset {
		t.Fatal("node allocation failed")
	}

	// Verify key.
	gotKey := a.getKey(off)
	if !bytes.Equal(gotKey, key) {
		t.Fatalf("expected key %q, got %q", key, gotKey)
	}

	// Verify value.
	gotValue := a.getValue(off)
	if !bytes.Equal(gotValue, value) {
		t.Fatalf("expected value %q, got %q", value, gotValue)
	}

	// Verify height.
	gotHeight := a.getHeight(off)
	if gotHeight != height {
		t.Fatalf("expected height %d, got %d", height, gotHeight)
	}

	// Verify tower is initialized to nullOffset.
	for i := 0; i < height; i++ {
		fwd := a.getForward(off, i)
		if fwd != nullOffset {
			t.Fatalf("expected forward[%d]=nullOffset, got %d", i, fwd)
		}
	}
}

func TestArenaSetForward(t *testing.T) {
	a := NewArena(4096)

	off := a.allocateNode([]byte("k"), []byte("v"), 3)
	if off == nullOffset {
		t.Fatal("node allocation failed")
	}

	// Set forward pointers.
	a.setForward(off, 0, 42)
	a.setForward(off, 1, 99)
	a.setForward(off, 2, 200)

	if a.getForward(off, 0) != 42 {
		t.Fatalf("expected forward[0]=42, got %d", a.getForward(off, 0))
	}
	if a.getForward(off, 1) != 99 {
		t.Fatalf("expected forward[1]=99, got %d", a.getForward(off, 1))
	}
	if a.getForward(off, 2) != 200 {
		t.Fatalf("expected forward[2]=200, got %d", a.getForward(off, 2))
	}
}

func TestArenaNodeSize(t *testing.T) {
	key := []byte("key123")
	value := []byte("value456")
	height := 4

	expected := fullNodeSize(len(key), len(value), height)
	// "key123" = 6 bytes, "value456" = 8 bytes
	// 4 + 6 + 4 + 8 + 4 + 4*4 = 42
	if expected != 42 {
		t.Fatalf("expected node size 42, got %d", expected)
	}

	a := NewArena(4096)
	off := a.allocateNode(key, value, height)
	if off == nullOffset {
		t.Fatal("allocation failed")
	}

	// The arena should have consumed exactly 'expected' bytes.
	if a.Size() != uint32(expected) {
		t.Fatalf("expected arena size %d, got %d", expected, a.Size())
	}
}

func TestArenaEmptyKeyAndValue(t *testing.T) {
	a := NewArena(4096)

	off := a.allocateNode(nil, nil, 3)
	if off == nullOffset {
		t.Fatal("allocation failed for nil key/value")
	}

	if a.getKey(off) != nil {
		t.Fatalf("expected nil key, got %q", a.getKey(off))
	}
	if a.getValue(off) != nil {
		t.Fatalf("expected nil value, got %q", a.getValue(off))
	}
	if a.getHeight(off) != 3 {
		t.Fatalf("expected height 3, got %d", a.getHeight(off))
	}
}

func TestArenaDefaultSize(t *testing.T) {
	a := NewArena(0)
	if a.Cap() != defaultArenaSize {
		t.Fatalf("expected default cap %d, got %d", defaultArenaSize, a.Cap())
	}
}

func TestArenaMultipleNodes(t *testing.T) {
	a := NewArena(4096)

	off1 := a.allocateNode([]byte("key1"), []byte("val1"), 2)
	off2 := a.allocateNode([]byte("key2"), []byte("val2"), 3)
	off3 := a.allocateNode([]byte("key3"), []byte("val3"), 1)

	if off1 == nullOffset || off2 == nullOffset || off3 == nullOffset {
		t.Fatal("one or more allocations failed")
	}

	// Set forward link from node1 to node2.
	a.setForward(off1, 0, off2)
	// Set forward link from node2 to node3.
	a.setForward(off2, 0, off3)

	// Traverse: off1 -> off2 -> off3.
	next := a.getForward(off1, 0)
	if next != off2 {
		t.Fatalf("expected forward to off2 (%d), got %d", off2, next)
	}

	next = a.getForward(off2, 0)
	if next != off3 {
		t.Fatalf("expected forward to off3 (%d), got %d", off3, next)
	}

	// Verify data at node3.
	if !bytes.Equal(a.getKey(off3), []byte("key3")) {
		t.Fatalf("unexpected key at node3: %q", a.getKey(off3))
	}
}
