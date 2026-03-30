package memtable

import (
	"encoding/binary"
	"sync"
)

const (
	// defaultArenaSize is the default arena size in bytes (4 MB).
	defaultArenaSize = 4 << 20

	// nullOffset is the sentinel value meaning "no node" in tower pointers.
	// Using max uint32 avoids conflicts with real offsets.
	nullOffset = ^uint32(0)
)

// Arena is a fixed-size bump allocator that holds skip list node data.
// It reduces GC pressure by allocating all node data in a contiguous byte slice.
//
// Node layout in arena (all multi-byte values are little-endian uint32):
//
//	[keyLen:4][key:keyLen][valueLen:4][value:valueLen][height:4][tower:height*4]
//
// Tower entries are uint32 offsets to the next node at each level,
// or nullOffset (^uint32(0)) to indicate no next node.
type Arena struct {
	mu   sync.Mutex
	buf  []byte
	size uint32
}

// NewArena creates a new arena with the given capacity in bytes.
func NewArena(capacity int) *Arena {
	if capacity <= 0 {
		capacity = defaultArenaSize
	}
	return &Arena{
		buf: make([]byte, capacity),
	}
}

// allocate reserves n bytes in the arena and returns the starting offset.
// Returns nullOffset if there is insufficient space.
func (a *Arena) allocate(n int) uint32 {
	if n <= 0 {
		return nullOffset
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	if uint64(a.size)+uint64(n) > uint64(len(a.buf)) {
		return nullOffset
	}
	offset := a.size
	a.size += uint32(n)
	return offset
}

// Reset clears the arena so all space can be reused.
func (a *Arena) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.size = 0
}

// Bytes returns the underlying byte slice of the arena.
func (a *Arena) Bytes() []byte {
	return a.buf
}

// Size returns the number of bytes currently allocated.
func (a *Arena) Size() uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.size
}

// Cap returns the total capacity of the arena in bytes.
func (a *Arena) Cap() int {
	return len(a.buf)
}

// --- Low-level helpers ---

func (a *Arena) getUint32(offset uint32) uint32 {
	return binary.LittleEndian.Uint32(a.buf[offset:])
}

func (a *Arena) putUint32(offset uint32, v uint32) {
	binary.LittleEndian.PutUint32(a.buf[offset:], v)
}

func (a *Arena) getBytes(offset uint32, n int) []byte {
	return a.buf[offset : offset+uint32(n)]
}

func (a *Arena) putBytes(offset uint32, data []byte) {
	copy(a.buf[offset:], data)
}

// --- Node layout constants ---
//
// At a node starting at `off`:
//   off+0                    : keyLen  (uint32)
//   off+4                    : key     (keyLen bytes)
//   off+4+keyLen             : valueLen(uint32)
//   off+8+keyLen             : value   (valueLen bytes)
//   off+8+keyLen+valueLen    : height  (uint32)
//   off+12+keyLen+valueLen   : tower   (height * uint32)

func nodeKeyLenOff(off uint32) uint32         { return off }
func nodeKeyOff(off uint32) uint32            { return off + 4 }
func nodeValueLenOff(off, kl uint32) uint32   { return off + 4 + kl }
func nodeValueOff(off, kl uint32) uint32      { return off + 8 + kl }
func nodeHeightOff(off, kl, vl uint32) uint32 { return off + 8 + kl + vl }
func nodeTowerOff(off, kl, vl uint32) uint32  { return off + 12 + kl + vl }

// fullNodeSize returns total bytes for a node with given key, value, and height.
func fullNodeSize(keyLen, valueLen int, height int) int {
	return 4 + keyLen + 4 + valueLen + 4 + height*4
}

// allocateNode allocates space and writes the full node. Returns the offset.
func (a *Arena) allocateNode(key, value []byte, height int) uint32 {
	n := fullNodeSize(len(key), len(value), height)
	off := a.allocate(n)
	if off == nullOffset {
		return nullOffset
	}
	a.writeNode(off, key, value, height)
	return off
}

// writeNode writes a complete node at the given offset.
func (a *Arena) writeNode(off uint32, key, value []byte, height int) {
	kl := uint32(len(key))
	vl := uint32(len(value))

	a.putUint32(nodeKeyLenOff(off), kl)
	if kl > 0 {
		a.putBytes(nodeKeyOff(off), key)
	}

	a.putUint32(nodeValueLenOff(off, kl), vl)
	if vl > 0 {
		a.putBytes(nodeValueOff(off, kl), value)
	}

	a.putUint32(nodeHeightOff(off, kl, vl), uint32(height))

	// Initialize tower to nullOffset (no forward links yet).
	tOff := nodeTowerOff(off, kl, vl)
	for i := 0; i < height; i++ {
		a.putUint32(tOff+uint32(i)*4, nullOffset)
	}
}

// --- Node field readers ---

func (a *Arena) getKey(off uint32) []byte {
	kl := a.getUint32(nodeKeyLenOff(off))
	if kl == 0 {
		return nil
	}
	return a.getBytes(nodeKeyOff(off), int(kl))
}

func (a *Arena) getValue(off uint32) []byte {
	kl := a.getUint32(nodeKeyLenOff(off))
	vl := a.getUint32(nodeValueLenOff(off, kl))
	if vl == 0 {
		return nil
	}
	return a.getBytes(nodeValueOff(off, kl), int(vl))
}

func (a *Arena) getHeight(off uint32) int {
	kl := a.getUint32(nodeKeyLenOff(off))
	vl := a.getUint32(nodeValueLenOff(off, kl))
	return int(a.getUint32(nodeHeightOff(off, kl, vl)))
}

func (a *Arena) getForward(off uint32, level int) uint32 {
	kl := a.getUint32(nodeKeyLenOff(off))
	vl := a.getUint32(nodeValueLenOff(off, kl))
	tOff := nodeTowerOff(off, kl, vl)
	return a.getUint32(tOff + uint32(level)*4)
}

func (a *Arena) setForward(off uint32, level int, target uint32) {
	kl := a.getUint32(nodeKeyLenOff(off))
	vl := a.getUint32(nodeValueLenOff(off, kl))
	tOff := nodeTowerOff(off, kl, vl)
	a.putUint32(tOff+uint32(level)*4, target)
}
