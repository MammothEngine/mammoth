package memtable

import (
	"bytes"
	"math/rand/v2"
	"sync"
	"sync/atomic"
)

const (
	// maxHeight is the maximum height of the skip list tower.
	maxHeight = 20

	// probability determines the chance of a node having height h+1 vs h.
	// 1/4 gives good space/time tradeoff for skip lists.
	probability = 1.0 / 4.0
)

// Tombstone is a sentinel value that marks a key as deleted.
// When a node's value equals Tombstone, the key is considered deleted.
var Tombstone = []byte("\x00\x00\x00\x00TOMB")

// SkipList is a concurrent skip list backed by an arena allocator.
// It supports ordered key-value operations with O(log n) expected time.
//
// Concurrency model: single-writer, concurrent readers.
// All mutations acquire writeMu. Reads proceed lock-free via atomic operations
// on the tower pointers and height field.
type SkipList struct {
	arena   *Arena
	headOff uint32 // offset of the sentinel head node in arena

	height atomic.Int32 // current max height of the skip list

	writeMu sync.Mutex // serializes writes (Put/Delete)
	rng     *rand.Rand // per-instance random generator for height
	rngMu   sync.Mutex // protects rng
}

// NewSkipList creates a new skip list with the given arena size in bytes.
func NewSkipList(arenaSize int) *SkipList {
	arena := NewArena(arenaSize)

	// Allocate the head sentinel node with empty key, empty value,
	// and maximum tower height. The head never holds real data.
	headOff := arena.allocateNode(nil, nil, maxHeight)
	if headOff == nullOffset {
		panic("memtable: arena too small for head node")
	}

	s := &SkipList{
		arena:   arena,
		headOff: headOff,
		rng:     rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}
	s.height.Store(1)
	return s
}

// randomHeight returns a random tower height in [1, maxHeight].
func (s *SkipList) randomHeight() int {
	s.rngMu.Lock()
	defer s.rngMu.Unlock()

	h := 1
	for h < maxHeight && s.rng.Float64() < probability {
		h++
	}
	return h
}

// Height returns the current maximum height of the skip list.
func (s *SkipList) Height() int {
	return int(s.height.Load())
}

// ApproximateSize returns the estimated memory usage in bytes.
func (s *SkipList) ApproximateSize() int {
	return int(s.arena.Size())
}

// Arena returns the underlying arena (used by iterator).
func (s *SkipList) Arena() *Arena {
	return s.arena
}

// HeadOffset returns the offset of the head node (used by iterator).
func (s *SkipList) HeadOffset() uint32 {
	return s.headOff
}

// Put inserts or updates a key-value pair in the skip list.
// Thread-safe: acquires writeMu to serialize writes.
func (s *SkipList) Put(key, value []byte) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	h := s.Height()
	// prev[i] holds the predecessor node at level i for the insertion.
	prev := make([]uint32, h)
	// Start from the head node.
	cur := s.headOff

	// Search from top level down to find insertion point.
	for i := h - 1; i >= 0; i-- {
		// Walk right while the next node's key < our key.
		for {
			next := s.arena.getForward(cur, i)
			if next == nullOffset {
				break
			}
			cmp := bytes.Compare(s.arena.getKey(next), key)
			if cmp >= 0 {
				// next key >= target key, stop.
				if cmp == 0 {
					// Exact match: update value in place.
					s.updateValue(next, value)
					return
				}
				break
			}
			cur = next
		}
		prev[i] = cur
	}

	// Insert new node.
	newHeight := s.randomHeight()
	newOff := s.arena.allocateNode(key, value, newHeight)
	if newOff == nullOffset {
		panic("memtable: arena out of space")
	}

	// If the new node is taller than the current list height,
	// extend the prev array with head as predecessor for new levels.
	curHeight := h
	if newHeight > curHeight {
		for i := curHeight; i < newHeight; i++ {
			prev = append(prev, s.headOff)
		}
		// Update list height atomically.
		s.height.Store(int32(newHeight))
	}

	// Build the tower bottom-up, splicing the new node in at each level.
	for i := 0; i < newHeight; i++ {
		// The new node's forward[i] takes the predecessor's current forward[i].
		prevNode := prev[i]
		s.arena.setForward(newOff, i, s.arena.getForward(prevNode, i))
		// The predecessor now points to the new node.
		s.arena.setForward(prevNode, i, newOff)
	}
}

// updateValue rewrites the value of an existing node at off.
// Since the value length may differ, we allocate a new node and fix links.
func (s *SkipList) updateValue(off uint32, value []byte) {
	key := s.arena.getKey(off)
	nodeH := s.arena.getHeight(off)

	// Allocate a fresh node and relink.
	newOff := s.arena.allocateNode(key, value, nodeH)
	if newOff == nullOffset {
		panic("memtable: arena out of space during update")
	}

	// Copy forward pointers from old node.
	for i := 0; i < nodeH; i++ {
		s.arena.setForward(newOff, i, s.arena.getForward(off, i))
	}

	// Fix all predecessor nodes that pointed to old node.
	h := s.Height()
	cur := s.headOff
	for i := h - 1; i >= 0; i-- {
		for {
			next := s.arena.getForward(cur, i)
			if next == nullOffset {
				break
			}
			if next == off {
				// cur -> off at level i. Redirect to newOff.
				s.arena.setForward(cur, i, newOff)
				break
			}
			if bytes.Compare(s.arena.getKey(next), key) >= 0 {
				break
			}
			cur = next
		}
	}
}

// Get looks up a key and returns its value. Returns (value, true) if found,
// or (nil, false) if the key does not exist or is tombstoned.
func (s *SkipList) Get(key []byte) ([]byte, bool) {
	cur := s.headOff
	h := s.Height()

	for i := h - 1; i >= 0; i-- {
		for {
			next := s.arena.getForward(cur, i)
			if next == nullOffset {
				break
			}
			cmp := bytes.Compare(s.arena.getKey(next), key)
			if cmp > 0 {
				break
			}
			if cmp == 0 {
				// Found. Check for tombstone.
				val := s.arena.getValue(next)
				if isTombstone(val) {
					return nil, false
				}
				return val, true
			}
			cur = next
		}
	}
	return nil, false
}

// Delete inserts a tombstone marker for the given key.
func (s *SkipList) Delete(key []byte) {
	tomb := make([]byte, len(Tombstone))
	copy(tomb, Tombstone)
	s.Put(key, tomb)
}

// isTombstone checks whether a value is the deletion sentinel.
func isTombstone(val []byte) bool {
	return bytes.Equal(val, Tombstone)
}

// findGreaterOrEqual finds the first node with key >= target.
// Returns the node offset, or nullOffset if no such node exists.
func (s *SkipList) findGreaterOrEqual(target []byte) uint32 {
	cur := s.headOff
	h := s.Height()

	for i := h - 1; i >= 0; i-- {
		for {
			next := s.arena.getForward(cur, i)
			if next == nullOffset {
				break
			}
			if bytes.Compare(s.arena.getKey(next), target) >= 0 {
				break
			}
			cur = next
		}
	}
	// cur is the predecessor at level 0. The next at level 0 is the answer.
	return s.arena.getForward(cur, 0)
}

// findLessThan finds the last node with key < target.
// Returns the node offset, or nullOffset if no such node exists.
func (s *SkipList) findLessThan(target []byte) uint32 {
	cur := s.headOff
	h := s.Height()

	for i := h - 1; i >= 0; i-- {
		for {
			next := s.arena.getForward(cur, i)
			if next == nullOffset {
				break
			}
			if bytes.Compare(s.arena.getKey(next), target) >= 0 {
				break
			}
			cur = next
		}
	}
	if cur == s.headOff {
		return nullOffset
	}
	return cur
}

// lastNode returns the offset of the last node in the list, or nullOffset if empty.
func (s *SkipList) lastNode() uint32 {
	cur := s.headOff
	h := s.Height()

	for i := h - 1; i >= 0; i-- {
		for {
			next := s.arena.getForward(cur, i)
			if next == nullOffset {
				break
			}
			cur = next
		}
	}
	if cur == s.headOff {
		return nullOffset
	}
	return cur
}

// firstNode returns the offset of the first real node, or nullOffset if empty.
func (s *SkipList) firstNode() uint32 {
	return s.arena.getForward(s.headOff, 0)
}
