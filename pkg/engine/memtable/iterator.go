package memtable

import (
	"bytes"
)

// Iterator provides forward and backward traversal over a skip list.
// It is not safe for concurrent use; create one per goroutine.
//
// An iterator is invalidated by mutations to the skip list.
type Iterator struct {
	list    *SkipList
	arena   *Arena
	current uint32 // offset of the current node (nullOffset = invalid)
}

// NewIterator creates a new iterator positioned before the first entry.
func (s *SkipList) NewIterator() *Iterator {
	return &Iterator{
		list:    s,
		arena:   s.arena,
		current: nullOffset,
	}
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *Iterator) Valid() bool {
	return it.current != nullOffset
}

// Key returns the key at the current position. Panics if !Valid().
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		panic("memtable: iterator key called when not valid")
	}
	return it.arena.getKey(it.current)
}

// Value returns the value at the current position. Panics if !Valid().
// Returns nil if the entry is a tombstone.
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		panic("memtable: iterator value called when not valid")
	}
	val := it.arena.getValue(it.current)
	if isTombstone(val) {
		return nil
	}
	return val
}

// SeekToFirst positions the iterator at the first entry in the list.
func (it *Iterator) SeekToFirst() {
	it.current = it.list.firstNode()
}

// SeekToLast positions the iterator at the last entry in the list.
func (it *Iterator) SeekToLast() {
	it.current = it.list.lastNode()
}

// Seek positions the iterator at the first key >= target.
// If no such key exists, the iterator becomes invalid.
func (it *Iterator) Seek(key []byte) {
	it.current = it.list.findGreaterOrEqual(key)
}

// Next moves the iterator to the next entry in sorted order.
// Panics if !Valid().
func (it *Iterator) Next() {
	if !it.Valid() {
		panic("memtable: iterator next called when not valid")
	}
	it.current = it.arena.getForward(it.current, 0)
}

// Prev moves the iterator to the previous entry in sorted order.
// This uses a search from the head to find the predecessor, which is O(log n).
// Panics if !Valid().
func (it *Iterator) Prev() {
	if !it.Valid() {
		panic("memtable: iterator prev called when not valid")
	}
	curKey := it.arena.getKey(it.current)
	it.current = it.list.findLessThan(curKey)
}

// SeekForPrev positions the iterator at the last key <= target.
// If no such key exists, the iterator becomes invalid.
func (it *Iterator) SeekForPrev(target []byte) {
	cur := it.list.headOff
	h := it.list.Height()

	for i := h - 1; i >= 0; i-- {
		for {
			next := it.arena.getForward(cur, i)
			if next == nullOffset {
				break
			}
			if bytes.Compare(it.arena.getKey(next), target) > 0 {
				break
			}
			cur = next
		}
	}

	if cur == it.list.headOff {
		it.current = nullOffset
	} else {
		it.current = cur
	}
}
