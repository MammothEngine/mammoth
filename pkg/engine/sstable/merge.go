package sstable

import "container/heap"

// MergeIterator performs a k-way merge over multiple SSTable iterators.
// In case of duplicate keys, the iterator from the earlier position in the
// list takes priority (newer data).
type MergeIterator struct {
	iters []*Iterator
	h     mergeHeap
	valid bool
}

type mergeItem struct {
	iter  *Iterator
	index int
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	c := compareBytes(h[i].iter.Key(), h[j].iter.Key())
	if c == 0 {
		return h[i].index < h[j].index // Earlier iterator has priority
	}
	return c < 0
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// NewMergeIterator creates a k-way merge iterator.
func NewMergeIterator(iters []*Iterator) *MergeIterator {
	m := &MergeIterator{iters: iters}
	m.h = make(mergeHeap, 0)

	for i, it := range iters {
		it.SeekToFirst()
		if it.Valid() {
			heap.Push(&m.h, mergeItem{iter: it, index: i})
		}
	}

	m.valid = len(m.h) > 0
	return m
}

// SeekToFirst positions at the first entry across all iterators.
func (m *MergeIterator) SeekToFirst() {
	m.h = m.h[:0]
	for i, it := range m.iters {
		it.SeekToFirst()
		if it.Valid() {
			heap.Push(&m.h, mergeItem{iter: it, index: i})
		}
	}
	m.valid = len(m.h) > 0
}

// Next advances to the next unique key.
func (m *MergeIterator) Next() {
	if !m.valid {
		return
	}

	// Get current key
	current := m.h[0].iter.Key()

	// Advance all iterators that have the current key
	for len(m.h) > 0 && compareBytes(m.h[0].iter.Key(), current) == 0 {
		item := heap.Pop(&m.h).(mergeItem)
		item.iter.Next()
		if item.iter.Valid() {
			heap.Push(&m.h, item)
		}
	}

	m.valid = len(m.h) > 0
}

// Valid returns whether the iterator is positioned at a valid entry.
func (m *MergeIterator) Valid() bool {
	return m.valid
}

// Key returns the current key.
func (m *MergeIterator) Key() []byte {
	if !m.valid || len(m.h) == 0 {
		return nil
	}
	return m.h[0].iter.Key()
}

// Value returns the value from the highest-priority iterator.
func (m *MergeIterator) Value() []byte {
	if !m.valid || len(m.h) == 0 {
		return nil
	}
	return m.h[0].iter.Value()
}
