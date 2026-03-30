package memtable

import (
	"sync"
	"sync/atomic"
)

const (
	// DefaultMemtableSize is the default arena size for a memtable (4 MB).
	DefaultMemtableSize = 4 << 20

	// DefaultRotateSize is the default size threshold (in bytes) that triggers
	// a memtable rotation.
	DefaultRotateSize = 2 << 20 // 2 MB
)

// Memtable wraps a skip list with WAL sequence numbers and read-only semantics.
// It represents an in-memory sorted table that receives writes before they are
// flushed to SSTables.
type Memtable struct {
	sl     *SkipList
	seqNum atomic.Uint64
	size   atomic.Int64 // approximate byte count of data written
	ro     atomic.Bool  // read-only flag
}

// NewMemtable creates a new writable memtable with the given arena size.
func NewMemtable(arenaSize int) *Memtable {
	return &Memtable{
		sl: NewSkipList(arenaSize),
	}
}

// Put inserts a key-value pair with the given sequence number.
// The sequence number is stored only if it is greater than the current one.
func (m *Memtable) Put(key, value []byte, seqNum uint64) {
	if m.ro.Load() {
		panic("memtable: put on read-only memtable")
	}
	m.sl.Put(key, value)
	m.size.Add(int64(len(key) + len(value)))

	// Track the highest sequence number seen.
	for {
		cur := m.seqNum.Load()
		if seqNum <= cur {
			break
		}
		if m.seqNum.CompareAndSwap(cur, seqNum) {
			break
		}
	}
}

// Get looks up a key and returns (value, true) if found and not tombstoned.
func (m *Memtable) Get(key []byte) ([]byte, bool) {
	return m.sl.Get(key)
}

// Delete inserts a tombstone for the given key with the given sequence number.
func (m *Memtable) Delete(key []byte, seqNum uint64) {
	if m.ro.Load() {
		panic("memtable: delete on read-only memtable")
	}
	m.sl.Delete(key)
	m.size.Add(int64(len(key)))

	for {
		cur := m.seqNum.Load()
		if seqNum <= cur {
			break
		}
		if m.seqNum.CompareAndSwap(cur, seqNum) {
			break
		}
	}
}

// NewIterator returns an iterator over the memtable's skip list.
func (m *Memtable) NewIterator() *Iterator {
	return m.sl.NewIterator()
}

// ApproximateSize returns an estimate of the memtable's data size in bytes.
func (m *Memtable) ApproximateSize() int64 {
	return m.size.Load()
}

// SeqNum returns the highest sequence number written to this memtable.
func (m *Memtable) SeqNum() uint64 {
	return m.seqNum.Load()
}

// SetReadOnly marks this memtable as immutable. After this call, Put and Delete
// will panic. This is called when the memtable is rotated for flush.
func (m *Memtable) SetReadOnly() {
	m.ro.Store(true)
}

// IsReadOnly returns whether this memtable is immutable.
func (m *Memtable) IsReadOnly() bool {
	return m.ro.Load()
}

// SkipList returns the underlying skip list (for advanced use).
func (m *Memtable) SkipList() *SkipList {
	return m.sl
}

// MemtableManager manages the lifecycle of active and immutable memtables.
// It coordinates the rotation from active (writable) to immutable (flush-pending).
type MemtableManager struct {
	mu          sync.Mutex
	active      *Memtable
	immutable   []*Memtable
	arenaSize   int
	rotateSize  int64
}

// NewMemtableManager creates a new manager with the given per-memtable arena size.
func NewMemtableManager(arenaSize int) *MemtableManager {
	m := &MemtableManager{
		arenaSize:  arenaSize,
		rotateSize: int64(DefaultRotateSize),
	}
	m.active = NewMemtable(arenaSize)
	return m
}

// SetRotateSize sets the approximate size threshold that triggers rotation.
func (m *MemtableManager) SetRotateSize(size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rotateSize = size
}

// ActiveMemtable returns the current writable memtable.
func (m *MemtableManager) ActiveMemtable() *Memtable {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active
}

// Rotate marks the active memtable as read-only, moves it to the immutable
// queue, and creates a fresh active memtable. Returns the now-immutable memtable.
func (m *MemtableManager) Rotate() *Memtable {
	m.mu.Lock()
	defer m.mu.Unlock()

	old := m.active
	old.SetReadOnly()
	m.immutable = append(m.immutable, old)
	m.active = NewMemtable(m.arenaSize)
	return old
}

// MaybeRotate checks if the active memtable has exceeded the rotation threshold,
// and if so, performs a rotation. Returns the rotated memtable and true if a
// rotation occurred, or nil and false otherwise.
func (m *MemtableManager) MaybeRotate() (*Memtable, bool) {
	m.mu.Lock()
	active := m.active
	threshold := m.rotateSize
	m.mu.Unlock()

	if active.ApproximateSize() >= threshold {
		rotated := m.Rotate()
		return rotated, true
	}
	return nil, false
}

// ImmutableMemtables returns a snapshot of all immutable memtables awaiting flush.
func (m *MemtableManager) ImmutableMemtables() []*Memtable {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*Memtable, len(m.immutable))
	copy(result, m.immutable)
	return result
}

// RemoveImmutable removes a memtable from the immutable queue after it has
// been successfully flushed to disk.
func (m *MemtableManager) RemoveImmutable(target *Memtable) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, mt := range m.immutable {
		if mt == target {
			m.immutable = append(m.immutable[:i], m.immutable[i+1:]...)
			return
		}
	}
}

// ImmutableCount returns the number of immutable memtables awaiting flush.
func (m *MemtableManager) ImmutableCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.immutable)
}
