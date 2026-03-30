package engine

import (
	"sync/atomic"
)

// Snapshot provides a consistent view of the database at a point in time.
type Snapshot struct {
	engine   *Engine
	seqNum   uint64
	released atomic.Bool
}

// Get retrieves a key from the snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if s.released.Load() {
		return nil, errSnapshotReleased
	}
	return s.engine.getAtSeqNum(key, s.seqNum)
}

// Scan iterates over all key-value pairs whose keys start with prefix,
// filtered to only include entries visible at the snapshot's sequence number.
// Keys are visited in sorted order. Tombstones are skipped.
func (s *Snapshot) Scan(prefix []byte, fn func(key, value []byte) bool) error {
	if s.released.Load() {
		return errSnapshotReleased
	}
	return s.engine.snapshotScan(prefix, s.seqNum, fn)
}

// SeqNum returns the sequence number of this snapshot.
func (s *Snapshot) SeqNum() uint64 { return s.seqNum }

// Release releases the snapshot.
func (s *Snapshot) Release() {
	if s.released.CompareAndSwap(false, true) {
		s.engine.releaseSnapshot(s)
	}
}
