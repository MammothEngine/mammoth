package engine

import (
	"sync/atomic"
)

// Snapshot provides a consistent view of the database at a point in time.
type Snapshot struct {
	engine  *Engine
	seqNum  uint64
	released atomic.Bool
}

// Get retrieves a key from the snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if s.released.Load() {
		return nil, errSnapshotReleased
	}
	return s.engine.getAtSeqNum(key, s.seqNum)
}

// Release releases the snapshot.
func (s *Snapshot) Release() {
	if s.released.CompareAndSwap(false, true) {
		s.engine.releaseSnapshot(s)
	}
}
