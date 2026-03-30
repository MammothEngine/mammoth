package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// SyncMode controls how aggressively WAL data is flushed to disk.
type SyncMode int

const (
	SyncNone  SyncMode = iota // No sync (testing only)
	SyncFull                  // Sync after every write
	SyncBatch                 // Sync periodically
)

// Options for the WAL.
type Options struct {
	Dir           string
	MaxSegmentSize int64 // Default: 64MB
	SyncMode      SyncMode
	BatchSyncInterval time.Duration // For SyncBatch mode
}

// DefaultOptions returns sensible defaults.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:           dir,
		MaxSegmentSize: defaultMaxSegSize,
		SyncMode:      SyncFull,
		BatchSyncInterval: 10 * time.Millisecond,
	}
}

// WAL manages the write-ahead log.
type WAL struct {
	mu       sync.Mutex
	opts     Options
	dir      string
	segments []*Segment
	active   *Segment
	seqNum   atomic.Uint64
	closed   bool

	batchDone chan struct{}
	closeCh   chan struct{}
}

// Open opens or creates a WAL in the given directory.
func Open(opts Options) (*WAL, error) {
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}

	w := &WAL{
		opts:    opts,
		dir:     opts.Dir,
		batchDone: make(chan struct{}),
		closeCh:  make(chan struct{}),
	}

	// Find existing segments
	paths, err := ListSegments(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("wal: list segments: %w", err)
	}

	var maxSeq uint64
	if len(paths) > 0 {
		// Replay to find max sequence
		for _, p := range paths {
			recs, err := ReadRecords(p)
			if err != nil {
				continue
			}
			for _, r := range recs {
				if r.SeqNum > maxSeq {
					maxSeq = r.SeqNum
				}
			}
			idx, err := ParseSegmentIndex(filepath.Base(p))
			if err != nil {
				continue
			}
			seg, err := OpenSegment(p)
			if err != nil {
				continue
			}
			seg.index = idx
			w.segments = append(w.segments, seg)
		}

		// Create new segment for writing
		lastIdx := w.segments[len(w.segments)-1].index
		seg, err := CreateSegment(opts.Dir, lastIdx+1)
		if err != nil {
			return nil, err
		}
		w.active = seg
		w.segments = append(w.segments, seg)
	} else {
		seg, err := CreateSegment(opts.Dir, 1)
		if err != nil {
			return nil, err
		}
		w.active = seg
		w.segments = append(w.segments, seg)
	}

	w.seqNum.Store(maxSeq)

	if opts.SyncMode == SyncBatch {
		go w.batchSyncLoop()
	}

	return w, nil
}

// Write appends a single record to the WAL.
func (w *WAL) WALWrite(payload []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, errClosed
	}

	seq := w.seqNum.Add(1)

	rec := &Record{
		Type:    RecordFull,
		SeqNum:  seq,
		Payload: payload,
	}

	if err := w.active.WriteRecord(rec, w.opts.SyncMode == SyncFull); err != nil {
		return 0, err
	}

	// Rotate if needed
	if w.active.Size() >= w.opts.MaxSegmentSize {
		if err := w.rotate(); err != nil {
			return seq, err
		}
	}

	return seq, nil
}

// WriteBatch writes multiple records atomically with sequential sequence numbers.
func (w *WAL) WriteBatch(payloads [][]byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, errClosed
	}

	if len(payloads) == 0 {
		return w.seqNum.Load(), nil
	}

	baseSeq := w.seqNum.Add(1)
	sync := w.opts.SyncMode == SyncFull

	for i, payload := range payloads {
		rec := &Record{
			Type:    RecordFull,
			SeqNum:  baseSeq + uint64(i),
			Payload: payload,
		}
		if err := w.active.WriteRecord(rec, false); err != nil {
			return 0, err
		}
	}

	// Update sequence to last
	w.seqNum.Store(baseSeq + uint64(len(payloads)) - 1)

	// Single sync for entire batch
	if sync {
		if err := w.active.Sync(); err != nil {
			return 0, err
		}
	}

	// Rotate if needed
	if w.active.Size() >= w.opts.MaxSegmentSize {
		if err := w.rotate(); err != nil {
			return baseSeq, err
		}
	}

	return baseSeq, nil
}

// Close flushes and closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	close(w.closeCh)

	if w.active != nil {
		_ = w.active.Sync()
	}

	for _, seg := range w.segments {
		_ = seg.Close()
	}

	return nil
}

// SeqNum returns the current sequence number.
func (w *WAL) SeqNum() uint64 {
	return w.seqNum.Load()
}

// Dir returns the WAL directory.
func (w *WAL) Dir() string {
	return w.dir
}

func (w *WAL) rotate() error {
	newIdx := w.active.index + 1
	seg, err := CreateSegment(w.dir, newIdx)
	if err != nil {
		return err
	}
	w.active = seg
	w.segments = append(w.segments, seg)
	return nil
}

func (w *WAL) batchSyncLoop() {
	ticker := time.NewTicker(w.opts.BatchSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if w.active != nil && !w.closed {
				_ = w.active.Sync()
			}
			w.mu.Unlock()
		case <-w.closeCh:
			return
		}
	}
}
