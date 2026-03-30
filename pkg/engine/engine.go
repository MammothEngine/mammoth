package engine

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mammothengine/mammoth/pkg/engine/cache"
	"github.com/mammothengine/mammoth/pkg/engine/compaction"
	"github.com/mammothengine/mammoth/pkg/engine/compression"
	"github.com/mammothengine/mammoth/pkg/engine/manifest"
	"github.com/mammothengine/mammoth/pkg/engine/memtable"
	"github.com/mammothengine/mammoth/pkg/engine/sstable"
	"github.com/mammothengine/mammoth/pkg/engine/wal"
)

const (
	tombstoneMarker = "\x00\x00\x00\x00TOMB"
	defaultArenaSize = 4 * 1024 * 1024 // 4MB
)


// Engine is the main LSM-tree storage engine.
type Engine struct {
	mu            sync.RWMutex
	opts          Options
	walLog        *wal.WAL
	mft           *manifest.Manifest
	compactor     *compaction.Compactor
	blockCache    cache.Cache
	mmgr          *memtable.MemtableManager
	readers       map[uint64]*sstable.Reader
	seqNum        atomic.Uint64
	nextFile      atomic.Uint64
	closed        atomic.Bool
	compactionDone chan struct{}
	putCount      atomic.Uint64
	getCount      atomic.Uint64
	deleteCount   atomic.Uint64
	scanCount     atomic.Uint64
	compactionCount atomic.Uint64
}

// Open opens or creates a storage engine.
func Open(opts Options) (*Engine, error) {
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("engine: mkdir: %w", err)
	}

	// Open WAL
	walDir := filepath.Join(opts.Dir, "wal")
	walOpts := wal.DefaultOptions(walDir)
	walOpts.SyncMode = opts.WALSyncMode
	if opts.WALMaxSegmentSize > 0 {
		walOpts.MaxSegmentSize = opts.WALMaxSegmentSize
	}
	w, err := wal.Open(walOpts)
	if err != nil {
		return nil, fmt.Errorf("engine: open wal: %w", err)
	}

	// Open manifest
	m, err := manifest.Open(opts.Dir)
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("engine: open manifest: %w", err)
	}

	e := &Engine{
		opts:       opts,
		walLog:     w,
		mft:        m,
		blockCache: opts.cache(),
		mmgr:       memtable.NewMemtableManager(defaultArenaSize),
		readers:    make(map[uint64]*sstable.Reader),
	}
	e.nextFile.Store(1)

	if opts.MemtableSize > 0 {
		e.mmgr.SetRotateSize(int64(opts.MemtableSize))
	}

	// Replay WAL
	if err := e.recover(); err != nil {
		w.Close()
		m.Close()
		return nil, fmt.Errorf("engine: recover: %w", err)
	}

	// Open existing SSTable readers
	if err := e.openReaders(); err != nil {
		w.Close()
		m.Close()
		return nil, fmt.Errorf("engine: open readers: %w", err)
	}

	e.compactor = compaction.NewCompactor(opts.Dir, m, e.nextFile.Load(), opts.Compression)
	e.compactionDone = make(chan struct{})
	go e.compactionLoop()
	return e, nil
}

// Put stores a key-value pair.
func (e *Engine) Put(key, value []byte) error {
	if e.closed.Load() {
		return errEngineClosed
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	seq := e.seqNum.Add(1)
	if _, err := e.walLog.WALWrite(makeWALEntry(key, value, false)); err != nil {
		return fmt.Errorf("engine: wal write: %w", err)
	}
	e.mmgr.ActiveMemtable().Put(key, value, seq)
	e.putCount.Add(1)
	e.maybeFlush()
	return nil
}

// Get retrieves a value by key.
func (e *Engine) Get(key []byte) ([]byte, error) {
	if e.closed.Load() {
		return nil, errEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	e.getCount.Add(1)

	// Check active memtable
	if val, ok := e.mmgr.ActiveMemtable().Get(key); ok {
		if string(val) == tombstoneMarker {
			return nil, errKeyNotFound
		}
		return val, nil
	}

	// Check immutable memtables
	for _, mt := range e.mmgr.ImmutableMemtables() {
		if val, ok := mt.Get(key); ok {
			if string(val) == tombstoneMarker {
				return nil, errKeyNotFound
			}
			return val, nil
		}
	}

	// Check SSTables via manifest
	v := e.mft.CurrentVersion()
	for level := 0; level < 7; level++ {
		for i := len(v.Files(level)) - 1; i >= 0; i-- {
			f := v.Files(level)[i]
			r, ok := e.readers[f.FileNum]
			if !ok {
				continue
			}
			if !r.MayContain(key) {
				continue
			}
			val, err := r.Get(key)
			if err == nil {
				if string(val) == tombstoneMarker {
					return nil, errKeyNotFound
				}
				return val, nil
			}
		}
	}

	return nil, errKeyNotFound
}

// Delete removes a key.
func (e *Engine) Delete(key []byte) error {
	if e.closed.Load() {
		return errEngineClosed
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	seq := e.seqNum.Add(1)
	if _, err := e.walLog.WALWrite(makeWALEntry(key, nil, true)); err != nil {
		return fmt.Errorf("engine: wal write: %w", err)
	}
	e.mmgr.ActiveMemtable().Put(key, []byte(tombstoneMarker), seq)
	e.deleteCount.Add(1)
	e.maybeFlush()
	return nil
}

// EngineStats holds engine statistics.
type EngineStats struct {
	MemtableCount     int
	MemtableSizeBytes int64
	SSTableCount      int
	SSTableTotalBytes uint64
	WALSegments       int
	CompactionCount   uint64
	SequenceNumber    uint64
	PutCount          uint64
	GetCount          uint64
	DeleteCount       uint64
	ScanCount         uint64
}

// Stats returns a snapshot of engine statistics.
func (e *Engine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var memSize int64
	memCount := 1
	if mt := e.mmgr.ActiveMemtable(); mt != nil {
		memSize = mt.ApproximateSize()
	}
	memCount += e.mmgr.ImmutableCount()

	v := e.mft.CurrentVersion()
	var sstCount int
	var sstBytes uint64
	for level := 0; level < 7; level++ {
		for _, f := range v.Files(level) {
			sstCount++
			sstBytes += f.Size
		}
	}

	return EngineStats{
		MemtableCount:     memCount,
		MemtableSizeBytes: memSize,
		SSTableCount:      sstCount,
		SSTableTotalBytes: sstBytes,
		CompactionCount:   e.compactionCount.Load(),
		SequenceNumber:    e.seqNum.Load(),
		PutCount:          e.putCount.Load(),
		GetCount:          e.getCount.Load(),
		DeleteCount:       e.deleteCount.Load(),
		ScanCount:         e.scanCount.Load(),
	}
}

// Flush forces memtable to disk.
func (e *Engine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.flushMemtable()
}

// Close closes the engine.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Stop background compaction loop
	if e.compactionDone != nil {
		close(e.compactionDone)
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	_ = e.flushMemtable()
	for _, r := range e.readers {
		r.Close()
	}
	if e.walLog != nil {
		e.walLog.Close()
	}
	if e.mft != nil {
		e.mft.Close()
	}
	return nil
}

// NewBatch creates a write batch.
func (e *Engine) NewBatch() *Batch {
	return &Batch{engine: e}
}

// NewSnapshot creates a point-in-time snapshot.
func (e *Engine) NewSnapshot() *Snapshot {
	return &Snapshot{engine: e, seqNum: e.seqNum.Load()}
}

// --- internal ---

func (e *Engine) maybeFlush() {
	mt := e.mmgr.ActiveMemtable()
	if mt != nil && mt.ApproximateSize() >= int64(e.opts.MemtableSize) {
		_ = e.flushMemtable()
	}
}

func (e *Engine) flushMemtable() error {
	mt := e.mmgr.ActiveMemtable()
	if mt == nil || mt.ApproximateSize() == 0 {
		return nil
	}

	fileNum := e.nextFile.Add(1)
	path := filepath.Join(e.opts.Dir, fmt.Sprintf("%06d.sst", fileNum))

	comp := e.opts.Compression
	if comp == 0 {
		comp = compression.CompressionNone
	}

	w, err := sstable.NewWriter(sstable.WriterOptions{
		Path:        path,
		BlockSize:   e.opts.BlockSize,
		Compression: comp,
		ExpectedKeys: 10000,
	})
	if err != nil {
		return fmt.Errorf("engine: create writer: %w", err)
	}

	// Get skiplist iterator from memtable
	it := mt.NewIterator()
	it.SeekToFirst()

	var smallestKey, largestKey []byte
	for it.Valid() {
		key := it.Key()
		value := it.Value()
		if smallestKey == nil {
			smallestKey = append([]byte{}, key...)
		}
		largestKey = append([]byte{}, key...)
		if err := w.Add(key, value); err != nil {
			w.Close()
			return err
		}
		it.Next()
	}

	if smallestKey == nil {
		w.Close()
		return nil
	}

	size, err := w.Finish()
	if err != nil {
		return fmt.Errorf("engine: finish sstable: %w", err)
	}

	// Log to manifest
	if err := e.mft.LogEdit(manifest.ManifestEdit{
		Type:        manifest.EditAddFile,
		Level:       0,
		FileNum:     fileNum,
		FileSize:    uint64(size),
		SmallestKey: smallestKey,
		LargestKey:  largestKey,
	}); err != nil {
		return err
	}

	// Open reader
	r, err := sstable.NewReader(path, sstable.ReaderOptions{
		Cache:       e.blockCache,
		Compression: comp,
	})
	if err == nil {
		e.readers[fileNum] = r
	}

	// Rotate
	old := e.mmgr.Rotate()
	e.mmgr.RemoveImmutable(old)

	// Run compaction synchronously and refresh readers
	if err := e.compactor.MaybeCompact(); err != nil {
		// Compaction failure is non-fatal; data is still in SSTables
		return nil
	}
	e.refreshReaders()
	return nil
}

func (e *Engine) recover() error {
	records, err := wal.Replay(filepath.Join(e.opts.Dir, "wal"))
	if err != nil {
		return err
	}
	for _, rec := range records {
		key, value, isDelete := parseWALEntry(rec.Payload)
		if isDelete {
			e.mmgr.ActiveMemtable().Put(key, []byte(tombstoneMarker), rec.SeqNum)
		} else {
			e.mmgr.ActiveMemtable().Put(key, value, rec.SeqNum)
		}
		if rec.SeqNum > e.seqNum.Load() {
			e.seqNum.Store(rec.SeqNum)
		}
	}
	return nil
}

// refreshReaders updates the readers map to reflect the current manifest version.
func (e *Engine) refreshReaders() {
	comp := e.opts.Compression
	if comp == 0 {
		comp = compression.CompressionNone
	}

	v := e.mft.CurrentVersion()
	activeFiles := make(map[uint64]bool)
	for level := 0; level < 7; level++ {
		for _, f := range v.Files(level) {
			activeFiles[f.FileNum] = true
			if _, ok := e.readers[f.FileNum]; !ok {
				path := filepath.Join(e.opts.Dir, fmt.Sprintf("%06d.sst", f.FileNum))
				if r, err := sstable.NewReader(path, sstable.ReaderOptions{
					Cache:       e.blockCache,
					Compression: comp,
				}); err == nil {
					e.readers[f.FileNum] = r
				}
			}
			if f.FileNum >= e.nextFile.Load() {
				e.nextFile.Store(f.FileNum + 1)
			}
		}
	}

	// Close and remove readers for files no longer in manifest
	for fileNum, r := range e.readers {
		if !activeFiles[fileNum] {
			r.Close()
			delete(e.readers, fileNum)
		}
	}
}

func (e *Engine) openReaders() error {
	v := e.mft.CurrentVersion()
	for level := 0; level < 7; level++ {
		for _, f := range v.Files(level) {
			path := filepath.Join(e.opts.Dir, fmt.Sprintf("%06d.sst", f.FileNum))
			if _, err := os.Stat(path); os.IsNotExist(err) {
				continue
			}
			r, err := sstable.NewReader(path, sstable.ReaderOptions{
				Cache:       e.blockCache,
				Compression: e.opts.Compression,
			})
			if err != nil {
				continue
			}
			e.readers[f.FileNum] = r
			if f.FileNum >= e.nextFile.Load() {
				e.nextFile.Store(f.FileNum + 1)
			}
		}
	}
	return nil
}

func (e *Engine) applyBatch(b *Batch) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	payloads := make([][]byte, len(b.writes))
	for i, w := range b.writes {
		payloads[i] = makeWALEntry(w.key, w.value, w.delete)
	}
	if _, err := e.walLog.WriteBatch(payloads); err != nil {
		return err
	}

	baseSeq := e.seqNum.Add(uint64(len(b.writes)))
	for i, w := range b.writes {
		seq := baseSeq - uint64(len(b.writes)-1-i)
		val := w.value
		if w.delete {
			val = []byte(tombstoneMarker)
		}
		e.mmgr.ActiveMemtable().Put(w.key, val, seq)
	}
	e.maybeFlush()
	return nil
}

// WAL entry format: delete_flag(1) + keyLen(4) + key + valueLen(4) + value
func makeWALEntry(key, value []byte, isDelete bool) []byte {
	deleteFlag := byte(0)
	if isDelete {
		deleteFlag = 1
	}
	keyLen := len(key)
	vLen := 0
	if value != nil {
		vLen = len(value)
	}
	buf := make([]byte, 1+4+keyLen+4+vLen)
	buf[0] = deleteFlag
	binary.LittleEndian.PutUint32(buf[1:], uint32(keyLen))
	copy(buf[5:], key)
	off := 5 + keyLen
	binary.LittleEndian.PutUint32(buf[off:], uint32(vLen))
	if value != nil {
		copy(buf[off+4:], value)
	}
	return buf
}

func parseWALEntry(data []byte) (key, value []byte, isDelete bool) {
	if len(data) < 5 {
		return nil, nil, false
	}
	isDelete = data[0] == 1
	keyLen := int(binary.LittleEndian.Uint32(data[1:5]))
	if len(data) < 5+keyLen+4 {
		return nil, nil, false
	}
	key = data[5 : 5+keyLen]
	off := 5 + keyLen
	vLen := int(binary.LittleEndian.Uint32(data[off : off+4]))
	if len(data) < off+4+vLen {
		return key, nil, isDelete
	}
	value = data[off+4 : off+4+vLen]
	return key, value, isDelete
}

// getAtSeqNum reads a key at a specific sequence number (snapshot read).
func (e *Engine) getAtSeqNum(key []byte, seqNum uint64) ([]byte, error) {
	if e.closed.Load() {
		return nil, errEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Check active memtable
	if val, ok := e.mmgr.ActiveMemtable().Get(key); ok {
		if string(val) == tombstoneMarker {
			return nil, errKeyNotFound
		}
		return val, nil
	}

	// Check immutable memtables
	for _, mt := range e.mmgr.ImmutableMemtables() {
		if val, ok := mt.Get(key); ok {
			if string(val) == tombstoneMarker {
				return nil, errKeyNotFound
			}
			return val, nil
		}
	}

	// Check SSTables
	v := e.mft.CurrentVersion()
	for level := 0; level < 7; level++ {
		for i := len(v.Files(level)) - 1; i >= 0; i-- {
			f := v.Files(level)[i]
			r, ok := e.readers[f.FileNum]
			if !ok {
				continue
			}
			if !r.MayContain(key) {
				continue
			}
			val, err := r.Get(key)
			if err == nil {
				if string(val) == tombstoneMarker {
					return nil, errKeyNotFound
				}
				return val, nil
			}
		}
	}

	return nil, errKeyNotFound
}

// releaseSnapshot is a no-op for now (future: notify compaction of safe seqnums).
func (e *Engine) releaseSnapshot(s *Snapshot) {}

// compactionLoop runs periodic background compaction.
func (e *Engine) compactionLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.compactionDone:
			return
		case <-ticker.C:
			e.maybeCompact()
		}
	}
}

// maybeCompact attempts background compaction.
func (e *Engine) maybeCompact() {
	if e.closed.Load() || e.compactor == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.compactor.MaybeCompact(); err == nil {
		e.compactionCount.Add(1)
		e.refreshReaders()
	}
}

// MaybeCompact is the public API for triggering compaction.
func (e *Engine) MaybeCompact() error {
	if e.closed.Load() || e.compactor == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	err := e.compactor.MaybeCompact()
	if err == nil {
		e.refreshReaders()
	}
	return err
}
