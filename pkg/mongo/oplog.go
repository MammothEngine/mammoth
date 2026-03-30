package mongo

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// OplogEntry represents a single operation in the oplog.
type OplogEntry struct {
	Timestamp int64
	Counter   int64
	Operation string // "i", "u", "d"
	Namespace string // "db.collection"
	Document  []byte // BSON-encoded document data
	Document2 []byte // BSON-encoded old document (for updates)
	WallTime  int64
}

// Oplog writes and reads operation log entries to/from the engine KV store.
type Oplog struct {
	eng     *engine.Engine
	counter atomic.Int64
}

// NewOplog creates a new Oplog backed by the engine.
func NewOplog(eng *engine.Engine) *Oplog {
	return &Oplog{eng: eng}
}

// oplogKeyPrefix is the namespace prefix for oplog entries.
var oplogKeyPrefix = []byte("\x00oplog")

func oplogKey(ts int64, counter int64) []byte {
	key := make([]byte, 0, len(oplogKeyPrefix)+16)
	key = append(key, oplogKeyPrefix...)
	key = binary.BigEndian.AppendUint64(key, uint64(ts))
	key = binary.BigEndian.AppendUint64(key, uint64(counter))
	return key
}

// WriteInsert writes an insert oplog entry.
func (o *Oplog) WriteInsert(ns string, docData []byte) error {
	return o.writeEntry("i", ns, docData, nil)
}

// WriteUpdate writes an update oplog entry.
func (o *Oplog) WriteUpdate(ns string, newDocData []byte, oldDocData []byte) error {
	return o.writeEntry("u", ns, newDocData, oldDocData)
}

// WriteDelete writes a delete oplog entry.
func (o *Oplog) WriteDelete(ns string, docData []byte) error {
	return o.writeEntry("d", ns, docData, nil)
}

func (o *Oplog) writeEntry(op, ns string, docData, doc2Data []byte) error {
	ts := time.Now().Unix()
	c := o.counter.Add(1)
	key := oplogKey(ts, c)

	// Encode entry as BSON document
	entryDoc := bson.NewDocument()
	entryDoc.Set("ts", bson.VInt64(ts))
	entryDoc.Set("c", bson.VInt64(c))
	entryDoc.Set("op", bson.VString(op))
	entryDoc.Set("ns", bson.VString(ns))
	if docData != nil {
		entryDoc.Set("o", bson.VBinary(bson.BinaryGeneric, docData))
	}
	if doc2Data != nil {
		entryDoc.Set("o2", bson.VBinary(bson.BinaryGeneric, doc2Data))
	}
	entryDoc.Set("wall", bson.VInt64(time.Now().UnixMilli()))

	val := bson.Encode(entryDoc)
	return o.eng.Put(key, val)
}

// decodeOplogEntry decodes a BSON-encoded oplog entry.
func decodeOplogEntry(data []byte) (OplogEntry, error) {
	doc, err := bson.Decode(data)
	if err != nil {
		return OplogEntry{}, err
	}
	entry := OplogEntry{}
	if v, ok := doc.Get("ts"); ok && v.Type == bson.TypeInt64 {
		entry.Timestamp = v.Int64()
	}
	if v, ok := doc.Get("c"); ok && v.Type == bson.TypeInt64 {
		entry.Counter = v.Int64()
	}
	if v, ok := doc.Get("op"); ok && v.Type == bson.TypeString {
		entry.Operation = v.String()
	}
	if v, ok := doc.Get("ns"); ok && v.Type == bson.TypeString {
		entry.Namespace = v.String()
	}
	if v, ok := doc.Get("o"); ok && v.Type == bson.TypeBinary {
		entry.Document = v.Binary().Data
	}
	if v, ok := doc.Get("o2"); ok && v.Type == bson.TypeBinary {
		entry.Document2 = v.Binary().Data
	}
	if v, ok := doc.Get("wall"); ok && v.Type == bson.TypeInt64 {
		entry.WallTime = v.Int64()
	}
	return entry, nil
}

// OplogScanner reads oplog entries since a given timestamp.
type OplogScanner struct {
	eng *engine.Engine
}

// NewOplogScanner creates a scanner for reading oplog entries.
func NewOplogScanner(eng *engine.Engine) *OplogScanner {
	return &OplogScanner{eng: eng}
}

// ScanSince reads all oplog entries with timestamp >= sinceTS.
func (s *OplogScanner) ScanSince(sinceTS int64, fn func(entry OplogEntry) bool) error {
	return s.eng.Scan(oplogKeyPrefix, func(key, value []byte) bool {
		if len(key) < len(oplogKeyPrefix) || string(key[:len(oplogKeyPrefix)]) != string(oplogKeyPrefix) {
			return false
		}
		entry, err := decodeOplogEntry(value)
		if err != nil {
			return true
		}
		if entry.Timestamp < sinceTS {
			return true
		}
		return fn(entry)
	})
}

// ScanAll reads all oplog entries.
func (s *OplogScanner) ScanAll(fn func(entry OplogEntry) bool) error {
	return s.eng.Scan(oplogKeyPrefix, func(key, value []byte) bool {
		if len(key) < len(oplogKeyPrefix) || string(key[:len(oplogKeyPrefix)]) != string(oplogKeyPrefix) {
			return false
		}
		entry, err := decodeOplogEntry(value)
		if err != nil {
			return true
		}
		return fn(entry)
	})
}

// ChangeStreamManager manages active change stream watchers.
type ChangeStreamManager struct {
	mu       sync.RWMutex
	watchers map[uint64]*ChangeWatcher
	nextID   atomic.Uint64
	oplog    *OplogScanner
}

// ChangeWatcher represents an active change stream subscription.
type ChangeWatcher struct {
	ID        uint64
	Namespace string // "db.coll" or empty for all
	ResumeTS  int64  // resume from this timestamp
	Ch        chan OplogEntry
	Done      chan struct{}
}

// NewChangeStreamManager creates a new change stream manager.
func NewChangeStreamManager(eng *engine.Engine) *ChangeStreamManager {
	return &ChangeStreamManager{
		watchers: make(map[uint64]*ChangeWatcher),
		oplog:    NewOplogScanner(eng),
	}
}

// Watch creates a new change stream watcher for a namespace.
func (m *ChangeStreamManager) Watch(ns string, resumeTS int64) *ChangeWatcher {
	id := m.nextID.Add(1)
	w := &ChangeWatcher{
		ID:        id,
		Namespace: ns,
		ResumeTS:  resumeTS,
		Ch:        make(chan OplogEntry, 256),
		Done:      make(chan struct{}),
	}
	m.mu.Lock()
	m.watchers[id] = w
	m.mu.Unlock()
	return w
}

// Notify is called when a new oplog entry is written.
func (m *ChangeStreamManager) Notify(entry OplogEntry) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, w := range m.watchers {
		if w.Namespace != "" && w.Namespace != entry.Namespace {
			continue
		}
		select {
		case w.Ch <- entry:
		default:
			// Drop if channel is full
		}
	}
}

// Remove removes a watcher.
func (m *ChangeStreamManager) Remove(id uint64) {
	m.mu.Lock()
	w, ok := m.watchers[id]
	if ok {
		delete(m.watchers, id)
		close(w.Done)
	}
	m.mu.Unlock()
}

// Poll reads recent oplog entries and returns them for a watcher.
func (m *ChangeStreamManager) Poll(watcher *ChangeWatcher) []OplogEntry {
	var entries []OplogEntry
	sinceTS := watcher.ResumeTS
	if sinceTS > 0 {
		sinceTS++ // Start after the resume point
	}
	m.oplog.ScanSince(sinceTS, func(entry OplogEntry) bool {
		if watcher.Namespace != "" && watcher.Namespace != entry.Namespace {
			return true
		}
		entries = append(entries, entry)
		return true
	})
	if len(entries) > 0 {
		watcher.ResumeTS = entries[len(entries)-1].Timestamp
	}
	return entries
}
