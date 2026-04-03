package repl

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// OpType represents the type of oplog operation.
type OpType string

const (
	OpInsert OpType = "i"
	OpUpdate OpType = "u"
	OpDelete OpType = "d"
	OpNoop   OpType = "n"
	OpCreate OpType = "c" // Create collection/index
	OpDrop   OpType = "drop"
)

// OplogEntry represents a single operation in the oplog.
type OplogEntry struct {
	Timestamp   time.Time       `json:"ts" bson:"ts"`
	Term        int64           `json:"t" bson:"t"`       // Raft term for elections
	Hash        int64           `json:"h" bson:"h"`       // Hash of the operation
	Version     int             `json:"v" bson:"v"`       // Oplog version
	Operation   OpType          `json:"op" bson:"op"`     // Operation type
	Namespace   string          `json:"ns" bson:"ns"`     // db.collection
	Object      *bson.Document  `json:"o" bson:"o"`       // Document for insert/update
	Object2     *bson.Document  `json:"o2,omitempty" bson:"o2,omitempty"` // Query for update/delete
	WallTime    time.Time       `json:"wall" bson:"wall"` // Wall clock time
	SessionID   string          `json:"lsid,omitempty" bson:"lsid,omitempty"`
	TxnNumber   int64           `json:"txnNumber,omitempty" bson:"txnNumber,omitempty"`
}

// Oplog manages the operations log for replication.
type Oplog struct {
	mu       sync.RWMutex
	eng      EngineInterface
	prefix   []byte
	lastTS   time.Time
	lastHash int64
}

// NewOplog creates a new oplog manager.
func NewOplog(eng EngineInterface) *Oplog {
	return &Oplog{
		eng:    eng,
		prefix: []byte("__oplog__."),
	}
}

// Append adds a new entry to the oplog.
func (o *Oplog) Append(op OpType, ns string, obj, obj2 *bson.Document) (*OplogEntry, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Generate timestamp
	ts := time.Now().UTC()
	if !ts.After(o.lastTS) {
		// Ensure monotonic timestamps
		ts = o.lastTS.Add(time.Nanosecond)
	}
	o.lastTS = ts

	// Generate hash
	o.lastHash = o.lastHash + 1

	entry := &OplogEntry{
		Timestamp: ts,
		Hash:      o.lastHash,
		Version:   2,
		Operation: op,
		Namespace: ns,
		Object:    obj,
		Object2:   obj2,
		WallTime:  time.Now().UTC(),
	}

	// Serialize entry
	data, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("oplog: marshal entry: %w", err)
	}

	// Create key: prefix + timestamp bytes
	key := o.makeKey(ts, o.lastHash)

	if err := o.eng.Put(key, data); err != nil {
		return nil, fmt.Errorf("oplog: store entry: %w", err)
	}

	return entry, nil
}

// AppendEntry adds a pre-constructed entry (used during replication).
func (o *Oplog) AppendEntry(entry *OplogEntry) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	key := o.makeKey(entry.Timestamp, entry.Hash)
	return o.eng.Put(key, data)
}

// GetSince retrieves oplog entries since the given timestamp.
func (o *Oplog) GetSince(since time.Time, limit int) ([]*OplogEntry, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	// Start key is the timestamp
	startKey := append([]byte{}, o.prefix...)
	startKey = append(startKey, timeToBytes(since)...)

	var entries []*OplogEntry
	count := 0

	err := o.eng.Scan(o.prefix, func(key, value []byte) bool {
		if len(entries) >= limit && limit > 0 {
			return false
		}

		var entry OplogEntry
		if err := json.Unmarshal(value, &entry); err != nil {
			return true // Skip corrupted entries
		}

		// Skip entries at or before since time
		if !entry.Timestamp.After(since) {
			return true
		}

		entries = append(entries, &entry)
		count++
		return true
	})

	if err != nil {
		return nil, err
	}

	return entries, nil
}

// GetLatestTimestamp returns the most recent oplog timestamp.
func (o *Oplog) GetLatestTimestamp() time.Time {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.lastTS
}

// Truncate removes entries older than the given timestamp.
func (o *Oplog) Truncate(before time.Time) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	var keysToDelete [][]byte

	err := o.eng.Scan(o.prefix, func(key, value []byte) bool {
		var entry OplogEntry
		if err := json.Unmarshal(value, &entry); err != nil {
			return true
		}

		if entry.Timestamp.Before(before) {
			keysToDelete = append(keysToDelete, append([]byte{}, key...))
		}
		return true
	})

	if err != nil {
		return err
	}

	for _, key := range keysToDelete {
		if err := o.eng.Delete(key); err != nil {
			return err
		}
	}

	return nil
}

// makeKey creates an oplog key from timestamp and hash.
func (o *Oplog) makeKey(ts time.Time, hash int64) []byte {
	key := make([]byte, 0, len(o.prefix)+16)
	key = append(key, o.prefix...)
	key = append(key, timeToBytes(ts)...)
	return key
}

// timeToBytes converts time to bytes for key encoding.
func timeToBytes(t time.Time) []byte {
	// Use UnixNano for precision
	nanos := t.UnixNano()
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[7-i] = byte(nanos >> (i * 8))
	}
	return b
}

// bytesToTime converts bytes back to time.
func bytesToTime(b []byte) time.Time {
	if len(b) < 8 {
		return time.Time{}
	}
	var nanos int64
	for i := 0; i < 8; i++ {
		nanos |= int64(b[i]) << ((7 - i) * 8)
	}
	return time.Unix(0, nanos).UTC()
}

// OplogApplier applies oplog entries to the local engine.
type OplogApplier struct {
	eng EngineInterface
}

// NewOplogApplier creates a new applier.
func NewOplogApplier(eng EngineInterface) *OplogApplier {
	return &OplogApplier{eng: eng}
}

// Apply applies a single oplog entry.
func (a *OplogApplier) Apply(entry *OplogEntry) error {
	switch entry.Operation {
	case OpInsert:
		return a.applyInsert(entry)
	case OpUpdate:
		return a.applyUpdate(entry)
	case OpDelete:
		return a.applyDelete(entry)
	case OpNoop:
		return nil // No-op, used for heartbeats
	default:
		return fmt.Errorf("oplog: unsupported operation: %s", entry.Operation)
	}
}

func (a *OplogApplier) applyInsert(entry *OplogEntry) error {
	if entry.Object == nil {
		return fmt.Errorf("oplog: insert missing object")
	}

	// Extract _id
	idVal, ok := entry.Object.Get("_id")
	if !ok {
		return fmt.Errorf("oplog: insert missing _id")
	}

	// Build key
	key := makeDocumentKey(entry.Namespace, idVal)
	data := bson.Encode(entry.Object)

	return a.eng.Put(key, data)
}

func (a *OplogApplier) applyUpdate(entry *OplogEntry) error {
	if entry.Object2 == nil || entry.Object == nil {
		return fmt.Errorf("oplog: update missing query or update")
	}

	// Find document by _id in Object2
	idVal, ok := entry.Object2.Get("_id")
	if !ok {
		return fmt.Errorf("oplog: update missing _id in query")
	}

	key := makeDocumentKey(entry.Namespace, idVal)
	data, err := a.eng.Get(key)
	if err != nil {
		return err // Document not found
	}

	doc, err := bson.Decode(data)
	if err != nil {
		return err
	}

	// Apply update operations ($set, $unset, etc.)
	for _, elem := range entry.Object.Elements() {
		switch elem.Key {
		case "$set":
			if setDoc := elem.Value.DocumentValue(); setDoc != nil {
				for _, se := range setDoc.Elements() {
					doc.Set(se.Key, se.Value)
				}
			}
		case "$unset":
			if unsetDoc := elem.Value.DocumentValue(); unsetDoc != nil {
				for _, ue := range unsetDoc.Elements() {
					doc.Delete(ue.Key)
				}
			}
		default:
			// Direct field replacement
			doc.Set(elem.Key, elem.Value)
		}
	}

	return a.eng.Put(key, bson.Encode(doc))
}

func (a *OplogApplier) applyDelete(entry *OplogEntry) error {
	if entry.Object == nil {
		return fmt.Errorf("oplog: delete missing query")
	}

	idVal, ok := entry.Object.Get("_id")
	if !ok {
		return fmt.Errorf("oplog: delete missing _id")
	}

	key := makeDocumentKey(entry.Namespace, idVal)
	return a.eng.Delete(key)
}

// makeDocumentKey creates a storage key for a document.
func makeDocumentKey(ns string, id bson.Value) []byte {
	prefix := []byte(ns + ".")
	var idBytes []byte

	switch id.Type {
	case bson.TypeObjectID:
		oid := id.ObjectID()
		idBytes = oid[:]
	case bson.TypeString:
		idBytes = []byte(id.String())
	case bson.TypeInt32:
		idBytes = []byte(fmt.Sprintf("%d", id.Int32()))
	case bson.TypeInt64:
		idBytes = []byte(fmt.Sprintf("%d", id.Int64()))
	default:
		idBytes = []byte(id.String())
	}

	return append(prefix, idBytes...)
}
