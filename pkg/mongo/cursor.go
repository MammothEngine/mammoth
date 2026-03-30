package mongo

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
)

const (
	defaultBatchSize  = 101
	cursorTimeout     = 10 * time.Minute
)

// Cursor manages iteration over query results with batch-based fetching.
type Cursor struct {
	id        uint64
	namespace string
	docs      []*bson.Document
	pos       int
	batchSize int
}

// ID returns the cursor identifier.
func (c *Cursor) ID() uint64 { return c.id }

// Namespace returns "db.collection".
func (c *Cursor) Namespace() string { return c.namespace }

// HasNext returns true if there are more documents.
func (c *Cursor) HasNext() bool { return c.pos < len(c.docs) }

// Next returns the next document, or nil if exhausted.
func (c *Cursor) Next() *bson.Document {
	if c.pos >= len(c.docs) {
		return nil
	}
	doc := c.docs[c.pos]
	c.pos++
	return doc
}

// GetBatch returns the next batch of documents up to batchSize.
func (c *Cursor) GetBatch(batchSize int) []*bson.Document {
	if batchSize <= 0 {
		batchSize = c.batchSize
	}
	remaining := len(c.docs) - c.pos
	if remaining == 0 {
		return nil
	}
	n := batchSize
	if n > remaining {
		n = remaining
	}
	batch := c.docs[c.pos : c.pos+n]
	c.pos += n
	return batch
}

// Exhausted returns true if all documents have been consumed.
func (c *Cursor) Exhausted() bool { return c.pos >= len(c.docs) }

// CursorManager manages active cursors with automatic timeout cleanup.
type CursorManager struct {
	mu      sync.RWMutex
	cursors map[uint64]*cursorEntry
	nextID  atomic.Uint64
}

type cursorEntry struct {
	cursor   *Cursor
	lastUsed time.Time
}

// NewCursorManager creates a new cursor manager.
func NewCursorManager() *CursorManager {
	cm := &CursorManager{
		cursors: make(map[uint64]*cursorEntry),
	}
	cm.nextID.Store(1)
	go cm.cleanup()
	return cm
}

// Register creates and registers a new cursor from documents.
func (cm *CursorManager) Register(namespace string, docs []*bson.Document, batchSize int) *Cursor {
	id := cm.nextID.Add(1)
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	c := &Cursor{
		id:        id - 1,
		namespace: namespace,
		docs:      docs,
		batchSize: batchSize,
	}
	cm.mu.Lock()
	cm.cursors[c.id] = &cursorEntry{cursor: c, lastUsed: time.Now()}
	cm.mu.Unlock()
	return c
}

// Get retrieves a cursor by ID.
func (cm *CursorManager) Get(id uint64) (*Cursor, bool) {
	cm.mu.RLock()
	entry, ok := cm.cursors[id]
	cm.mu.RUnlock()
	if !ok {
		return nil, false
	}
	entry.lastUsed = time.Now()
	return entry.cursor, true
}

// Kill removes cursors by IDs.
func (cm *CursorManager) Kill(ids []uint64) {
	cm.mu.Lock()
	for _, id := range ids {
		delete(cm.cursors, id)
	}
	cm.mu.Unlock()
}

// Close removes all cursors.
func (cm *CursorManager) Close() {
	cm.mu.Lock()
	cm.cursors = nil
	cm.mu.Unlock()
}

func (cm *CursorManager) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cm.mu.Lock()
		now := time.Now()
		for id, entry := range cm.cursors {
			if now.Sub(entry.lastUsed) > cursorTimeout {
				delete(cm.cursors, id)
			}
		}
		cm.mu.Unlock()
	}
}
