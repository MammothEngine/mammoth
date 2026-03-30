package mongo

import (
	"github.com/mammothengine/mammoth/pkg/engine"
)

// CappedCollection manages size and document count limits for capped collections.
type CappedCollection struct {
	db      string
	coll    string
	eng     *engine.Engine
	cat     *Catalog
	maxSize int64
	maxDocs int64
}

// NewCappedCollection creates a capped collection manager.
func NewCappedCollection(db, coll string, eng *engine.Engine, cat *Catalog, maxSize, maxDocs int64) *CappedCollection {
	return &CappedCollection{
		db: db, coll: coll, eng: eng, cat: cat,
		maxSize: maxSize, maxDocs: maxDocs,
	}
}

// EnforceLimits checks and enforces capped collection limits.
// Deletes oldest documents if limits are exceeded.
// Returns the number of documents removed.
func (c *CappedCollection) EnforceLimits() (int, error) {
	if c.maxSize <= 0 && c.maxDocs <= 0 {
		return 0, nil
	}

	prefix := EncodeNamespacePrefix(c.db, c.coll)

	// Collect all docs with their sizes
	type docEntry struct {
		key  []byte
		size int
	}
	var entries []docEntry
	totalSize := int64(0)

	c.eng.Scan(prefix, func(key, value []byte) bool {
		entries = append(entries, docEntry{key: append([]byte{}, key...), size: len(value)})
		totalSize += int64(len(value))
		return true
	})

	removed := 0

	// Check document count limit
	if c.maxDocs > 0 && int64(len(entries)) > c.maxDocs {
		excess := int64(len(entries)) - c.maxDocs
		for i := int64(0); i < excess && i < int64(len(entries)); i++ {
			c.eng.Delete(entries[i].key)
			totalSize -= int64(entries[i].size)
			removed++
		}
		entries = entries[excess:]
	}

	// Check size limit
	if c.maxSize > 0 && totalSize > c.maxSize {
		for i := 0; i < len(entries) && totalSize > c.maxSize; i++ {
			c.eng.Delete(entries[i].key)
			totalSize -= int64(entries[i].size)
			removed++
		}
	}

	return removed, nil
}

// IsCapped checks if a collection is capped.
func IsCapped(cat *Catalog, db, coll string) bool {
	info, err := cat.GetCollection(db, coll)
	if err != nil {
		return false
	}
	return info.Capped
}

// GetCappedInfo returns capped collection parameters.
func GetCappedInfo(cat *Catalog, db, coll string) (capped bool, maxSize, maxDocs int64) {
	info, err := cat.GetCollection(db, coll)
	if err != nil {
		return false, 0, 0
	}
	return info.Capped, info.MaxSize, info.MaxDocs
}
