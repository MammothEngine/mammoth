package mongo

import (
	"bytes"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// IndexIterator iterates over index entries in sorted order.
type IndexIterator struct {
	eng       *engine.Engine
	prefix    []byte // Index prefix (ns + idx + indexName)
	startKey  []byte // Start bound (nil = unbounded)
	endKey    []byte // End bound (nil = unbounded)
	startInc  bool   // Start bound inclusive
	endInc    bool   // End bound inclusive

	// Iterator state
	snapshot  *engine.Snapshot
	currentID []byte
	valid     bool
	closed    bool
}

// IndexScanBounds represents range bounds for index scanning.
type IndexScanBounds struct {
	StartKey       []byte // Inclusive lower bound
	EndKey         []byte // Exclusive upper bound
	StartInclusive bool
	EndInclusive   bool
}

// NewIndexIterator creates an iterator over index entries.
func (idx *Index) NewIterator(bounds *IndexScanBounds) *IndexIterator {
	prefix := idx.ScanPrefix()

	it := &IndexIterator{
		eng:      idx.eng,
		prefix:   prefix,
		snapshot: idx.eng.NewSnapshot(),
		valid:    true,
	}

	if bounds != nil {
		// Build full keys including prefix
		if len(bounds.StartKey) > 0 {
			it.startKey = make([]byte, 0, len(prefix)+len(bounds.StartKey))
			it.startKey = append(it.startKey, prefix...)
			it.startKey = append(it.startKey, bounds.StartKey...)
			it.startInc = bounds.StartInclusive
		}
		if len(bounds.EndKey) > 0 {
			it.endKey = make([]byte, 0, len(prefix)+len(bounds.EndKey))
			it.endKey = append(it.endKey, prefix...)
			it.endKey = append(it.endKey, bounds.EndKey...)
			it.endInc = bounds.EndInclusive
		}
	}

	return it
}

// Seek positions the iterator at the first key >= target (or > target if inclusive=false).
func (it *IndexIterator) Seek(target []byte, inclusive bool) bool {
	if it.closed {
		return false
	}

	// Use engine snapshot seek
	// Since engine doesn't have direct seek, we scan from the prefix
	it.valid = false
	it.currentID = nil

	_ = it.snapshot.Scan(it.prefix, func(key, _ []byte) bool {
		if len(key) <= len(it.prefix) {
			return true
		}

		cmp := bytes.Compare(key[len(it.prefix):], target)

		if inclusive {
			if cmp >= 0 {
				// Check bounds
				if it.withinBounds(key) {
					it.currentID = key[len(it.prefix):]
					it.valid = true
					return false
				}
			}
		} else {
			if cmp > 0 {
				if it.withinBounds(key) {
					it.currentID = key[len(it.prefix):]
					it.valid = true
					return false
				}
			}
		}
		return true
	})

	return it.valid
}

// Next advances to the next index entry.
func (it *IndexIterator) Next() bool {
	if it.closed || !it.valid {
		return false
	}

	found := false
	_ = it.snapshot.Scan(it.prefix, func(key, _ []byte) bool {
		if len(key) <= len(it.prefix) {
			return true
		}

		// Skip current
		idBytes := key[len(it.prefix):]
		if it.currentID != nil && bytes.Equal(idBytes, it.currentID) {
			return true
		}

		// Check if we've passed our current position
		if it.currentID != nil && bytes.Compare(idBytes, it.currentID) <= 0 {
			return true
		}

		// Check bounds
		if !it.withinBounds(key) {
			return false
		}

		it.currentID = idBytes
		found = true
		return false
	})

	it.valid = found
	return found
}

// Valid returns whether the iterator is positioned at a valid entry.
func (it *IndexIterator) Valid() bool {
	return it != nil && it.valid && !it.closed
}

// ID returns the current document ID (ObjectID bytes).
func (it *IndexIterator) ID() []byte {
	if !it.Valid() {
		return nil
	}
	return it.currentID
}

// Close releases iterator resources.
func (it *IndexIterator) Close() {
	if !it.closed {
		it.snapshot.Release()
		it.closed = true
		it.valid = false
	}
}

// withinBounds checks if a key is within the iterator's bounds.
func (it *IndexIterator) withinBounds(key []byte) bool {
	if len(key) <= len(it.prefix) {
		return false
	}

	// Check start bound
	if len(it.startKey) > 0 {
		cmp := bytes.Compare(key, it.startKey)
		if it.startInc {
			if cmp < 0 {
				return false
			}
		} else {
			if cmp <= 0 {
				return false
			}
		}
	}

	// Check end bound
	if len(it.endKey) > 0 {
		cmp := bytes.Compare(key, it.endKey)
		if it.endInc {
			if cmp > 0 {
				return false
			}
		} else {
			if cmp >= 0 {
				return false
			}
		}
	}

	return true
}

// IndexEntry represents a single index entry.
type IndexEntry struct {
	ID    bson.ObjectID
	Value bson.Value
}

// RangeScan performs a range scan over the index and returns matching document IDs.
// This is a convenience method that doesn't require manual iterator management.
func (idx *Index) RangeScan(bounds *IndexScanBounds, limit int) ([]bson.ObjectID, error) {
	it := idx.NewIterator(bounds)
	defer it.Close()

	var ids []bson.ObjectID
	count := 0

	// Start from beginning or seek to start
	if bounds != nil && len(bounds.StartKey) > 0 {
		if !it.Seek(bounds.StartKey, bounds.StartInclusive) {
			return ids, nil
		}
	} else {
		// Get first entry
		if !it.Next() {
			return ids, nil
		}
	}

	for it.Valid() {
		idBytes := it.ID()
		if len(idBytes) >= 12 {
			var id bson.ObjectID
			copy(id[:], idBytes[:12])
			ids = append(ids, id)
			count++
		}

		if limit > 0 && count >= limit {
			break
		}

		if !it.Next() {
			break
		}
	}

	return ids, nil
}

// PointLookup performs an exact match lookup on the index.
func (idx *Index) PointLookup(values []bson.Value) ([]bson.ObjectID, error) {
	// Build the key prefix for the lookup
	keyPrefix := make([]byte, 0)
	for i, v := range values {
		if i >= len(idx.spec.Key) {
			break
		}
		env := encodeIndexValue(v)
		if idx.spec.Key[i].Descending {
			flipped := make([]byte, len(env))
			copy(flipped, env)
			flipForDescending(flipped)
			env = flipped
		}
		keyPrefix = append(keyPrefix, env...)
	}

	fullPrefix := idx.ScanPrefix()
	scanPrefix := make([]byte, 0, len(fullPrefix)+len(keyPrefix))
	scanPrefix = append(scanPrefix, fullPrefix...)
	scanPrefix = append(scanPrefix, keyPrefix...)

	var ids []bson.ObjectID
	_ = idx.eng.Scan(scanPrefix, func(key, _ []byte) bool {
		if !bytes.HasPrefix(key, scanPrefix) {
			return false
		}

		// Extract ID from end of key
		if len(key) > len(scanPrefix) {
			idBytes := key[len(scanPrefix):]
			if len(idBytes) >= 12 {
				var id bson.ObjectID
				copy(id[:], idBytes[:12])
				ids = append(ids, id)
			}
		}
		return true
	})

	return ids, nil
}

// Count returns an estimate of the number of entries in the index.
func (idx *Index) Count() int64 {
	prefix := idx.ScanPrefix()
	var count int64
	_ = idx.eng.Scan(prefix, func(_, _ []byte) bool {
		count++
		return true
	})
	return count
}
