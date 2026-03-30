package engine

import (
	"bytes"
	"sort"
	"sync"

	"github.com/mammothengine/mammoth/pkg/engine/memtable"
	"github.com/mammothengine/mammoth/pkg/engine/sstable"
)

// Scan iterates over all key-value pairs whose keys start with prefix.
// Keys are visited in sorted order. Tombstones are skipped.
// The callback fn receives each key-value pair; returning false stops iteration.
func (e *Engine) Scan(prefix []byte, fn func(key, value []byte) bool) error {
	if e.closed.Load() {
		return errEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	merged := e.collectPrefix(prefix)
	for _, entry := range merged {
		if !fn(entry.key, entry.value) {
			break
		}
	}
	return nil
}

// NewPrefixIterator creates a stateful iterator over keys with the given prefix.
// Not safe for concurrent use; create one per goroutine.
func (e *Engine) NewPrefixIterator(prefix []byte) *PrefixIterator {
	return &PrefixIterator{
		engine: e,
		prefix: prefix,
		pos:    -1,
	}
}

// PrefixIterator iterates over key-value pairs matching a prefix in sorted order.
type PrefixIterator struct {
	engine  *Engine
	prefix  []byte
	entries []kvEntry
	pos     int
	err     error
	init    sync.Once
}

type kvEntry struct {
	key   []byte
	value []byte
}

type scanEntry struct {
	value []byte
	del   bool
}

// Next advances to the next matching entry. Returns false when done.
func (it *PrefixIterator) Next() bool {
	it.init.Do(func() { it.entries = it.engine.collectPrefix(it.prefix) })
	it.pos++
	return it.pos < len(it.entries)
}

// Key returns the current key.
func (it *PrefixIterator) Key() []byte {
	if it.pos < 0 || it.pos >= len(it.entries) {
		return nil
	}
	return it.entries[it.pos].key
}

// Value returns the current value.
func (it *PrefixIterator) Value() []byte {
	if it.pos < 0 || it.pos >= len(it.entries) {
		return nil
	}
	return it.entries[it.pos].value
}

// Err returns any error encountered.
func (it *PrefixIterator) Err() error {
	return it.err
}

// Close releases resources.
func (it *PrefixIterator) Close() {
	it.entries = nil
}

// collectPrefix gathers all non-tombstone entries matching prefix from all sources,
// deduplicates (newer wins), and returns them in sorted order.
func (e *Engine) collectPrefix(prefix []byte) []kvEntry {
	seen := make(map[string]scanEntry)

	// 1. Scan active memtable (highest priority)
	scanMem(e.mmgr.ActiveMemtable(), prefix, seen)

	// 2. Scan immutable memtables
	for _, mt := range e.mmgr.ImmutableMemtables() {
		scanMem(mt, prefix, seen)
	}

	// 3. Scan SSTables (only fill keys not yet seen from memtables)
	v := e.mft.CurrentVersion()
	for level := 0; level < 7; level++ {
		for _, f := range v.Files(level) {
			r, ok := e.readers[f.FileNum]
			if !ok {
				continue
			}
			scanSST(r, prefix, seen)
		}
	}

	var result []kvEntry
	for k, ent := range seen {
		if ent.del {
			continue
		}
		result = append(result, kvEntry{key: []byte(k), value: ent.value})
	}
	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i].key, result[j].key) < 0
	})
	return result
}

func scanMem(mt *memtable.Memtable, prefix []byte, seen map[string]scanEntry) {
	it := mt.NewIterator()
	it.Seek(prefix)
	for it.Valid() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		ks := string(key)
		val := it.Value()
		if val == nil {
			seen[ks] = scanEntry{del: true}
		} else {
			seen[ks] = scanEntry{value: val}
		}
		it.Next()
	}
}

func scanSST(r *sstable.Reader, prefix []byte, seen map[string]scanEntry) {
	it := sstable.NewIterator(r)
	it.Seek(prefix)
	for it.Valid() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		ks := string(key)
		val := it.Value()
		if string(val) == tombstoneMarker {
			if _, exists := seen[ks]; !exists {
				seen[ks] = scanEntry{del: true}
			}
		} else {
			if _, exists := seen[ks]; !exists {
				seen[ks] = scanEntry{value: val}
			}
		}
		it.Next()
	}
}
