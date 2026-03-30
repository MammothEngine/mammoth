package mongo

import (
	"encoding/json"
	"sync"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// IndexCatalog manages index metadata and maintenance.
type IndexCatalog struct {
	mu     sync.RWMutex
	engine *engine.Engine
	cat    *Catalog
}

// NewIndexCatalog creates a new index catalog.
func NewIndexCatalog(eng *engine.Engine, cat *Catalog) *IndexCatalog {
	return &IndexCatalog{engine: eng, cat: cat}
}

// CreateIndex creates a new secondary index and builds entries for existing documents.
func (ic *IndexCatalog) CreateIndex(db, coll string, spec IndexSpec) error {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	if _, err := ic.cat.GetCollection(db, coll); err != nil {
		return err
	}

	key := encodeCatalogKeyIndex(db, coll, spec.Name)
	if _, err := ic.engine.Get(key); err == nil {
		return ErrNamespaceExists
	}

	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	if err := ic.engine.Put(key, data); err != nil {
		return err
	}

	// Build index entries for existing documents
	// Collect docs first to avoid deadlock (Scan holds read lock, Put needs write lock)
	idx := NewIndex(db, coll, &spec, ic.engine)
	prefix := EncodeNamespacePrefix(db, coll)
	var existingDocs []*bson.Document
	ic.engine.Scan(prefix, func(_, docValue []byte) bool {
		doc, err := bson.Decode(docValue)
		if err != nil {
			return true
		}
		existingDocs = append(existingDocs, doc)
		return true
	})

	for _, doc := range existingDocs {
		idx.AddEntry(doc)
	}

	return nil
}

// DropIndex removes an index and its entries.
func (ic *IndexCatalog) DropIndex(db, coll, name string) error {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	key := encodeCatalogKeyIndex(db, coll, name)
	if _, err := ic.engine.Get(key); err != nil {
		return ErrNamespaceNotFound
	}

	idx := NewIndex(db, coll, &IndexSpec{Name: name}, ic.engine)
	prefix := idx.ScanPrefix()
	ic.engine.Scan(prefix, func(k, _ []byte) bool {
		ic.engine.Delete(k)
		return true
	})

	return ic.engine.Delete(key)
}

// ListIndexes returns all indexes for a collection.
func (ic *IndexCatalog) ListIndexes(db, coll string) ([]IndexSpec, error) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()

	var indexes []IndexSpec
	prefix := append([]byte(catalogPrefix), catalogTypeIndex)
	ic.engine.Scan(prefix, func(_, value []byte) bool {
		var spec IndexSpec
		if err := json.Unmarshal(value, &spec); err == nil {
			indexes = append(indexes, spec)
		}
		return true
	})
	return indexes, nil
}

// GetIndex returns a specific index by name.
func (ic *IndexCatalog) GetIndex(db, coll, name string) (*Index, error) {
	key := encodeCatalogKeyIndex(db, coll, name)
	data, err := ic.engine.Get(key)
	if err != nil {
		return nil, ErrNamespaceNotFound
	}
	var spec IndexSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return NewIndex(db, coll, &spec, ic.engine), nil
}

// OnDocumentInsert updates all indexes when a document is inserted.
func (ic *IndexCatalog) OnDocumentInsert(db, coll string, doc *bson.Document) error {
	indexes, err := ic.ListIndexes(db, coll)
	if err != nil {
		return err
	}
	for _, spec := range indexes {
		idx := NewIndex(db, coll, &spec, ic.engine)
		if err := idx.AddEntry(doc); err != nil {
			return err
		}
	}
	return nil
}

// OnDocumentDelete updates all indexes when a document is deleted.
func (ic *IndexCatalog) OnDocumentDelete(db, coll string, doc *bson.Document) error {
	indexes, err := ic.ListIndexes(db, coll)
	if err != nil {
		return err
	}
	for _, spec := range indexes {
		idx := NewIndex(db, coll, &spec, ic.engine)
		idx.RemoveEntry(doc)
	}
	return nil
}

// OnDocumentUpdate updates all indexes when a document is updated.
func (ic *IndexCatalog) OnDocumentUpdate(db, coll string, oldDoc, newDoc *bson.Document) error {
	indexes, err := ic.ListIndexes(db, coll)
	if err != nil {
		return err
	}
	for _, spec := range indexes {
		idx := NewIndex(db, coll, &spec, ic.engine)
		idx.RemoveEntry(oldDoc)
		if err := idx.AddEntry(newDoc); err != nil {
			return err
		}
	}
	return nil
}

// IndexBounds describes lower and upper bounds for a range predicate on an indexed field.
type IndexBounds struct {
	Field    string
	Low      []byte // inclusive lower bound
	High     []byte // exclusive upper bound
	Equality bool   // exact match
}

// FindBestIndex finds the best index for a query filter.
// Returns the index spec, the encoded prefix key for scanning, and whether an index was found.
func (ic *IndexCatalog) FindBestIndex(db, coll string, filter *bson.Document) (spec *IndexSpec, prefixKey []byte, ok bool) {
	indexes, err := ic.ListIndexes(db, coll)
	if err != nil || len(indexes) == 0 {
		return nil, nil, false
	}

	// Analyze filter to extract field constraints
	filterFields := analyzeFilterFields(filter)

	var bestSpec *IndexSpec
	bestScore := 0

	for i := range indexes {
		score := scoreIndex(&indexes[i], filterFields)
		if score > bestScore {
			bestSpec = &indexes[i]
			bestScore = score
		}
	}

	if bestSpec == nil {
		return nil, nil, false
	}

	// Build the prefix key for the index lookup
	prefixKey = buildIndexScanKey(db, coll, bestSpec, filter)
	return bestSpec, prefixKey, true
}

// analyzeFilterFields extracts per-field information from a filter.
func analyzeFilterFields(filter *bson.Document) map[string]filterFieldInfo {
	if filter == nil {
		return nil
	}
	result := make(map[string]filterFieldInfo, filter.Len())
	for _, e := range filter.Elements() {
		info := filterFieldInfo{equality: true}
		if e.Value.Type == bson.TypeDocument {
			// Check for operator expressions
			opDoc := e.Value.DocumentValue()
			hasRange := false
			for _, oe := range opDoc.Elements() {
				switch oe.Key {
				case "$gt", "$gte", "$lt", "$lte":
					hasRange = true
					info.equality = false
				case "$eq":
					// Still equality
				default:
					info.equality = false
				}
			}
			info.hasRange = hasRange
		}
		result[e.Key] = info
	}
	return result
}

type filterFieldInfo struct {
	equality bool
	hasRange bool
}

// scoreIndex returns a score for how well an index matches the filter.
// Higher is better.
func scoreIndex(spec *IndexSpec, fields map[string]filterFieldInfo) int {
	score := 0
	for _, ik := range spec.Key {
		info, found := fields[ik.Field]
		if !found {
			break // prefix must be contiguous
		}
		if info.equality {
			score += 2 // equality is most selective
		} else if info.hasRange {
			score += 1 // range is less selective but still usable
		} else {
			break
		}
	}
	return score
}

// buildIndexScanKey builds the encoded prefix key for index scanning.
func buildIndexScanKey(db, coll string, spec *IndexSpec, filter *bson.Document) []byte {
	ns := EncodeNamespacePrefix(db, coll)
	buf := make([]byte, 0, len(ns)+len(indexSeparator)+len(spec.Name)+64)
	buf = append(buf, ns...)
	buf = append(buf, indexSeparator...)
	buf = append(buf, spec.Name...)

	// Encode equality filter values
	for _, ik := range spec.Key {
		v, found := filter.Get(ik.Field)
		if !found {
			break
		}
		// Check if it's an operator expression
		if v.Type == bson.TypeDocument {
			opDoc := v.DocumentValue()
			if eqVal, ok := opDoc.Get("$eq"); ok {
				v = eqVal
			} else {
				break // Can't use range for prefix, stop here
			}
		}
		encoded := encodeIndexValue(v)
		if ik.Descending {
			flipped := make([]byte, len(encoded))
			copy(flipped, encoded)
			flipForDescending(flipped)
			encoded = flipped
		}
		buf = append(buf, encoded...)
	}

	return buf
}
