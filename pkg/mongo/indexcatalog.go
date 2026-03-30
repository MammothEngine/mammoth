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
	idx := NewIndex(db, coll, &spec, ic.engine)
	prefix := EncodeNamespacePrefix(db, coll)
	ic.engine.Scan(prefix, func(_, docValue []byte) bool {
		doc, err := bson.Decode(docValue)
		if err != nil {
			return true
		}
		idx.AddEntry(doc)
		return true
	})

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
