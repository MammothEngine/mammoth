package mongo

import (
	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// Collection provides document CRUD operations for a single collection.
type Collection struct {
	db   string
	name string
	eng  *engine.Engine
	cat  *Catalog
}

// NewCollection creates a Collection handle. Does not verify existence.
func NewCollection(db, name string, eng *engine.Engine, cat *Catalog) *Collection {
	return &Collection{db: db, name: name, eng: eng, cat: cat}
}

// FullName returns "db.collection".
func (c *Collection) FullName() string {
	return c.db + "." + c.name
}

// DB returns the database name.
func (c *Collection) DB() string { return c.db }

// Name returns the collection name.
func (c *Collection) Name() string { return c.name }

// InsertOne inserts a single document. Generates _id if missing.
func (c *Collection) InsertOne(doc *bson.Document) (bson.ObjectID, error) {
	id := ensureID(doc)
	data := bson.Encode(doc)
	key := encodeDocKey(c.db, c.name, id[:])
	if err := c.eng.Put(key, data); err != nil {
		return bson.ObjectID{}, err
	}
	return id, nil
}

// InsertMany inserts multiple documents atomically.
func (c *Collection) InsertMany(docs []*bson.Document) ([]bson.ObjectID, error) {
	batch := c.eng.NewBatch()
	ids := make([]bson.ObjectID, len(docs))
	for i, doc := range docs {
		id := ensureID(doc)
		ids[i] = id
		data := bson.Encode(doc)
		key := encodeDocKey(c.db, c.name, id[:])
		batch.Put(key, data)
	}
	if err := batch.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

// FindOne retrieves a document by _id.
func (c *Collection) FindOne(id bson.ObjectID) (*bson.Document, error) {
	key := encodeDocKey(c.db, c.name, id[:])
	data, err := c.eng.Get(key)
	if err != nil {
		return nil, ErrNotFound
	}
	return bson.Decode(data)
}

// FindOneByKey retrieves a document by raw key bytes.
func (c *Collection) FindOneByKey(keyBytes []byte) (*bson.Document, error) {
	data, err := c.eng.Get(keyBytes)
	if err != nil {
		return nil, ErrNotFound
	}
	return bson.Decode(data)
}

// DeleteOne deletes a document by _id.
func (c *Collection) DeleteOne(id bson.ObjectID) error {
	key := encodeDocKey(c.db, c.name, id[:])
	if err := c.eng.Delete(key); err != nil {
		return err
	}
	return nil
}

// DeleteByKey deletes a document by raw key bytes.
func (c *Collection) DeleteByKey(keyBytes []byte) error {
	return c.eng.Delete(keyBytes)
}

// ScanAll iterates over all documents in the collection.
// The callback receives each raw key and decoded document.
func (c *Collection) ScanAll(fn func(key []byte, doc *bson.Document) bool) error {
	prefix := EncodeNamespacePrefix(c.db, c.name)
	return c.eng.Scan(prefix, func(key, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true // skip corrupt
		}
		return fn(key, doc)
	})
}

// Count returns the number of documents in the collection.
func (c *Collection) Count() (int64, error) {
	var count int64
	prefix := EncodeNamespacePrefix(c.db, c.name)
	c.eng.Scan(prefix, func(_, _ []byte) bool {
		count++
		return true
	})
	return count, nil
}

// ReplaceOne replaces an entire document by _id.
func (c *Collection) ReplaceOne(id bson.ObjectID, doc *bson.Document) error {
	// Ensure the _id matches
	doc.Set("_id", bson.VObjectID(id))
	data := bson.Encode(doc)
	key := encodeDocKey(c.db, c.name, id[:])
	return c.eng.Put(key, data)
}

// ensureID returns the _id from the document, generating a new ObjectID if missing.
func ensureID(doc *bson.Document) bson.ObjectID {
	v, ok := doc.Get("_id")
	if ok && v.Type == bson.TypeObjectID {
		return v.ObjectID()
	}
	id := bson.NewObjectID()
	doc.Set("_id", bson.VObjectID(id))
	return id
}
