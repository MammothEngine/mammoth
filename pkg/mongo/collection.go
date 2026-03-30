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
func (c *Collection) InsertOne(doc *bson.Document) error {
	idBytes := ensureID(doc)
	data := bson.Encode(doc)
	key := encodeDocKey(c.db, c.name, idBytes)
	return c.eng.Put(key, data)
}

// InsertMany inserts multiple documents atomically.
func (c *Collection) InsertMany(docs []*bson.Document) error {
	batch := c.eng.NewBatch()
	for _, doc := range docs {
		idBytes := ensureID(doc)
		data := bson.Encode(doc)
		key := encodeDocKey(c.db, c.name, idBytes)
		batch.Put(key, data)
	}
	return batch.Commit()
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

// ReplaceOne replaces an entire document by ObjectID _id.
func (c *Collection) ReplaceOne(id bson.ObjectID, doc *bson.Document) error {
	doc.Set("_id", bson.VObjectID(id))
	data := bson.Encode(doc)
	key := encodeDocKey(c.db, c.name, id[:])
	return c.eng.Put(key, data)
}

// ReplaceByKey replaces a document using its raw storage key.
func (c *Collection) ReplaceByKey(keyBytes []byte, doc *bson.Document) error {
	data := bson.Encode(doc)
	return c.eng.Put(keyBytes, data)
}

// ensureID ensures the document has an _id field. If missing, a new ObjectID is generated.
// Returns the raw bytes of the _id value for use as a storage key.
func ensureID(doc *bson.Document) []byte {
	v, ok := doc.Get("_id")
	if ok {
		return encodeIDValue(v)
	}
	id := bson.NewObjectID()
	doc.Set("_id", bson.VObjectID(id))
	return id[:]
}

// encodeIDValue converts a BSON _id value to bytes suitable for storage keys.
func encodeIDValue(v bson.Value) []byte {
	switch v.Type {
	case bson.TypeObjectID:
		oid := v.ObjectID()
		return oid[:]
	case bson.TypeString:
		return []byte(v.String())
	case bson.TypeInt32:
		b := make([]byte, 4)
		b[0] = byte(v.Int32() >> 24)
		b[1] = byte(v.Int32() >> 16)
		b[2] = byte(v.Int32() >> 8)
		b[3] = byte(v.Int32())
		return b
	case bson.TypeInt64:
		b := make([]byte, 8)
		b[0] = byte(v.Int64() >> 56)
		b[1] = byte(v.Int64() >> 48)
		b[2] = byte(v.Int64() >> 40)
		b[3] = byte(v.Int64() >> 32)
		b[4] = byte(v.Int64() >> 24)
		b[5] = byte(v.Int64() >> 16)
		b[6] = byte(v.Int64() >> 8)
		b[7] = byte(v.Int64())
		return b
	default:
		// Fallback: encode the full document as the key
		d := bson.NewDocument()
		d.Set("_id", v)
		return bson.Encode(d)
	}
}
