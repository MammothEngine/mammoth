// Package mammoth provides an embedded MongoDB-compatible database API.
// No server, wire protocol, or network required — direct engine access.
package mammoth

import (
	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// Document is an alias for bson.Document for convenience.
type Document = bson.Document

// DB is the top-level database handle.
type DB struct {
	eng     *engine.Engine
	cat     *mongo.Catalog
	idxCat  *mongo.IndexCatalog
}

// Database represents a named database within the engine.
type Database struct {
	name string
	db   *DB
}

// Collection represents a named collection within a database.
type Collection struct {
	db   *Database
	coll *mongo.Collection
	info *mongo.CollectionInfo
}

// Cursor iterates over query results.
type Cursor struct {
	docs []*Document
	pos  int
}

// Option configures the database.
type Option func(*config)

type config struct {
	dataDir string
}

// WithDataDir sets the data directory path.
func WithDataDir(path string) Option {
	return func(c *config) { c.dataDir = path }
}

// Open creates or opens a database at the given path.
func Open(path string, opts ...Option) (*DB, error) {
	cfg := config{dataDir: path}
	for _, o := range opts {
		o(&cfg)
	}

	engOpts := engine.DefaultOptions(cfg.dataDir)
	eng, err := engine.Open(engOpts)
	if err != nil {
		return nil, err
	}

	cat := mongo.NewCatalog(eng)
	idxCat := mongo.NewIndexCatalog(eng, cat)

	return &DB{eng: eng, cat: cat, idxCat: idxCat}, nil
}

// Close releases all resources.
func (db *DB) Close() error {
	return db.eng.Close()
}

// Engine returns the underlying storage engine (for advanced use).
func (db *DB) Engine() *engine.Engine {
	return db.eng
}

// Catalog returns the underlying catalog (for advanced use).
func (db *DB) Catalog() *mongo.Catalog {
	return db.cat
}

// IndexCatalog returns the underlying index catalog (for advanced use).
func (db *DB) IndexCatalog() *mongo.IndexCatalog {
	return db.idxCat
}

// Database returns a handle for a named database.
func (db *DB) Database(name string) *Database {
	_ = db.cat.EnsureDatabase(name)
	return &Database{name: name, db: db}
}

// DropDatabase drops a database and all its collections.
func (db *DB) DropDatabase(name string) error {
	return db.cat.DropDatabase(name)
}

// --- Database ---

// Name returns the database name.
func (d *Database) Name() string { return d.name }

// Collection returns a handle for a named collection.
func (d *Database) Collection(name string) *Collection {
	_ = d.db.cat.EnsureCollection(d.name, name)
	coll := mongo.NewCollection(d.name, name, d.db.eng, d.db.cat)
	info, _ := d.db.cat.GetCollection(d.name, name)
	return &Collection{db: d, coll: coll, info: &info}
}

// Drop drops the entire database.
func (d *Database) Drop() error {
	return d.db.cat.DropDatabase(d.name)
}

// ListCollections returns all collection names in the database.
func (d *Database) ListCollections() ([]string, error) {
	colls, err := d.db.cat.ListCollections(d.name)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(colls))
	for i, c := range colls {
		names[i] = c.Name
	}
	return names, nil
}

// CreateCollection creates a new collection with optional config.
func (d *Database) CreateCollection(name string) error {
	return d.db.cat.CreateCollection(d.name, name)
}

// DropCollection drops a collection.
func (d *Database) DropCollection(name string) error {
	return d.db.cat.DropCollection(d.name, name)
}

// CreateIndex creates a secondary index.
func (d *Database) CreateIndex(collName string, spec mongo.IndexSpec) error {
	return d.db.idxCat.CreateIndex(d.name, collName, spec)
}

// DropIndex drops a secondary index.
func (d *Database) DropIndex(collName, indexName string) error {
	return d.db.idxCat.DropIndex(d.name, collName, indexName)
}

// --- Collection ---

// Name returns the collection name.
func (c *Collection) Name() string { return c.coll.Name() }

// InsertOne inserts a single document.
func (c *Collection) InsertOne(doc *Document) error {
	err := c.coll.InsertOne(doc)
	if err != nil {
		return err
	}
	// Index maintenance
	c.db.db.idxCat.OnDocumentInsert(c.db.name, c.coll.Name(), doc)
	// Capped enforcement
	if c.info != nil && c.info.Capped {
		cc := mongo.NewCappedCollection(c.db.name, c.coll.Name(), c.db.db.eng, c.db.db.cat, c.info.MaxSize, c.info.MaxDocs)
		cc.EnforceLimits()
	}
	return nil
}

// InsertMany inserts multiple documents atomically.
func (c *Collection) InsertMany(docs []*Document) error {
	err := c.coll.InsertMany(docs)
	if err != nil {
		return err
	}
	// Index maintenance
	for _, doc := range docs {
		c.db.db.idxCat.OnDocumentInsert(c.db.name, c.coll.Name(), doc)
	}
	// Capped enforcement
	if c.info != nil && c.info.Capped {
		cc := mongo.NewCappedCollection(c.db.name, c.coll.Name(), c.db.db.eng, c.db.db.cat, c.info.MaxSize, c.info.MaxDocs)
		cc.EnforceLimits()
	}
	return nil
}

// FindOne finds a single document matching the filter.
// Returns nil if no document matches.
func (c *Collection) FindOne(filter *Document) (*Document, error) {
	if filter == nil {
		filter = bson.NewDocument()
	}

	// Try index lookup
	if spec, prefixKey, ok := c.db.db.idxCat.FindBestIndex(c.db.name, c.coll.Name(), filter); ok && spec != nil {
		ids := mongo.LookupByPrefix(c.db.db.eng, prefixKey)
		matcher := mongo.NewMatcher(filter)
		for _, id := range ids {
			docKey := mongo.EncodeDocumentKey(c.db.name, c.coll.Name(), id)
			val, err := c.db.db.eng.Get(docKey)
			if err != nil {
				continue
			}
			doc, err := bson.Decode(val)
			if err != nil {
				continue
			}
			if matcher.Match(doc) {
				return doc, nil
			}
		}
		return nil, nil
	}

	// Full scan
	matcher := mongo.NewMatcher(filter)
	var result *Document
	prefix := mongo.EncodeNamespacePrefix(c.db.name, c.coll.Name())
	c.db.db.eng.Scan(prefix, func(_, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		if matcher.Match(doc) {
			result = doc
			return false
		}
		return true
	})
	return result, nil
}

// Find returns a cursor for all documents matching the filter.
// Pass nil for no filter (match all).
func (c *Collection) Find(filter *Document) (*Cursor, error) {
	if filter == nil {
		filter = bson.NewDocument()
	}

	matcher := mongo.NewMatcher(filter)
	var docs []*Document

	// Try index lookup
	if spec, prefixKey, ok := c.db.db.idxCat.FindBestIndex(c.db.name, c.coll.Name(), filter); ok && spec != nil {
		ids := mongo.LookupByPrefix(c.db.db.eng, prefixKey)
		for _, id := range ids {
			docKey := mongo.EncodeDocumentKey(c.db.name, c.coll.Name(), id)
			val, err := c.db.db.eng.Get(docKey)
			if err != nil {
				continue
			}
			doc, err := bson.Decode(val)
			if err != nil {
				continue
			}
			if matcher.Match(doc) {
				docs = append(docs, doc)
			}
		}
	} else {
		prefix := mongo.EncodeNamespacePrefix(c.db.name, c.coll.Name())
		c.db.db.eng.Scan(prefix, func(_, value []byte) bool {
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}
			if matcher.Match(doc) {
				docs = append(docs, doc)
			}
			return true
		})
	}

	return &Cursor{docs: docs, pos: -1}, nil
}

// UpdateOne updates the first document matching the filter.
// Returns matched and modified counts.
func (c *Collection) UpdateOne(filter, update *Document) (matched, modified int, err error) {
	if filter == nil {
		filter = bson.NewDocument()
	}

	matcher := mongo.NewMatcher(filter)
	prefix := mongo.EncodeNamespacePrefix(c.db.name, c.coll.Name())

	type matchEntry struct {
		key []byte
		doc *Document
	}
	var matches []matchEntry

	c.db.db.eng.Scan(prefix, func(key, value []byte) bool {
		if len(matches) > 0 {
			return false
		}
		doc, derr := bson.Decode(value)
		if derr != nil {
			return true
		}
		if matcher.Match(doc) {
			matches = append(matches, matchEntry{key: append([]byte{}, key...), doc: doc})
		}
		return true
	})

	for _, m := range matches {
		matched++
		newDoc := mongo.ApplyUpdate(m.doc, update, false)
		if idVal, ok := m.doc.Get("_id"); ok {
			newDoc.Set("_id", idVal)
			if err := c.coll.ReplaceByKey(m.key, newDoc); err == nil {
				modified++
				c.db.db.idxCat.OnDocumentUpdate(c.db.name, c.coll.Name(), m.doc, newDoc)
			}
		}
	}
	return matched, modified, nil
}

// DeleteOne deletes the first document matching the filter.
// Returns the number of deleted documents.
func (c *Collection) DeleteOne(filter *Document) (deleted int, err error) {
	if filter == nil {
		filter = bson.NewDocument()
	}

	// Capped collections don't allow explicit deletes
	if c.info != nil && c.info.Capped {
		return 0, nil
	}

	matcher := mongo.NewMatcher(filter)
	prefix := mongo.EncodeNamespacePrefix(c.db.name, c.coll.Name())

	var keys [][]byte
	var docs []*Document
	c.db.db.eng.Scan(prefix, func(key, value []byte) bool {
		if len(keys) > 0 {
			return false
		}
		doc, derr := bson.Decode(value)
		if derr != nil {
			return true
		}
		if matcher.Match(doc) {
			keys = append(keys, append([]byte{}, key...))
			docs = append(docs, doc)
		}
		return true
	})

	for i, k := range keys {
		if err := c.db.db.eng.Delete(k); err == nil {
			deleted++
			c.db.db.idxCat.OnDocumentDelete(c.db.name, c.coll.Name(), docs[i])
		}
	}
	return deleted, nil
}

// Count returns the number of documents matching the filter.
// Pass nil for total count.
func (c *Collection) Count(filter *Document) (int64, error) {
	if filter == nil {
		return c.coll.Count()
	}
	matcher := mongo.NewMatcher(filter)
	var count int64
	prefix := mongo.EncodeNamespacePrefix(c.db.name, c.coll.Name())
	c.db.db.eng.Scan(prefix, func(_, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		if matcher.Match(doc) {
			count++
		}
		return true
	})
	return count, nil
}

// --- Cursor ---

// Next advances the cursor to the next document.
// Returns false when there are no more documents.
func (cur *Cursor) Next() bool {
	if cur.docs == nil {
		return false
	}
	cur.pos++
	return cur.pos < len(cur.docs)
}

// Decode decodes the current document into the provided Document.
func (cur *Cursor) Decode(doc *Document) error {
	if cur.pos < 0 || cur.pos >= len(cur.docs) {
		return mongo.ErrNotFound
	}
	// Copy values
	for _, e := range cur.docs[cur.pos].Elements() {
		doc.Set(e.Key, e.Value)
	}
	return nil
}

// Close releases cursor resources (no-op for embedded cursors).
func (cur *Cursor) Close() error {
	cur.docs = nil
	cur.pos = -1
	return nil
}

// All returns all documents from the cursor.
func (cur *Cursor) All() []*Document {
	return cur.docs
}
