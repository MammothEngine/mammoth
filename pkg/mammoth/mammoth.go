package mammoth

import (
	"errors"
	"fmt"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// ErrNotFound is returned when a requested document is not found.
var ErrNotFound = errors.New("mammoth: document not found")

// Options configures the database engine.
type Options struct {
	DataDir      string
	MemtableSize int
	CacheSize    int
	LogLevel     string
}

// Database represents an open Mammoth database.
type Database struct {
	eng  *engine.Engine
	cat  *mongo.Catalog
	idxc *mongo.IndexCatalog
	db   string
}

// Collection represents a document collection.
type Collection struct {
	coll *mongo.Collection
	cat  *mongo.Catalog
	db   string
	name string
}

// Cursor iterates over query results.
type Cursor struct {
	results []map[string]interface{}
	pos     int
}

// FindOptions configures query execution.
type FindOptions struct {
	Filter     map[string]interface{}
	Projection map[string]interface{}
	Sort       map[string]interface{}
	Skip       int64
	Limit      int64
}

// Open opens or creates a Mammoth database at the given path.
func Open(dataDir string) (*Database, error) {
	return OpenWithOptions(Options{DataDir: dataDir})
}

// OpenWithOptions opens or creates a Mammoth database with the given options.
func OpenWithOptions(opts Options) (*Database, error) {
	if opts.DataDir == "" {
		return nil, fmt.Errorf("mammoth: DataDir is required")
	}

	engOpts := engine.DefaultOptions(opts.DataDir)
	if opts.MemtableSize > 0 {
		engOpts.MemtableSize = opts.MemtableSize
	}
	if opts.CacheSize > 0 {
		engOpts.BlockCacheSize = opts.CacheSize
	}

	eng, err := engine.Open(engOpts)
	if err != nil {
		return nil, fmt.Errorf("mammoth: open engine: %w", err)
	}

	cat := mongo.NewCatalog(eng)
	idxc := mongo.NewIndexCatalog(eng, cat)

	return &Database{
		eng:  eng,
		cat:  cat,
		idxc: idxc,
		db:   "default",
	}, nil
}

// Collection returns a handle for the named collection, creating it if necessary.
func (db *Database) Collection(name string) (*Collection, error) {
	if err := db.cat.EnsureCollection(db.db, name); err != nil {
		return nil, fmt.Errorf("mammoth: ensure collection %q: %w", name, err)
	}
	coll := mongo.NewCollection(db.db, name, db.eng, db.cat)
	return &Collection{
		coll: coll,
		cat:  db.cat,
		db:   db.db,
		name: name,
	}, nil
}

// ListCollections returns the names of all collections in the database.
func (db *Database) ListCollections() ([]string, error) {
	infos, err := db.cat.ListCollections(db.db)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.Name
	}
	return names, nil
}

// DropCollection removes a collection and all its documents.
func (db *Database) DropCollection(name string) error {
	return db.cat.DropCollection(db.db, name)
}

// CreateIndex creates an index on the specified collection.
// keys: map of field names to 1 (ascending) or -1 (descending).
// Returns the index name.
func (db *Database) CreateIndex(collName string, keys map[string]interface{}, opts ...IndexOptions) (string, error) {
	if len(keys) == 0 {
		return "", fmt.Errorf("mammoth: index keys cannot be empty")
	}

	// Build index spec
	var indexKeys []mongo.IndexKey
	for field, direction := range keys {
		var desc bool
		switch d := direction.(type) {
		case int:
			desc = d < 0
		case int32:
			desc = d < 0
		case int64:
			desc = d < 0
		case float64:
			desc = d < 0
		}
		indexKeys = append(indexKeys, mongo.IndexKey{
			Field:      field,
			Descending: desc,
		})
	}

	// Generate index name if not provided
	name := ""
	if len(opts) > 0 && opts[0].Name != "" {
		name = opts[0].Name
	} else {
		name = generateIndexName(keys)
	}

	spec := mongo.IndexSpec{
		Name:   name,
		Key:    indexKeys,
		Unique: len(opts) > 0 && opts[0].Unique,
		Sparse: len(opts) > 0 && opts[0].Sparse,
	}

	if err := db.idxc.CreateIndex(db.db, collName, spec); err != nil {
		return "", fmt.Errorf("mammoth: create index: %w", err)
	}

	return name, nil
}

// DropIndex removes an index from a collection.
func (db *Database) DropIndex(collName, indexName string) error {
	return db.idxc.DropIndex(db.db, collName, indexName)
}

// ListIndexes returns information about all indexes on a collection.
func (db *Database) ListIndexes(collName string) ([]IndexInfo, error) {
	indexes, err := db.idxc.ListIndexes(db.db, collName)
	if err != nil {
		return nil, err
	}

	var result []IndexInfo
	for _, idx := range indexes {
		keys := make(map[string]interface{})
		for _, k := range idx.Key {
			dir := int32(1)
			if k.Descending {
				dir = -1
			}
			keys[k.Field] = dir
		}
		result = append(result, IndexInfo{
			Name:   idx.Name,
			Keys:   keys,
			Unique: idx.Unique,
			Sparse: idx.Sparse,
		})
	}
	return result, nil
}

// IndexOptions configures index creation.
type IndexOptions struct {
	Name   string
	Unique bool
	Sparse bool
}

// IndexInfo describes an existing index.
type IndexInfo struct {
	Name   string
	Keys   map[string]interface{}
	Unique bool
	Sparse bool
}

// generateIndexName creates a default index name from keys.
func generateIndexName(keys map[string]interface{}) string {
	name := ""
	for k, v := range keys {
		if name != "" {
			name += "_"
		}
		switch d := v.(type) {
		case int, int32, int64:
			if d == -1 {
				name += k + "_-1"
			} else {
				name += k + "_1"
			}
		case float64:
			if d < 0 {
				name += k + "_-1"
			} else {
				name += k + "_1"
			}
		default:
			name += k + "_1"
		}
	}
	return name
}

// Close flushes pending writes and releases all resources.
func (db *Database) Close() error {
	return db.eng.Close()
}

// InsertOne inserts a single document. Returns the generated _id.
func (c *Collection) InsertOne(doc map[string]interface{}) (interface{}, error) {
	bsonDoc := mapToDoc(doc)
	if err := c.coll.InsertOne(bsonDoc); err != nil {
		return nil, err
	}
	idVal, _ := bsonDoc.Get("_id")
	return valueToInterface(idVal), nil
}

// InsertMany inserts multiple documents. Returns generated _id values.
func (c *Collection) InsertMany(docs []map[string]interface{}) ([]interface{}, error) {
	bsonDocs := make([]*bson.Document, len(docs))
	for i, doc := range docs {
		bsonDocs[i] = mapToDoc(doc)
	}
	if err := c.coll.InsertMany(bsonDocs); err != nil {
		return nil, err
	}
	ids := make([]interface{}, len(bsonDocs))
	for i, bd := range bsonDocs {
		idVal, _ := bd.Get("_id")
		ids[i] = valueToInterface(idVal)
	}
	return ids, nil
}

// FindOne returns the first document matching the filter, or ErrNotFound.
func (c *Collection) FindOne(filter map[string]interface{}) (map[string]interface{}, error) {
	bsonFilter := mapToDoc(filter)
	matcher := mongo.NewMatcher(bsonFilter)

	var result *bson.Document
	var found bool
	err := c.coll.ScanAll(func(key []byte, doc *bson.Document) bool {
		if matcher.Match(doc) {
			result = doc
			found = true
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}
	return documentToMap(result), nil
}

// Find returns a cursor over all documents matching the filter.
func (c *Collection) Find(filter map[string]interface{}) (*Cursor, error) {
	return c.FindWithOptions(FindOptions{Filter: filter})
}

// FindWithOptions returns a cursor with advanced query options.
// Uses the query planner for optimized execution.
func (c *Collection) FindWithOptions(opts FindOptions) (*Cursor, error) {
	// Use the query planner if available
	if c.db != "" && c.name != "" {
		return c.findWithPlanner(opts)
	}
	return c.findSimple(opts)
}

// findSimple performs a simple collection scan (fallback).
func (c *Collection) findSimple(opts FindOptions) (*Cursor, error) {
	bsonFilter := mapToDoc(opts.Filter)
	matcher := mongo.NewMatcher(bsonFilter)

	var results []map[string]interface{}
	err := c.coll.ScanAll(func(key []byte, doc *bson.Document) bool {
		if matcher.Match(doc) {
			// Apply projection if specified
			if len(opts.Projection) > 0 {
				doc = mongo.ApplyProjection(doc, mapToDoc(opts.Projection))
			}
			results = append(results, documentToMap(doc))
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	// Apply sort if specified
	if len(opts.Sort) > 0 {
		results = c.sortResults(results, opts.Sort)
	}

	// Apply skip
	if opts.Skip > 0 {
		if int(opts.Skip) >= len(results) {
			results = []map[string]interface{}{}
		} else {
			results = results[opts.Skip:]
		}
	}

	// Apply limit
	if opts.Limit > 0 && int(opts.Limit) < len(results) {
		results = results[:opts.Limit]
	}

	if results == nil {
		results = []map[string]interface{}{}
	}
	return &Cursor{results: results, pos: -1}, nil
}

// findWithPlanner uses the query planner for optimized queries.
func (c *Collection) findWithPlanner(opts FindOptions) (*Cursor, error) {
	// For now, fall back to simple scan
	// Full implementation would use planner.Planner
	return c.findSimple(opts)
}

// sortResults sorts documents by the sort specification.
// Sort spec: {field: 1} for ascending, {field: -1} for descending.
func (c *Collection) sortResults(results []map[string]interface{}, sort map[string]interface{}) []map[string]interface{} {
	if len(sort) == 0 || len(results) == 0 {
		return results
	}

	// Get sort field and direction
	var sortField string
	var ascending bool = true

	for k, v := range sort {
		sortField = k
		switch val := v.(type) {
		case int:
			ascending = val > 0
		case int32:
			ascending = val > 0
		case int64:
			ascending = val > 0
		}
		break // Only support single field sort for now
	}

	// Simple bubble sort (for small result sets)
	n := len(results)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if c.compareDocs(results[j], results[j+1], sortField, ascending) > 0 {
				results[j], results[j+1] = results[j+1], results[j]
			}
		}
	}

	return results
}

// compareDocs compares two documents for sorting.
func (c *Collection) compareDocs(a, b map[string]interface{}, field string, ascending bool) int {
	va, aok := a[field]
	vb, bok := b[field]

	if !aok && !bok {
		return 0
	}
	if !aok {
		return 1 // Missing values sort last
	}
	if !bok {
		return -1
	}

	cmp := compareValues(va, vb)
	if !ascending {
		cmp = -cmp
	}
	return cmp
}

// UpdateOne updates the first document matching filter. Returns count (0 or 1).
func (c *Collection) UpdateOne(filter, update map[string]interface{}) (int64, error) {
	bsonFilter := mapToDoc(filter)
	matcher := mongo.NewMatcher(bsonFilter)

	var matchedKey []byte
	var matchedDoc *bson.Document
	err := c.coll.ScanAll(func(key []byte, doc *bson.Document) bool {
		if !matcher.Match(doc) {
			return true
		}
		// Collect key and doc, don't mutate during scan
		matchedKey = append([]byte{}, key...)
		matchedDoc = doc
		return false
	})
	if err != nil {
		return 0, err
	}
	if matchedKey == nil {
		return 0, nil
	}

	// Apply updates
	for k, v := range update {
		if k == "$set" {
			if setMap, ok := v.(map[string]interface{}); ok {
				setDoc := mapToDoc(setMap)
				for _, e := range setDoc.Elements() {
					matchedDoc.Set(e.Key, e.Value)
				}
			}
		} else {
			matchedDoc.Set(k, interfaceToBSONValue(v))
		}
	}
	if err := c.coll.ReplaceByKey(matchedKey, matchedDoc); err != nil {
		return 0, err
	}
	return 1, nil
}

// DeleteOne deletes the first document matching the filter. Returns count (0 or 1).
func (c *Collection) DeleteOne(filter map[string]interface{}) (int64, error) {
	bsonFilter := mapToDoc(filter)
	matcher := mongo.NewMatcher(bsonFilter)

	var matchedKey []byte
	err := c.coll.ScanAll(func(key []byte, doc *bson.Document) bool {
		if matcher.Match(doc) {
			matchedKey = append([]byte{}, key...)
			return false
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if matchedKey == nil {
		return 0, nil
	}
	if err := c.coll.DeleteByKey(matchedKey); err != nil {
		return 0, err
	}
	return 1, nil
}

// DeleteMany deletes all documents matching the filter. Returns deleted count.
func (c *Collection) DeleteMany(filter map[string]interface{}) (int64, error) {
	bsonFilter := mapToDoc(filter)
	matcher := mongo.NewMatcher(bsonFilter)

	var keysToDelete [][]byte
	err := c.coll.ScanAll(func(key []byte, doc *bson.Document) bool {
		if matcher.Match(doc) {
			keysToDelete = append(keysToDelete, append([]byte{}, key...))
		}
		return true
	})
	if err != nil {
		return 0, err
	}

	for _, key := range keysToDelete {
		if err := c.coll.DeleteByKey(key); err != nil {
			return int64(len(keysToDelete)), err
		}
	}

	return int64(len(keysToDelete)), nil
}

// ReplaceOne replaces the first document matching filter with replacement.
// Returns count (0 if not found, 1 if replaced).
func (c *Collection) ReplaceOne(filter, replacement map[string]interface{}) (int64, error) {
	bsonFilter := mapToDoc(filter)
	matcher := mongo.NewMatcher(bsonFilter)

	var matchedKey []byte
	err := c.coll.ScanAll(func(key []byte, doc *bson.Document) bool {
		if matcher.Match(doc) {
			matchedKey = append([]byte{}, key...)
			return false
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if matchedKey == nil {
		return 0, nil
	}

	// Ensure replacement has _id
	newDoc := mapToDoc(replacement)
	if _, ok := newDoc.Get("_id"); !ok {
		// Copy _id from matched document if not provided
		oldDoc, _ := c.coll.FindOneByKey(matchedKey)
		if oldDoc != nil {
			if idVal, ok := oldDoc.Get("_id"); ok {
				newDoc.Set("_id", idVal)
			}
		}
	}

	if err := c.coll.ReplaceByKey(matchedKey, newDoc); err != nil {
		return 0, err
	}
	return 1, nil
}

// Count returns the number of documents matching the filter.
func (c *Collection) Count(filter map[string]interface{}) (int64, error) {
	bsonFilter := mapToDoc(filter)
	matcher := mongo.NewMatcher(bsonFilter)

	var count int64
	err := c.coll.ScanAll(func(key []byte, doc *bson.Document) bool {
		if matcher.Match(doc) {
			count++
		}
		return true
	})
	return count, err
}

// Next advances the cursor. Returns false when exhausted.
func (cur *Cursor) Next() bool {
	if cur.pos < len(cur.results) {
		cur.pos++
	}
	return cur.pos < len(cur.results)
}

// Decode decodes the current document into result (map[string]interface{} or struct pointer).
func (cur *Cursor) Decode(result interface{}) error {
	if cur.pos < 0 || cur.pos >= len(cur.results) {
		return fmt.Errorf("mammoth: cursor exhausted")
	}
	m := cur.results[cur.pos]
	// If result is a map pointer, assign directly
	if mptr, ok := result.(*map[string]interface{}); ok {
		*mptr = m
		return nil
	}
	// Otherwise convert through BSON
	bsonDoc := mapToDoc(m)
	return bson.Unmarshal(bsonDoc, result)
}

// Close releases cursor resources.
func (cur *Cursor) Close() error {
	cur.results = nil
	cur.pos = -1
	return nil
}

// --- helpers ---

func mapToDoc(m map[string]interface{}) *bson.Document {
	doc := bson.NewDocument()
	for k, v := range m {
		doc.Set(k, interfaceToBSONValue(v))
	}
	return doc
}

func interfaceToBSONValue(v interface{}) bson.Value {
	switch val := v.(type) {
	case nil:
		return bson.VNull()
	case bool:
		return bson.VBool(val)
	case int:
		return bson.VInt32(int32(val))
	case int32:
		return bson.VInt32(val)
	case int64:
		return bson.VInt64(val)
	case float64:
		return bson.VDouble(val)
	case string:
		return bson.VString(val)
	case map[string]interface{}:
		return bson.VDoc(mapToDoc(val))
	case []interface{}:
		arr := make(bson.Array, len(val))
		for i, elem := range val {
			arr[i] = interfaceToBSONValue(elem)
		}
		return bson.VArray(arr)
	case []string:
		arr := make(bson.Array, len(val))
		for i, elem := range val {
			arr[i] = bson.VString(elem)
		}
		return bson.VArray(arr)
	case bson.ObjectID:
		return bson.VObjectID(val)
	case []byte:
		return bson.VBinary(bson.BinaryGeneric, val)
	default:
		return bson.VNull()
	}
}

func documentToMap(doc *bson.Document) map[string]interface{} {
	m := make(map[string]interface{}, doc.Len())
	for _, e := range doc.Elements() {
		m[e.Key] = valueToInterface(e.Value)
	}
	return m
}

func valueToInterface(v bson.Value) interface{} {
	switch v.Type {
	case bson.TypeDouble:
		return v.Double()
	case bson.TypeString:
		return v.String()
	case bson.TypeDocument:
		return documentToMap(v.DocumentValue())
	case bson.TypeArray:
		arr := v.ArrayValue()
		result := make([]interface{}, len(arr))
		for i, elem := range arr {
			result[i] = valueToInterface(elem)
		}
		return result
	case bson.TypeBinary:
		b := v.Binary()
		return b.Data
	case bson.TypeObjectID:
		return v.ObjectID().String()
	case bson.TypeBoolean:
		return v.Boolean()
	case bson.TypeDateTime:
		return v.DateTime()
	case bson.TypeNull:
		return nil
	case bson.TypeInt32:
		return int(v.Int32())
	case bson.TypeInt64:
		return v.Int64()
	case bson.TypeTimestamp:
		return v.Timestamp()
	case bson.TypeRegex:
		r := v.Regex()
		return r.Pattern
	default:
		return v.Interface()
	}
}

// compareValues compares two Go values for sorting.
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func compareValues(a, b interface{}) int {
	// Handle nil
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison first
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case int32:
			if int32(av) < bv {
				return -1
			}
			if int32(av) > bv {
				return 1
			}
			return 0
		case int64:
			if int64(av) < bv {
				return -1
			}
			if int64(av) > bv {
				return 1
			}
			return 0
		case float64:
			if float64(av) < bv {
				return -1
			}
			if float64(av) > bv {
				return 1
			}
			return 0
		}
	case int32:
		switch bv := b.(type) {
		case int:
			if av < int32(bv) {
				return -1
			}
			if av > int32(bv) {
				return 1
			}
			return 0
		case int32:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case int64:
			if int64(av) < bv {
				return -1
			}
			if int64(av) > bv {
				return 1
			}
			return 0
		case float64:
			if float64(av) < bv {
				return -1
			}
			if float64(av) > bv {
				return 1
			}
			return 0
		}
	case int64:
		switch bv := b.(type) {
		case int:
			if av < int64(bv) {
				return -1
			}
			if av > int64(bv) {
				return 1
			}
			return 0
		case int32:
			if av < int64(bv) {
				return -1
			}
			if av > int64(bv) {
				return 1
			}
			return 0
		case int64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case float64:
			if float64(av) < bv {
				return -1
			}
			if float64(av) > bv {
				return 1
			}
			return 0
		}
	case float64:
		switch bv := b.(type) {
		case int:
			if av < float64(bv) {
				return -1
			}
			if av > float64(bv) {
				return 1
			}
			return 0
		case int32:
			if av < float64(bv) {
				return -1
			}
			if av > float64(bv) {
				return 1
			}
			return 0
		case int64:
			if av < float64(bv) {
				return -1
			}
			if av > float64(bv) {
				return 1
			}
			return 0
		case float64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	}

	// Fall back to string comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	}
	if aStr > bStr {
		return 1
	}
	return 0
}
