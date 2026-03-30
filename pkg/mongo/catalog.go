package mongo

import (
	"encoding/json"
	"sync"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// DatabaseInfo holds metadata about a database.
type DatabaseInfo struct {
	Name string `json:"name"`
}

// CollectionInfo holds metadata about a collection.
type CollectionInfo struct {
	DB   string `json:"db"`
	Name string `json:"name"`
}

// Catalog manages database and collection metadata, backed by the KV engine.
type Catalog struct {
	mu     sync.RWMutex
	engine *engine.Engine
}

// NewCatalog creates a new Catalog backed by the given engine.
func NewCatalog(eng *engine.Engine) *Catalog {
	return &Catalog{engine: eng}
}

// --- Database operations ---

// CreateDatabase creates a new database entry in the catalog.
func (c *Catalog) CreateDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := encodeCatalogKeyDB(name)
	if _, err := c.engine.Get(key); err == nil {
		return ErrNamespaceExists
	}

	info := DatabaseInfo{Name: name}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return c.engine.Put(key, data)
}

// DropDatabase removes a database and all its collections from the catalog.
func (c *Catalog) DropDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := encodeCatalogKeyDB(name)
	if _, err := c.engine.Get(key); err != nil {
		return ErrNamespaceNotFound
	}

	// Drop all collections in this database
	colls := c.listCollectionsLocked(name)
	for _, coll := range colls {
		collKey := encodeCatalogKeyColl(name, coll.Name)
		c.engine.Delete(collKey)
	}

	return c.engine.Delete(key)
}

// ListDatabases returns all database names.
func (c *Catalog) ListDatabases() ([]DatabaseInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.listDatabasesLocked()
}

func (c *Catalog) listDatabasesLocked() ([]DatabaseInfo, error) {
	prefix := []byte(catalogPrefix + string([]byte{catalogTypeDB}))
	var dbs []DatabaseInfo
	err := c.engine.Scan(prefix, func(key, value []byte) bool {
		var info DatabaseInfo
		if err := json.Unmarshal(value, &info); err == nil {
			dbs = append(dbs, info)
		}
		return true
	})
	return dbs, err
}

// GetDatabase returns database info or ErrNamespaceNotFound.
func (c *Catalog) GetDatabase(name string) (DatabaseInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := encodeCatalogKeyDB(name)
	data, err := c.engine.Get(key)
	if err != nil {
		return DatabaseInfo{}, ErrNamespaceNotFound
	}
	var info DatabaseInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return DatabaseInfo{}, err
	}
	return info, nil
}

// --- Collection operations ---

// CreateCollection creates a new collection in the catalog.
func (c *Catalog) CreateCollection(db, coll string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Ensure database exists
	dbKey := encodeCatalogKeyDB(db)
	if _, err := c.engine.Get(dbKey); err != nil {
		return ErrNamespaceNotFound
	}

	collKey := encodeCatalogKeyColl(db, coll)
	if _, err := c.engine.Get(collKey); err == nil {
		return ErrNamespaceExists
	}

	info := CollectionInfo{DB: db, Name: coll}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return c.engine.Put(collKey, data)
}

// DropCollection removes a collection from the catalog.
func (c *Catalog) DropCollection(db, coll string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	collKey := encodeCatalogKeyColl(db, coll)
	if _, err := c.engine.Get(collKey); err != nil {
		return ErrNamespaceNotFound
	}
	return c.engine.Delete(collKey)
}

// ListCollections returns all collections in a database.
func (c *Catalog) ListCollections(db string) ([]CollectionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.listCollectionsLocked(db), nil
}

func (c *Catalog) listCollectionsLocked(db string) []CollectionInfo {
	// Scan catalog keys for collections in this database
	prefix := append([]byte(catalogPrefix), catalogTypeColl)
	var colls []CollectionInfo
	c.engine.Scan(prefix, func(key, value []byte) bool {
		var info CollectionInfo
		if err := json.Unmarshal(value, &info); err == nil && info.DB == db {
			colls = append(colls, info)
		}
		return true
	})
	return colls
}

// GetCollection returns collection info or ErrNamespaceNotFound.
func (c *Catalog) GetCollection(db, coll string) (CollectionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	collKey := encodeCatalogKeyColl(db, coll)
	data, err := c.engine.Get(collKey)
	if err != nil {
		return CollectionInfo{}, ErrNamespaceNotFound
	}
	var info CollectionInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return CollectionInfo{}, err
	}
	return info, nil
}

// --- Helpers ---

// EnsureDatabase creates a database if it doesn't exist.
func (c *Catalog) EnsureDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := encodeCatalogKeyDB(name)
	if _, err := c.engine.Get(key); err == nil {
		return nil // already exists
	}
	info := DatabaseInfo{Name: name}
	data, _ := json.Marshal(info)
	return c.engine.Put(key, data)
}

// EnsureCollection creates a collection (and its database) if they don't exist.
func (c *Catalog) EnsureCollection(db, coll string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Ensure database
	dbKey := encodeCatalogKeyDB(db)
	if _, err := c.engine.Get(dbKey); err != nil {
		info := DatabaseInfo{Name: db}
		data, _ := json.Marshal(info)
		c.engine.Put(dbKey, data)
	}

	// Ensure collection
	collKey := encodeCatalogKeyColl(db, coll)
	if _, err := c.engine.Get(collKey); err == nil {
		return nil
	}
	info := CollectionInfo{DB: db, Name: coll}
	data, _ := json.Marshal(info)
	return c.engine.Put(collKey, data)
}

// BSON helpers for catalog entries (used by wire protocol).
// ListDatabasesBSON returns listDatabases result as BSON documents.
func (c *Catalog) ListDatabasesBSON() ([]*bson.Document, error) {
	dbs, err := c.ListDatabases()
	if err != nil {
		return nil, err
	}
	result := make([]*bson.Document, len(dbs))
	for i, db := range dbs {
		doc := bson.NewDocument()
		doc.Set("name", bson.VString(db.Name))
		// Count collections
		colls, _ := c.ListCollections(db.Name)
		doc.Set("collectionsOnDisk", bson.VInt32(int32(len(colls))))
		result[i] = doc
	}
	return result, nil
}

// ListCollectionsBSON returns listCollections result as BSON documents.
func (c *Catalog) ListCollectionsBSON(db string) ([]*bson.Document, error) {
	colls, err := c.ListCollections(db)
	if err != nil {
		return nil, err
	}
	result := make([]*bson.Document, len(colls))
	for i, c := range colls {
		doc := bson.NewDocument()
		doc.Set("name", bson.VString(c.Name))
		doc.Set("type", bson.VString("collection"))
		result[i] = doc
	}
	return result, nil
}
