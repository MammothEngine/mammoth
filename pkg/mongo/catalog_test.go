package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/engine"
)

func tempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func TestCatalogCreateDrop(t *testing.T) {
	dir := tempDir(t)
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)

	// Create database
	if err := cat.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}

	// Duplicate should fail
	if err := cat.CreateDatabase("testdb"); err != ErrNamespaceExists {
		t.Errorf("duplicate CreateDatabase = %v, want ErrNamespaceExists", err)
	}

	// Create collection
	if err := cat.CreateCollection("testdb", "users"); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Duplicate should fail
	if err := cat.CreateCollection("testdb", "users"); err != ErrNamespaceExists {
		t.Errorf("duplicate CreateCollection = %v, want ErrNamespaceExists", err)
	}

	// List
	dbs, err := cat.ListDatabases()
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(dbs) != 1 || dbs[0].Name != "testdb" {
		t.Errorf("ListDatabases = %v, want [{testdb}]", dbs)
	}

	colls, err := cat.ListCollections("testdb")
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(colls) != 1 || colls[0].Name != "users" {
		t.Errorf("ListCollections = %v, want [{users}]", colls)
	}

	// Drop collection
	if err := cat.DropCollection("testdb", "users"); err != nil {
		t.Fatalf("DropCollection: %v", err)
	}
	colls, _ = cat.ListCollections("testdb")
	if len(colls) != 0 {
		t.Errorf("ListCollections after drop = %v, want empty", colls)
	}

	// Drop database
	if err := cat.DropDatabase("testdb"); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}
	dbs, _ = cat.ListDatabases()
	if len(dbs) != 0 {
		t.Errorf("ListDatabases after drop = %v, want empty", dbs)
	}
}

func TestCatalogPersistence(t *testing.T) {
	dir := tempDir(t)
	opts := engine.DefaultOptions(dir)

	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cat := NewCatalog(eng)

	cat.CreateDatabase("mydb")
	cat.CreateCollection("mydb", "orders")
	eng.Close()

	// Reopen and verify metadata persists
	eng2, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer eng2.Close()

	cat2 := NewCatalog(eng2)
	dbs, err := cat2.ListDatabases()
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(dbs) != 1 || dbs[0].Name != "mydb" {
		t.Errorf("ListDatabases after reopen = %v, want [{mydb}]", dbs)
	}

	colls, err := cat2.ListCollections("mydb")
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(colls) != 1 || colls[0].Name != "orders" {
		t.Errorf("ListCollections after reopen = %v, want [{orders}]", colls)
	}
}

func TestCatalogEnsure(t *testing.T) {
	dir := tempDir(t)
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)

	// EnsureCollection should create both db and collection
	if err := cat.EnsureCollection("newdb", "newcoll"); err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}

	// Second call should be idempotent
	if err := cat.EnsureCollection("newdb", "newcoll"); err != nil {
		t.Fatalf("EnsureCollection (idempotent): %v", err)
	}

	info, err := cat.GetCollection("newdb", "newcoll")
	if err != nil {
		t.Fatalf("GetCollection: %v", err)
	}
	if info.DB != "newdb" || info.Name != "newcoll" {
		t.Errorf("GetCollection = %+v, want {DB:newdb, Name:newcoll}", info)
	}
}

func TestKeyCodecRoundtrip(t *testing.T) {
	key := encodeDocKey("mydb", "users", []byte("doc123"))
	db, coll, ok := decodeNamespaceFromKey(key)
	if !ok {
		t.Fatal("decodeNamespaceFromKey failed")
	}
	if db != "mydb" || coll != "users" {
		t.Errorf("decodeNamespaceFromKey = %q, %q; want mydb, users", db, coll)
	}
}
