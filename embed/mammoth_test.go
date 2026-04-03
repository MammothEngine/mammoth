package mammoth

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func setupEmbeddedTest(t *testing.T) (*DB, func()) {
	t.Helper()
	dir := filepath.Join(os.TempDir(), "mammoth_embedded_test")
	os.RemoveAll(dir)
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db, cleanup
}

func TestEmbeddedOpenClose(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	if db == nil {
		t.Fatal("expected non-nil DB")
	}
	if db.Engine() == nil {
		t.Error("expected Engine to be accessible")
	}
}

func TestEmbeddedInsertAndFind(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	users := db.Database("myapp").Collection("users")

	// Insert
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("Ersin"))
	doc.Set("age", bson.VInt32(30))
	if err := users.InsertOne(doc); err != nil {
		t.Fatal(err)
	}

	// Find
	filter := bson.NewDocument()
	filter.Set("name", bson.VString("Ersin"))
	result, err := users.FindOne(filter)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected to find document")
	}
	if name, ok := result.Get("name"); !ok || name.String() != "Ersin" {
		t.Error("expected name=Ersin")
	}
}

func TestEmbeddedInsertMany(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	users := db.Database("myapp").Collection("users")

	docs := []*bson.Document{
		bson.D("_id", bson.VInt32(1), "name", bson.VString("Alice"), "age", bson.VInt32(25)),
		bson.D("_id", bson.VInt32(2), "name", bson.VString("Bob"), "age", bson.VInt32(30)),
		bson.D("_id", bson.VInt32(3), "name", bson.VString("Charlie"), "age", bson.VInt32(35)),
	}
	if err := users.InsertMany(docs); err != nil {
		t.Fatal(err)
	}

	// Count
	count, err := users.Count(nil)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("expected count=3, got %d", count)
	}

	// Find with cursor
	cur, err := users.Find(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cur.Close()

	var found int
	for cur.Next() {
		found++
	}
	if found != 3 {
		t.Errorf("expected 3 docs from cursor, got %d", found)
	}
}

func TestEmbeddedUpdate(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	users := db.Database("myapp").Collection("users")
	users.InsertOne(bson.D("_id", bson.VInt32(1), "name", bson.VString("Alice"), "age", bson.VInt32(25)))

	filter := bson.D("_id", bson.VInt32(1))
	update := bson.D("$set", bson.VDoc(bson.D("age", bson.VInt32(26))))

	matched, modified, err := users.UpdateOne(filter, update)
	if err != nil {
		t.Fatal(err)
	}
	if matched != 1 {
		t.Errorf("expected matched=1, got %d", matched)
	}
	if modified != 1 {
		t.Errorf("expected modified=1, got %d", modified)
	}

	// Verify update
	result, _ := users.FindOne(filter)
	if age, ok := result.Get("age"); !ok || age.Int32() != 26 {
		t.Error("expected age=26 after update")
	}
}

func TestEmbeddedDelete(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	users := db.Database("myapp").Collection("users")
	users.InsertOne(bson.D("_id", bson.VInt32(1), "name", bson.VString("Alice")))

	deleted, err := users.DeleteOne(bson.D("_id", bson.VInt32(1)))
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Errorf("expected deleted=1, got %d", deleted)
	}

	// Verify deletion
	result, _ := users.FindOne(bson.D("_id", bson.VInt32(1)))
	if result != nil {
		t.Error("expected nil after delete")
	}
}

func TestEmbeddedIndexLifecycle(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	myapp := db.Database("myapp")
	users := myapp.Collection("users")

	// Create index
	err := myapp.CreateIndex("users", mongo.IndexSpec{
		Name: "name_idx",
		Key:  []mongo.IndexKey{{Field: "name"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert docs after index creation
	users.InsertOne(bson.D("name", bson.VString("Alice"), "age", bson.VInt32(25)))
	users.InsertOne(bson.D("name", bson.VString("Bob"), "age", bson.VInt32(30)))

	// Find by indexed field
	result, _ := users.FindOne(bson.D("name", bson.VString("Bob")))
	if result == nil {
		t.Fatal("expected to find Bob via index")
	}
	if name, ok := result.Get("name"); !ok || name.String() != "Bob" {
		t.Error("expected name=Bob")
	}

	// Update indexed field
	filter := bson.D("name", bson.VString("Bob"))
	update := bson.D("$set", bson.VDoc(bson.D("name", bson.VString("Robert"))))
	users.UpdateOne(filter, update)

	// Find by new name
	result2, _ := users.FindOne(bson.D("name", bson.VString("Robert")))
	if result2 == nil {
		t.Fatal("expected to find Robert after update")
	}

	// Old name should not be found
	result3, _ := users.FindOne(bson.D("name", bson.VString("Bob")))
	if result3 != nil {
		t.Error("Bob should no longer be found after update")
	}
}

func TestEmbeddedDropDatabase(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	myapp := db.Database("myapp")
	myapp.Collection("users").InsertOne(bson.D("name", bson.VString("test")))

	if err := myapp.Drop(); err != nil {
		t.Fatal(err)
	}
}

func TestEmbeddedListCollections(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	myapp := db.Database("myapp")
	myapp.Collection("users")
	myapp.Collection("orders")

	colls, err := myapp.ListCollections()
	if err != nil {
		t.Fatal(err)
	}
	if len(colls) != 2 {
		t.Errorf("expected 2 collections, got %d: %v", len(colls), colls)
	}
}

func TestEmbeddedCountWithFilter(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	users := db.Database("myapp").Collection("users")
	users.InsertMany([]*bson.Document{
		bson.D("name", bson.VString("Alice"), "age", bson.VInt32(25)),
		bson.D("name", bson.VString("Bob"), "age", bson.VInt32(30)),
		bson.D("name", bson.VString("Charlie"), "age", bson.VInt32(35)),
	})

	count, err := users.Count(bson.D("age", bson.VDoc(bson.D("$gt", bson.VInt32(28)))))
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("expected 2 docs with age>28, got %d", count)
	}
}

func TestWithDataDir(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "mammoth_embed_datadir_test")
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := Open(dir, WithDataDir(dir))
	if err != nil {
		t.Fatalf("Open with WithDataDir: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Error("expected non-nil DB")
	}
}

func TestDB_Catalog(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	cat := db.Catalog()
	if cat == nil {
		t.Error("expected non-nil Catalog")
	}
}

func TestDB_IndexCatalog(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	idxCat := db.IndexCatalog()
	if idxCat == nil {
		t.Error("expected non-nil IndexCatalog")
	}
}

func TestDB_DropDatabase(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	// Create a database and collection
	myapp := db.Database("testdropdb")
	myapp.Collection("testcoll").InsertOne(bson.D("name", bson.VString("test")))

	// Drop the database using DB.DropDatabase
	if err := db.DropDatabase("testdropdb"); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}
}

func TestDatabase_Name(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	myapp := db.Database("myapp")
	if name := myapp.Name(); name != "myapp" {
		t.Errorf("expected Name() = 'myapp', got '%s'", name)
	}
}

func TestDatabase_CreateCollection(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	myapp := db.Database("testdb")

	// Create collection explicitly
	if err := myapp.CreateCollection("explicit_coll"); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Verify collection exists
	colls, err := myapp.ListCollections()
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}

	found := false
	for _, c := range colls {
		if c == "explicit_coll" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected to find 'explicit_coll' in collections")
	}
}

func TestDatabase_DropCollection(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	myapp := db.Database("testdb")
	myapp.Collection("to_drop").InsertOne(bson.D("name", bson.VString("test")))

	// Drop the collection
	if err := myapp.DropCollection("to_drop"); err != nil {
		t.Fatalf("DropCollection: %v", err)
	}

	// Verify collection is gone
	colls, _ := myapp.ListCollections()
	for _, c := range colls {
		if c == "to_drop" {
			t.Error("expected 'to_drop' collection to be deleted")
		}
	}
}

func TestDatabase_DropIndex(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	myapp := db.Database("testdb")
	users := myapp.Collection("users")
	users.InsertOne(bson.D("name", bson.VString("test")))

	// Create an index
	if err := myapp.CreateIndex("users", mongo.IndexSpec{
		Name: "name_idx",
		Key:  []mongo.IndexKey{{Field: "name"}},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// Drop the index
	if err := myapp.DropIndex("users", "name_idx"); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}
}

func TestCollection_Name(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	myapp := db.Database("myapp")
	users := myapp.Collection("users")

	if name := users.Name(); name != "users" {
		t.Errorf("expected Name() = 'users', got '%s'", name)
	}
}

func TestCursor_Decode(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	users := db.Database("myapp").Collection("users")
	users.InsertOne(bson.D("name", bson.VString("Alice"), "age", bson.VInt32(25)))

	cur, err := users.Find(nil)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	defer cur.Close()

	// Move to first document
	if !cur.Next() {
		t.Fatal("expected at least one document")
	}

	// Decode into a new document
	decoded := bson.NewDocument()
	if err := cur.Decode(decoded); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// Verify decoded data
	if name, ok := decoded.Get("name"); !ok || name.String() != "Alice" {
		t.Error("expected name=Alice in decoded document")
	}
	if age, ok := decoded.Get("age"); !ok || age.Int32() != 25 {
		t.Error("expected age=25 in decoded document")
	}
}

func TestCursor_Decode_InvalidPosition(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	users := db.Database("myapp").Collection("users")
	users.InsertOne(bson.D("name", bson.VString("Alice")))

	cur, _ := users.Find(nil)
	defer cur.Close()

	// Try to decode before calling Next
	decoded := bson.NewDocument()
	err := cur.Decode(decoded)
	if err == nil {
		t.Error("expected error for Decode before Next")
	}

	// Move past all documents
	for cur.Next() {
	}

	// Try to decode after exhausting cursor
	err = cur.Decode(decoded)
	if err == nil {
		t.Error("expected error for Decode after exhausting cursor")
	}
}

func TestCursor_All(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	users := db.Database("myapp").Collection("users")
	users.InsertMany([]*bson.Document{
		bson.D("name", bson.VString("Alice")),
		bson.D("name", bson.VString("Bob")),
		bson.D("name", bson.VString("Charlie")),
	})

	cur, err := users.Find(nil)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	defer cur.Close()

	// Get all documents
	all := cur.All()
	if len(all) != 3 {
		t.Errorf("expected 3 documents from All(), got %d", len(all))
	}
}

func TestCursor_Next_NilDocs(t *testing.T) {
	// Cursor with nil docs
	cur := &Cursor{docs: nil, pos: -1}
	if cur.Next() {
		t.Error("expected Next() = false for nil docs")
	}
}

func TestCursor_Close(t *testing.T) {
	db, cleanup := setupEmbeddedTest(t)
	defer cleanup()

	users := db.Database("myapp").Collection("users")
	users.InsertOne(bson.D("name", bson.VString("Alice")))

	cur, _ := users.Find(nil)

	// Close should not error
	if err := cur.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	// After close, Next should return false
	if cur.Next() {
		t.Error("expected Next() = false after Close")
	}
}
