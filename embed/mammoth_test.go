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
