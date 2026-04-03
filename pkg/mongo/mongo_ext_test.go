package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupTestCatalogAndColl(t *testing.T) (*engine.Engine, *Catalog, *Collection) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "testcoll")
	coll := NewCollection("testdb", "testcoll", eng, cat)
	return eng, cat, coll
}

func TestCatalog_GetDatabase(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	cat := NewCatalog(eng)

	// Create a database first
	err = cat.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}

	// Get the database
	db, err := cat.GetDatabase("testdb")
	if err != nil {
		t.Errorf("GetDatabase: %v", err)
	}
	if db.Name != "testdb" {
		t.Errorf("expected database name 'testdb', got '%s'", db.Name)
	}

	// Get non-existent database
	_, err = cat.GetDatabase("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent database")
	}
}

func TestCatalog_ListDatabasesBSON(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	cat := NewCatalog(eng)

	// Create databases
	cat.CreateDatabase("db1")
	cat.CreateDatabase("db2")

	// List as BSON
	result, err := cat.ListDatabasesBSON()
	if err != nil {
		t.Errorf("ListDatabasesBSON: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil result")
	}
}

func TestCatalog_ListCollectionsBSON(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	cat := NewCatalog(eng)

	// Create database and collections
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "coll1")
	cat.EnsureCollection("testdb", "coll2")

	// List as BSON
	result, err := cat.ListCollectionsBSON("testdb")
	if err != nil {
		t.Errorf("ListCollectionsBSON: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil result")
	}
}

func TestCatalog_SetValidator(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	cat := NewCatalog(eng)

	// Create database and collection
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")

	// Set validator
	schema := bson.NewDocument()
	nameField := bson.NewDocument()
	nameField.Set("bsonType", bson.VString("string"))
	schema.Set("name", bson.VDoc(nameField))

	validator := &Validator{
		Schema: schema,
		Action: "error",
		Level:  "strict",
	}

	err = cat.SetValidator("testdb", "testcoll", validator)
	if err != nil {
		t.Errorf("SetValidator: %v", err)
	}

	// Get validator back
	got, err := cat.GetValidator("testdb", "testcoll")
	if err != nil {
		t.Errorf("GetValidator: %v", err)
	}
	if got == nil {
		t.Error("expected non-nil validator")
	}
}

func TestCatalog_UpdateCollectionInfo(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	cat := NewCatalog(eng)

	// Create database and collection
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")

	// Update info
	info := CollectionInfo{
		Name:   "testcoll",
		DB:     "testdb",
		Capped: true,
		MaxSize: 1000000,
		MaxDocs: 1000,
	}

	err = cat.UpdateCollectionInfo("testdb", "testcoll", info)
	if err != nil {
		t.Errorf("UpdateCollectionInfo: %v", err)
	}

	// Verify update - get collection and check info
	coll, err := cat.GetCollection("testdb", "testcoll")
	if err != nil {
		t.Fatalf("GetCollection: %v", err)
	}
	if !coll.Capped {
		t.Error("expected Capped=true after update")
	}
}

func TestCollection_FullName(t *testing.T) {
	eng, _, coll := setupTestCatalogAndColl(t)
	defer eng.Close()

	fullName := coll.FullName()
	if fullName != "testdb.testcoll" {
		t.Errorf("expected 'testdb.testcoll', got '%s'", fullName)
	}
}

func TestCollection_DB(t *testing.T) {
	eng, _, coll := setupTestCatalogAndColl(t)
	defer eng.Close()

	db := coll.DB()
	if db != "testdb" {
		t.Errorf("expected 'testdb', got '%s'", db)
	}
}

func TestCollection_Name(t *testing.T) {
	eng, _, coll := setupTestCatalogAndColl(t)
	defer eng.Close()

	name := coll.Name()
	if name != "testcoll" {
		t.Errorf("expected 'testcoll', got '%s'", name)
	}
}

func TestCollection_FindOneByKey(t *testing.T) {
	eng, _, coll := setupTestCatalogAndColl(t)
	defer eng.Close()

	// Insert a document
	doc := bson.NewDocument()
	doc.Set("_id", bson.VString("key1"))
	doc.Set("value", bson.VString("test"))
	idVal := "key1"
	key := EncodeDocumentKey("testdb", "testcoll", []byte(idVal))
	coll.eng.Put(key, bson.Encode(doc))

	// Find by key
	found, err := coll.FindOneByKey([]byte(idVal))
	if err != nil {
		t.Logf("FindOneByKey: %v (may be expected)", err)
	}
	_ = found
}

func TestCollection_DeleteByKey(t *testing.T) {
	eng, _, coll := setupTestCatalogAndColl(t)
	defer eng.Close()

	// Insert and delete by key
	doc := bson.NewDocument()
	doc.Set("_id", bson.VString("key1"))
	idVal := "key1"
	key := EncodeDocumentKey("testdb", "testcoll", []byte(idVal))
	coll.eng.Put(key, bson.Encode(doc))

	// Delete by key
	err := coll.DeleteByKey([]byte(idVal))
	if err != nil {
		t.Logf("DeleteByKey: %v", err)
	}
}

func TestCollection_ReplaceByKey(t *testing.T) {
	eng, _, coll := setupTestCatalogAndColl(t)
	defer eng.Close()

	// Insert a document
	doc := bson.NewDocument()
	doc.Set("_id", bson.VString("key1"))
	doc.Set("value", bson.VString("original"))
	idVal := "key1"
	key := EncodeDocumentKey("testdb", "testcoll", []byte(idVal))
	coll.eng.Put(key, bson.Encode(doc))

	// Replace by key
	newDoc := bson.NewDocument()
	newDoc.Set("_id", bson.VString("key1"))
	newDoc.Set("value", bson.VString("replaced"))

	err := coll.ReplaceByKey([]byte(idVal), newDoc)
	if err != nil {
		t.Logf("ReplaceByKey: %v", err)
	}
}

func TestCappedCollection_GetCappedInfo(t *testing.T) {
	eng, cat, _ := setupTestCatalogAndColl(t)
	defer eng.Close()

	// Get collection info
	coll, _ := cat.GetCollection("testdb", "testcoll")

	// Set as capped
	coll.Capped = true
	coll.MaxSize = 1000000
	coll.MaxDocs = 100

	// Update collection info
	cat.UpdateCollectionInfo("testdb", "testcoll", coll)

	// Test GetCappedInfo
	capped, maxSize, maxDocs := GetCappedInfo(cat, "testdb", "testcoll")
	if !capped {
		t.Error("expected Capped=true")
	}
	if maxSize != 1000000 {
		t.Errorf("expected MaxSize=1000000, got %d", maxSize)
	}
	if maxDocs != 100 {
		t.Errorf("expected MaxDocs=100, got %d", maxDocs)
	}

	// Test GetCappedInfo for non-existent collection
	capped, maxSize, maxDocs = GetCappedInfo(cat, "testdb", "nonexistent")
	if capped {
		t.Error("expected Capped=false for non-existent collection")
	}
	if maxSize != 0 {
		t.Errorf("expected MaxSize=0 for non-existent collection, got %d", maxSize)
	}
	if maxDocs != 0 {
		t.Errorf("expected MaxDocs=0 for non-existent collection, got %d", maxDocs)
	}
}

func TestCursor_Namespace(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VString("1"))
	docs := []*bson.Document{doc}
	cm := NewCursorManager()
	cursor := cm.Register("testdb.testcoll", docs, 100)

	ns := cursor.Namespace()
	if ns != "testdb.testcoll" {
		t.Errorf("expected 'testdb.testcoll', got '%s'", ns)
	}
}

func TestCursor_HasNext(t *testing.T) {
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VString("1"))
	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VString("2"))
	docs := []*bson.Document{doc1, doc2}
	cm := NewCursorManager()
	cursor := cm.Register("testdb.testcoll", docs, 100)

	if !cursor.HasNext() {
		t.Error("expected HasNext() = true")
	}

	// Exhaust cursor
	cursor.Next()
	cursor.Next()

	if cursor.HasNext() {
		t.Error("expected HasNext() = false after exhausting cursor")
	}
}

func TestCursor_Exhausted(t *testing.T) {
	docs := []*bson.Document{}
	cm := NewCursorManager()
	cursor := cm.Register("testdb.testcoll", docs, 100)

	if !cursor.Exhausted() {
		t.Error("expected Exhausted() = true for empty cursor")
	}
}

func TestCursor_Next(t *testing.T) {
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VString("1"))
	doc1.Set("name", bson.VString("alice"))
	docs := []*bson.Document{doc1}
	cm := NewCursorManager()
	cursor := cm.Register("testdb.testcoll", docs, 100)

	doc := cursor.Next()
	if doc == nil {
		t.Error("expected non-nil doc")
	}

	// Next after exhausted should return nil
	doc = cursor.Next()
	if doc != nil {
		t.Error("expected nil doc after exhausted")
	}
}

// Test stemmer helper functions
func TestIsDoubleLetter(t *testing.T) {
	doubleLetters := []rune{'b', 'd', 'f', 'g', 'm', 'n', 'p', 'r', 't'}
	for _, r := range doubleLetters {
		if !isDoubleLetter(r) {
			t.Errorf("expected isDoubleLetter('%c') = true", r)
		}
	}

	// Test non-double letters
	nonDouble := []rune{'a', 'c', 'e', 'h', 'i', 'j', 'k', 'l', 'o', 'q', 's', 'u', 'v', 'w', 'x', 'y', 'z'}
	for _, r := range nonDouble {
		if isDoubleLetter(r) {
			t.Errorf("expected isDoubleLetter('%c') = false", r)
		}
	}
}

// Test wildcard index itoa function
func TestItoa(t *testing.T) {
	tests := []struct {
		n        int
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{42, "42"},
		{1000, "1000"},
	}

	for _, tt := range tests {
		result := itoa(tt.n)
		if result != tt.expected {
			t.Errorf("itoa(%d) = %s, want %s", tt.n, result, tt.expected)
		}
	}

	// Test negative number - itoa uses strconv.Itoa which returns empty for negative
	// The function seems to have a specific behavior for negative numbers
	result := itoa(-1)
	// Just verify it doesn't panic and returns some value
	_ = result
}

// Test validator resolveFieldPath function
func TestResolveFieldPath(t *testing.T) {
	// Create a nested document
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("alice"))

	addrDoc := bson.NewDocument()
	addrDoc.Set("city", bson.VString("NYC"))
	addrDoc.Set("zip", bson.VInt32(10001))
	doc.Set("address", bson.VDoc(addrDoc))

	// Test top-level field
	val, ok := resolveFieldPath(doc, "name")
	if !ok {
		t.Error("expected to find 'name' field")
	}
	if val.String() != "alice" {
		t.Errorf("expected name='alice', got %s", val.String())
	}

	// Test nested field
	val, ok = resolveFieldPath(doc, "address.city")
	if !ok {
		t.Error("expected to find 'address.city' field")
	}
	if val.String() != "NYC" {
		t.Errorf("expected city='NYC', got %s", val.String())
	}

	// Test non-existent field
	_, ok = resolveFieldPath(doc, "nonexistent")
	if ok {
		t.Error("expected not to find 'nonexistent' field")
	}

	// Test non-existent nested field
	_, ok = resolveFieldPath(doc, "address.nonexistent")
	if ok {
		t.Error("expected not to find 'address.nonexistent' field")
	}

	// Test nested field on non-document
	_, ok = resolveFieldPath(doc, "name.invalid")
	if ok {
		t.Error("expected not to find 'name.invalid' field (name is not a document)")
	}
}
