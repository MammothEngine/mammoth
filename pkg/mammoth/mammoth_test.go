package mammoth

import (
	"fmt"
	"testing"
)

func openTestDB(t *testing.T) *Database {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestInsertOneFindOne(t *testing.T) {
	db := openTestDB(t)
	coll, err := db.Collection("users")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	id, err := coll.InsertOne(map[string]interface{}{
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}
	if id == nil {
		t.Fatal("InsertOne returned nil _id")
	}

	doc, err := coll.FindOne(map[string]interface{}{
		"name": "Alice",
	})
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}

	if doc["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", doc["name"])
	}
	if age, ok := doc["age"].(int); !ok || age != 30 {
		t.Errorf("expected age=30 (int), got %v (%T)", doc["age"], doc["age"])
	}
	if doc["_id"] == nil {
		t.Error("expected _id to be set")
	}
}

func TestFindOneNotFound(t *testing.T) {
	db := openTestDB(t)
	coll, err := db.Collection("items")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	_, err = coll.FindOne(map[string]interface{}{"name": "nonexistent"})
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestFind(t *testing.T) {
	db := openTestDB(t)
	coll, err := db.Collection("products")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	docs := []map[string]interface{}{
		{"name": "Widget", "price": 10},
		{"name": "Gadget", "price": 25},
		{"name": "Thingy", "price": 50},
	}
	ids, err := coll.InsertMany(docs)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(ids) != 3 {
		t.Errorf("expected 3 ids, got %d", len(ids))
	}

	cursor, err := coll.Find(map[string]interface{}{
		"price": map[string]interface{}{"$gt": 15},
	})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	defer cursor.Close()

	var count int
	for cursor.Next() {
		var result map[string]interface{}
		if err := cursor.Decode(&result); err != nil {
			t.Errorf("Decode: %v", err)
		}
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 results, got %d", count)
	}
}

func TestCount(t *testing.T) {
	db := openTestDB(t)
	coll, err := db.Collection("orders")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	coll.InsertOne(map[string]interface{}{"status": "pending"})
	coll.InsertOne(map[string]interface{}{"status": "pending"})
	coll.InsertOne(map[string]interface{}{"status": "shipped"})

	count, err := coll.Count(map[string]interface{}{"status": "pending"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Errorf("expected count=2, got %d", count)
	}

	total, err := coll.Count(nil)
	if err != nil {
		t.Fatalf("Count(nil): %v", err)
	}
	if total != 3 {
		t.Errorf("expected total=3, got %d", total)
	}
}

func TestDeleteOne(t *testing.T) {
	db := openTestDB(t)
	coll, err := db.Collection("tasks")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	coll.InsertOne(map[string]interface{}{"title": "Task A"})
	coll.InsertOne(map[string]interface{}{"title": "Task B"})

	deleted, err := coll.DeleteOne(map[string]interface{}{"title": "Task A"})
	if err != nil {
		t.Fatalf("DeleteOne: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected deleted=1, got %d", deleted)
	}

	// Verify it's gone
	_, err = coll.FindOne(map[string]interface{}{"title": "Task A"})
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}

	// Verify the other one still exists
	doc, err := coll.FindOne(map[string]interface{}{"title": "Task B"})
	if err != nil {
		t.Fatalf("FindOne Task B: %v", err)
	}
	if doc["title"] != "Task B" {
		t.Errorf("expected title=Task B, got %v", doc["title"])
	}
}

func TestUpdateOne(t *testing.T) {
	db := openTestDB(t)
	coll, err := db.Collection("people")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	coll.InsertOne(map[string]interface{}{"name": "Bob", "age": 25})

	modified, err := coll.UpdateOne(
		map[string]interface{}{"name": "Bob"},
		map[string]interface{}{"age": 26},
	)
	if err != nil {
		t.Fatalf("UpdateOne: %v", err)
	}
	if modified != 1 {
		t.Errorf("expected modified=1, got %d", modified)
	}

	doc, err := coll.FindOne(map[string]interface{}{"name": "Bob"})
	if err != nil {
		t.Fatalf("FindOne after update: %v", err)
	}
	if age, ok := doc["age"].(int); !ok || age != 26 {
		t.Errorf("expected age=26 after update, got %v", doc["age"])
	}
}

func TestUpdateOneWithSet(t *testing.T) {
	db := openTestDB(t)
	coll, err := db.Collection("records")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	coll.InsertOne(map[string]interface{}{"key": "k1", "value": "old"})

	modified, err := coll.UpdateOne(
		map[string]interface{}{"key": "k1"},
		map[string]interface{}{
			"$set": map[string]interface{}{"value": "new"},
		},
	)
	if err != nil {
		t.Fatalf("UpdateOne $set: %v", err)
	}
	if modified != 1 {
		t.Errorf("expected modified=1, got %d", modified)
	}

	doc, err := coll.FindOne(map[string]interface{}{"key": "k1"})
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	if doc["value"] != "new" {
		t.Errorf("expected value=new, got %v", doc["value"])
	}
}

func TestListCollections(t *testing.T) {
	db := openTestDB(t)

	coll1, _ := db.Collection("alpha")
	coll1.InsertOne(map[string]interface{}{"x": 1})

	coll2, _ := db.Collection("beta")
	coll2.InsertOne(map[string]interface{}{"y": 2})

	names, err := db.ListCollections()
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(names) != 2 {
		t.Errorf("expected 2 collections, got %d: %v", len(names), names)
	}

	found := map[string]bool{}
	for _, n := range names {
		found[n] = true
	}
	if !found["alpha"] || !found["beta"] {
		t.Errorf("expected alpha and beta, got %v", names)
	}
}

func TestDropCollection(t *testing.T) {
	db := openTestDB(t)

	coll, _ := db.Collection("temp_data")
	coll.InsertOne(map[string]interface{}{"val": 42})

	if err := db.DropCollection("temp_data"); err != nil {
		t.Fatalf("DropCollection: %v", err)
	}

	names, _ := db.ListCollections()
	for _, n := range names {
		if n == "temp_data" {
			t.Error("temp_data should have been dropped")
		}
	}
}

func TestInsertMany(t *testing.T) {
	db := openTestDB(t)
	coll, err := db.Collection("batch")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	docs := []map[string]interface{}{
		{"seq": 1},
		{"seq": 2},
		{"seq": 3},
	}
	ids, err := coll.InsertMany(docs)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 ids, got %d", len(ids))
	}

	count, err := coll.Count(nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 documents, got %d", count)
	}
}

func TestOpenWithOptions(t *testing.T) {
	opts := Options{
		DataDir:      t.TempDir(),
		MemtableSize: 1024 * 1024,
		CacheSize:    500,
	}
	db, err := OpenWithOptions(opts)
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer db.Close()

	coll, err := db.Collection("test")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	id, err := coll.InsertOne(map[string]interface{}{"ok": true})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}
	if id == nil {
		t.Error("expected non-nil id")
	}
}

func TestFindWithOptions(t *testing.T) {
	db := openTestDB(t)
	coll, _ := db.Collection("items")

	// Insert test data
	for i := 1; i <= 10; i++ {
		coll.InsertOne(map[string]interface{}{
			"value": i,
			"name":  fmt.Sprintf("item%d", i),
		})
	}

	// Test with filter only
	opts := FindOptions{
		Filter: map[string]interface{}{"value": map[string]interface{}{"$gte": 5}},
	}
	cursor, err := coll.FindWithOptions(opts)
	if err != nil {
		t.Fatalf("FindWithOptions: %v", err)
	}
	defer cursor.Close()

	count := 0
	for cursor.Next() {
		count++
	}
	if count != 6 { // values 5,6,7,8,9,10
		t.Errorf("expected 6 results with filter, got %d", count)
	}

	// Test with sort
	opts = FindOptions{
		Sort: map[string]interface{}{"value": -1}, // descending
	}
	cursor, _ = coll.FindWithOptions(opts)
	defer cursor.Close()

	var firstValue int
	if cursor.Next() {
		var doc map[string]interface{}
		cursor.Decode(&doc)
		firstValue = doc["value"].(int)
	}
	if firstValue != 10 {
		t.Errorf("expected first value=10 with desc sort, got %d", firstValue)
	}

	// Test with limit
	opts = FindOptions{
		Limit: 3,
	}
	cursor, _ = coll.FindWithOptions(opts)
	defer cursor.Close()

	count = 0
	for cursor.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 results with limit, got %d", count)
	}

	// Test with skip
	opts = FindOptions{
		Skip:  5,
		Limit: 3,
	}
	cursor, _ = coll.FindWithOptions(opts)
	defer cursor.Close()

	count = 0
	for cursor.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 results with skip+limit, got %d", count)
	}
}

func TestReplaceOne(t *testing.T) {
	db := openTestDB(t)
	coll, _ := db.Collection("replace_test")

	// Insert document
	coll.InsertOne(map[string]interface{}{"key": "k1", "value": "old", "keep": true})

	// Replace it
	replaced, err := coll.ReplaceOne(
		map[string]interface{}{"key": "k1"},
		map[string]interface{}{"key": "k1", "value": "new"},
	)
	if err != nil {
		t.Fatalf("ReplaceOne: %v", err)
	}
	if replaced != 1 {
		t.Errorf("expected replaced=1, got %d", replaced)
	}

	// Verify replacement
	doc, _ := coll.FindOne(map[string]interface{}{"key": "k1"})
	if doc["value"] != "new" {
		t.Errorf("expected value=new, got %v", doc["value"])
	}
	if _, ok := doc["keep"]; ok {
		t.Error("expected 'keep' field to be removed after replace")
	}

	// Replace non-existent
	replaced, _ = coll.ReplaceOne(
		map[string]interface{}{"key": "nonexistent"},
		map[string]interface{}{"key": "nonexistent", "value": "x"},
	)
	if replaced != 0 {
		t.Errorf("expected replaced=0 for non-existent, got %d", replaced)
	}
}

func TestDeleteMany(t *testing.T) {
	db := openTestDB(t)
	coll, _ := db.Collection("delete_many_test")

	// Insert documents
	for i := 0; i < 10; i++ {
		coll.InsertOne(map[string]interface{}{
			"group": "A",
			"num":   i,
		})
	}
	for i := 0; i < 5; i++ {
		coll.InsertOne(map[string]interface{}{
			"group": "B",
			"num":   i,
		})
	}

	// Delete all in group A
	deleted, err := coll.DeleteMany(map[string]interface{}{"group": "A"})
	if err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}
	if deleted != 10 {
		t.Errorf("expected deleted=10, got %d", deleted)
	}

	// Verify
	countA, _ := coll.Count(map[string]interface{}{"group": "A"})
	if countA != 0 {
		t.Errorf("expected 0 remaining in group A, got %d", countA)
	}
	countB, _ := coll.Count(map[string]interface{}{"group": "B"})
	if countB != 5 {
		t.Errorf("expected 5 remaining in group B, got %d", countB)
	}
}

func TestCreateIndex(t *testing.T) {
	db := openTestDB(t)
	_, _ = db.Collection("indexed_coll") // Ensure collection exists

	// Create index
	name, err := db.CreateIndex("indexed_coll", map[string]interface{}{
		"email": 1,
	}, IndexOptions{Unique: true})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if name != "email_1" {
		t.Errorf("expected index name email_1, got %s", name)
	}

	// List indexes
	indexes, err := db.ListIndexes("indexed_coll")
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}

	found := false
	for _, idx := range indexes {
		if idx.Name == "email_1" {
			found = true
			if !idx.Unique {
				t.Error("expected index to be unique")
			}
		}
	}
	if !found {
		t.Errorf("email_1 index not found in list: %v", indexes)
	}

	// Drop index
	if err := db.DropIndex("indexed_coll", "email_1"); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}

	// Verify dropped
	indexes, _ = db.ListIndexes("indexed_coll")
	for _, idx := range indexes {
		if idx.Name == "email_1" {
			t.Error("email_1 index should have been dropped")
		}
	}
}
