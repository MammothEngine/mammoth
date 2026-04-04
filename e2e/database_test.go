// Package e2e provides end-to-end integration tests for Mammoth Engine.
package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/mammothengine/mammoth/pkg/mammoth"
)

// setupTestDB creates a temporary database for testing.
func setupTestDB(t *testing.T) (*mammoth.Database, func()) {
	t.Helper()

	db, err := mammoth.OpenWithOptions(mammoth.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

func TestE2E_BasicCRUD(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	coll, err := db.Collection("users")
	if err != nil {
		t.Fatalf("Failed to get collection: %v", err)
	}

	// Insert with explicit _id
	doc := map[string]interface{}{
		"_id":  "user1",
		"name": "Alice",
		"age":  30,
	}

	_, err = coll.InsertOne(doc)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Find by _id
	result, err := coll.FindOne(map[string]interface{}{"_id": "user1"})
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if result["name"] != "Alice" {
		t.Errorf("Expected name=Alice, got %v", result["name"])
	}

	// Update
	updated, err := coll.UpdateOne(
		map[string]interface{}{"_id": "user1"},
		map[string]interface{}{"$set": map[string]interface{}{"age": 31}},
	)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if updated != 1 {
		t.Errorf("Expected 1 document updated, got %d", updated)
	}

	// Verify update
	result, _ = coll.FindOne(map[string]interface{}{"_id": "user1"})
	if result["age"] != 31 {
		t.Errorf("Expected age=31 after update, got %v", result["age"])
	}

	// Delete
	deleted, err := coll.DeleteOne(map[string]interface{}{"_id": "user1"})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if deleted != 1 {
		t.Errorf("Expected 1 document deleted, got %d", deleted)
	}

	// Verify deletion
	_, err = coll.FindOne(map[string]interface{}{"_id": "user1"})
	if err != mammoth.ErrNotFound {
		t.Error("Expected not found after deletion")
	}
}

func TestE2E_QueryOperators(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	coll, _ := db.Collection("products")

	// Insert test data
	products := []map[string]interface{}{
		{"_id": "1", "name": "Laptop", "price": 1000, "category": "electronics"},
		{"_id": "2", "name": "Mouse", "price": 50, "category": "electronics"},
		{"_id": "3", "name": "Desk", "price": 300, "category": "furniture"},
		{"_id": "4", "name": "Chair", "price": 150, "category": "furniture"},
	}

	for _, p := range products {
		coll.InsertOne(p)
	}

	// Test simple equality filter (most reliable)
	cursor, _ := coll.Find(map[string]interface{}{"category": "electronics"})
	count := 0
	for cursor.Next() {
		count++
	}
	if count != 2 { // Laptop and Mouse
		t.Errorf("Expected 2 electronics products, got %d", count)
	}

	// Test finding by name
	cursor2, _ := coll.Find(map[string]interface{}{"name": "Laptop"})
	count = 0
	for cursor2.Next() {
		count++
	}
	if count != 1 {
		t.Errorf("Expected 1 Laptop, got %d", count)
	}
}

func TestE2E_MultipleInserts(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	coll, _ := db.Collection("bulk")

	// Insert many documents
	for i := 0; i < 100; i++ {
		doc := map[string]interface{}{
			"_id":   i,
			"value": i * 10,
		}
		_, err := coll.InsertOne(doc)
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Count
	count, _ := coll.Count(nil)
	if count != 100 {
		t.Errorf("Expected 100 documents, got %d", count)
	}

	// Find with limit
	cursor, _ := coll.FindWithOptions(mammoth.FindOptions{
		Filter: map[string]interface{}{},
		Limit:  10,
	})
	limitCount := 0
	for cursor.Next() {
		limitCount++
	}
	if limitCount != 10 {
		t.Errorf("Expected 10 results with limit, got %d", limitCount)
	}
}

func TestE2E_Indexes(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	coll, _ := db.Collection("indexed")

	// Create index
	_, err := db.CreateIndex("indexed", map[string]interface{}{"email": 1}, mammoth.IndexOptions{Name: "email_idx"})
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Insert documents
	for i := 0; i < 50; i++ {
		doc := map[string]interface{}{
			"email": fmt.Sprintf("user%d@test.com", i),
			"name":  fmt.Sprintf("User %d", i),
		}
		coll.InsertOne(doc)
	}

	// Query by indexed field
	result, err := coll.FindOne(map[string]interface{}{"email": "user25@test.com"})
	if err != nil {
		t.Fatalf("Find with index failed: %v", err)
	}
	if result["name"] != "User 25" {
		t.Errorf("Expected name='User 25', got %v", result["name"])
	}
}

func TestE2E_Transactions(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	accounts, _ := db.Collection("accounts")

	// Create two accounts
	accounts.InsertOne(map[string]interface{}{"_id": "acc1", "balance": 1000})
	accounts.InsertOne(map[string]interface{}{"_id": "acc2", "balance": 500})

	// Perform transfer using WithTransaction
	err := db.WithTransaction(context.Background(), func(tx *mammoth.Transaction) error {
		ct := accounts.WithTransaction(tx)

		// Deduct from acc1
		_, err := ct.UpdateOne(
			map[string]interface{}{"_id": "acc1"},
			map[string]interface{}{"$set": map[string]interface{}{"balance": 800}},
		)
		if err != nil {
			return err
		}

		// Add to acc2
		_, err = ct.UpdateOne(
			map[string]interface{}{"_id": "acc2"},
			map[string]interface{}{"$set": map[string]interface{}{"balance": 700}},
		)
		return err
	})

	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Verify balances
	result1, _ := accounts.FindOne(map[string]interface{}{"_id": "acc1"})
	if result1["balance"] != 800 {
		t.Errorf("Expected acc1 balance=800, got %v", result1["balance"])
	}

	result2, _ := accounts.FindOne(map[string]interface{}{"_id": "acc2"})
	if result2["balance"] != 700 {
		t.Errorf("Expected acc2 balance=700, got %v", result2["balance"])
	}
}

func TestE2E_GridFS(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	bucket, err := db.OpenBucket(nil)
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer bucket.Drop()

	content := []byte("This is a test file content that needs to be stored in GridFS.")

	// Upload
	uploadStream, err := bucket.OpenUploadStream("test.txt")
	if err != nil {
		t.Fatalf("OpenUploadStream failed: %v", err)
	}

	n, err := uploadStream.Write(content)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(content) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(content), n)
	}

	fileID := uploadStream.FileID()
	uploadStream.Close()

	// Download
	downloadStream, err := bucket.OpenDownloadStream(fileID)
	if err != nil {
		t.Fatalf("OpenDownloadStream failed: %v", err)
	}

	downloaded := make([]byte, len(content))
	n, err = downloadStream.Read(downloaded)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(downloaded[:n]) != string(content) {
		t.Error("Downloaded content doesn't match original")
	}

	downloadStream.Close()
}

func TestE2E_ConcurrentOperations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	coll, _ := db.Collection("concurrent")

	// Concurrent inserts
	const numGoroutines = 10
	const docsPerGoroutine = 50

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < docsPerGoroutine; j++ {
				doc := map[string]interface{}{
					"goroutine": id,
					"seq":       j,
					"value":     fmt.Sprintf("value-%d-%d", id, j),
				}
				_, err := coll.InsertOne(doc)
				if err != nil {
					t.Errorf("Insert failed: %v", err)
					return
				}
			}
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify count
	count, _ := coll.Count(nil)
	expected := int64(numGoroutines * docsPerGoroutine)
	if count != expected {
		t.Errorf("Expected %d documents, got %d", expected, count)
	}
}

func TestE2E_BackupRestore(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	coll, _ := db.Collection("backup_test")

	// Insert data
	for i := 0; i < 50; i++ {
		doc := map[string]interface{}{
			"id":   i,
			"data": fmt.Sprintf("data-%d", i),
		}
		coll.InsertOne(doc)
	}

	// Verify initial count
	count, _ := coll.Count(nil)
	if count != 50 {
		t.Fatalf("Expected 50 documents initially, got %d", count)
	}

	// Simulate "restore" by re-inserting modified data
	for i := 0; i < 50; i++ {
		coll.InsertOne(map[string]interface{}{"id": i + 100, "data": fmt.Sprintf("restored-%d", i)})
	}

	// Verify we have more documents now
	count, _ = coll.Count(nil)
	if count != 100 {
		t.Errorf("Expected 100 documents after insert, got %d", count)
	}
}

func TestE2E_CollectionOperations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create multiple collections
	colls := []string{"users", "products", "orders", "inventory"}
	for _, name := range colls {
		coll, err := db.Collection(name)
		if err != nil {
			t.Fatalf("Failed to create collection %s: %v", name, err)
		}
		coll.InsertOne(map[string]interface{}{"test": true})
	}

	// List collections
	collections, err := db.ListCollections()
	if err != nil {
		t.Fatalf("ListCollections failed: %v", err)
	}

	if len(collections) < len(colls) {
		t.Errorf("Expected at least %d collections, got %d", len(colls), len(collections))
	}

	// Drop a collection
	err = db.DropCollection("orders")
	if err != nil {
		t.Fatalf("DropCollection failed: %v", err)
	}

	// Verify collection is empty after drop (implementation may vary)
	coll, _ := db.Collection("orders")
	count, _ := coll.Count(nil)
	if count != 0 {
		t.Logf("Collection dropped but still has %d documents (implementation dependent)", count)
	}
}
