// Package e2e provides end-to-end integration tests for Mammoth Engine.
package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mammoth"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// setupTestDB creates a temporary database for testing.
func setupTestDB(t *testing.T) (*mammoth.Database, func()) {
	t.Helper()

	dir := t.TempDir()

	server, err := mammoth.NewServer(mammoth.ServerConfig{
		DataDir: dir,
		Port:    0, // Random port
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	client, err := mammoth.Connect(mammoth.ConnectOptions{
		Address: server.Addr(),
	})
	if err != nil {
		server.Close()
		t.Fatalf("Failed to connect: %v", err)
	}

	db := client.Database("testdb")

	cleanup := func() {
		client.Close()
		server.Close()
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

	ctx := context.Background()

	// Insert
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("Alice"))
	doc.Set("age", bson.VInt32(30))

	id, err := coll.InsertOne(ctx, doc)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Find
	filter := bson.NewDocument()
	filter.Set("_id", bson.VString(id))

	result, err := coll.FindOne(ctx, filter)
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if result["name"] != "Alice" {
		t.Errorf("Expected name=Alice, got %v", result["name"])
	}

	// Update
	update := bson.NewDocument()
	update.Set("$set", bson.VDoc(bson.NewDocument()))
	docUpdate, _ := update.Get("$set")
	docUpdateDoc := docUpdate.DocumentValue()
	docUpdateDoc.Set("age", bson.VInt32(31))

	updated, err := coll.UpdateOne(ctx, filter, update)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if updated != 1 {
		t.Errorf("Expected 1 document updated, got %d", updated)
	}

	// Delete
	deleted, err := coll.DeleteOne(ctx, filter)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if deleted != 1 {
		t.Errorf("Expected 1 document deleted, got %d", deleted)
	}

	// Verify deletion
	_, err = coll.FindOne(ctx, filter)
	if err != mongo.ErrNotFound {
		t.Error("Expected not found after deletion")
	}
}

func TestE2E_QueryOperators(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	coll, _ := db.Collection("products")
	ctx := context.Background()

	// Insert test data
	products := []map[string]interface{}{
		{"name": "Laptop", "price": 1000, "category": "electronics"},
		{"name": "Mouse", "price": 50, "category": "electronics"},
		{"name": "Desk", "price": 300, "category": "furniture"},
		{"name": "Chair", "price": 150, "category": "furniture"},
	}

	for _, p := range products {
		doc := bson.NewDocument()
		for k, v := range p {
			switch val := v.(type) {
			case string:
				doc.Set(k, bson.VString(val))
			case int:
				doc.Set(k, bson.VInt32(int32(val)))
			}
		}
		coll.InsertOne(ctx, doc)
	}

	// Test $gt operator
	filter := bson.NewDocument()
	priceFilter := bson.NewDocument()
	priceFilter.Set("$gt", bson.VInt32(100))
	filter.Set("price", bson.VDoc(priceFilter))

	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		t.Fatalf("Find with $gt failed: %v", err)
	}

	var count int
	for cursor.Next() {
		count++
	}

	if count != 2 { // Laptop and Desk
		t.Errorf("Expected 2 products with price > 100, got %d", count)
	}

	// Test $in operator
	filter2 := bson.NewDocument()
	categories := bson.NewDocument()
	categories.Set("$in", bson.VArray(bson.A(bson.VString("electronics"))))
	filter2.Set("category", bson.VDoc(categories))

	cursor2, _ := coll.Find(ctx, filter2)
	count = 0
	for cursor2.Next() {
		count++
	}

	if count != 2 { // Laptop and Mouse
		t.Errorf("Expected 2 electronics products, got %d", count)
	}
}

func TestE2E_Aggregation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	coll, _ := db.Collection("sales")
	ctx := context.Background()

	// Insert sales data
	sales := []map[string]interface{}{
		{"product": "A", "amount": 100, "quarter": "Q1"},
		{"product": "B", "amount": 200, "quarter": "Q1"},
		{"product": "A", "amount": 150, "quarter": "Q2"},
		{"product": "B", "amount": 300, "quarter": "Q2"},
	}

	for _, s := range sales {
		doc := bson.NewDocument()
		for k, v := range s {
			switch val := v.(type) {
			case string:
				doc.Set(k, bson.VString(val))
			case int:
				doc.Set(k, bson.VInt32(int32(val)))
			}
		}
		coll.InsertOne(ctx, doc)
	}

	// Test aggregation pipeline
	pipeline := mongo.Pipeline{
		{{"$group", bson.VDoc(bson.D(
			"_id", bson.VString("$product"),
			"totalSales", bson.VDoc(bson.D("$sum", bson.VString("$amount"))),
		))}},
		{{"$sort", bson.VDoc(bson.D("totalSales", bson.VInt32(-1)))}},
	}

	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregation failed: %v", err)
	}

	var results []map[string]interface{}
	for cursor.Next() {
		results = append(results, cursor.Current())
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(results))
	}

	// Product B should have higher total (200 + 300 = 500)
	// Product A should have lower total (100 + 150 = 250)
	if results[0]["_id"] != "B" {
		t.Error("Expected Product B first (higher total)")
	}
}

func TestE2E_Indexing(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	coll, _ := db.Collection("indexed")
	ctx := context.Background()

	// Create index
	indexModel := mongo.IndexModel{
		Keys: bson.NewDocument(),
	}
	indexModel.Keys.Set("email", bson.VInt32(1))

	err := coll.CreateIndex(ctx, indexModel)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Insert documents
	for i := 0; i < 100; i++ {
		doc := bson.NewDocument()
		doc.Set("email", bson.VString(fmt.Sprintf("user%d@test.com", i)))
		doc.Set("name", bson.VString(fmt.Sprintf("User %d", i)))
		coll.InsertOne(ctx, doc)
	}

	// Query should use index
	filter := bson.NewDocument()
	filter.Set("email", bson.VString("user50@test.com"))

	start := time.Now()
	_, err = coll.FindOne(ctx, filter)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	t.Logf("Indexed query took %v", elapsed)
	// With index, query should be fast (< 10ms for 100 docs)
	if elapsed > 10*time.Millisecond {
		t.Log("Warning: Query slower than expected (index may not be used)")
	}
}

func TestE2E_Transactions(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	accounts, _ := db.Collection("accounts")
	ctx := context.Background()

	// Create two accounts
	acc1 := bson.NewDocument()
	acc1.Set("_id", bson.VString("acc1"))
	acc1.Set("balance", bson.VInt32(1000))

	acc2 := bson.NewDocument()
	acc2.Set("_id", bson.VString("acc2"))
	acc2.Set("balance", bson.VInt32(500))

	accounts.InsertOne(ctx, acc1)
	accounts.InsertOne(ctx, acc2)

	// Perform transfer in a transaction
	session, err := db.Client().StartSession()
	if err != nil {
		t.Fatalf("Failed to start session: %v", err)
	}
	defer session.EndSession(ctx)

	err = session.WithTransaction(ctx, func(sessCtx context.Context) error {
		// Deduct from acc1
		filter1 := bson.NewDocument()
		filter1.Set("_id", bson.VString("acc1"))
		update1 := bson.NewDocument()
		update1.Set("$inc", bson.VDoc(bson.NewDocument()))
		inc1, _ := update1.Get("$inc")
		inc1Doc := inc1.DocumentValue()
		inc1Doc.Set("balance", bson.VInt32(-200))

		if _, err := accounts.UpdateOne(sessCtx, filter1, update1); err != nil {
			return err
		}

		// Add to acc2
		filter2 := bson.NewDocument()
		filter2.Set("_id", bson.VString("acc2"))
		update2 := bson.NewDocument()
		update2.Set("$inc", bson.VDoc(bson.NewDocument()))
		inc2, _ := update2.Get("$inc")
		inc2Doc := inc2.DocumentValue()
		inc2Doc.Set("balance", bson.VInt32(200))

		if _, err := accounts.UpdateOne(sessCtx, filter2, update2); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Verify balances
	result1, _ := accounts.FindOne(ctx, bson.D("_id", bson.VString("acc1")))
	if result1["balance"] != int32(800) {
		t.Errorf("Expected acc1 balance=800, got %v", result1["balance"])
	}

	result2, _ := accounts.FindOne(ctx, bson.D("_id", bson.VString("acc2")))
	if result2["balance"] != int32(700) {
		t.Errorf("Expected acc2 balance=700, got %v", result2["balance"])
	}
}

func TestE2E_ChangeStreams(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	coll, _ := db.Collection("watched")
	ctx := context.Background()

	// Start watching
	stream, err := coll.Watch(ctx, nil)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer stream.Close()

	// Insert a document (will trigger change event)
	doc := bson.NewDocument()
	doc.Set("test", bson.VString("value"))
	coll.InsertOne(ctx, doc)

	// Wait for change event
	select {
	case event := <-stream.Events():
		if event.OperationType != "insert" {
			t.Errorf("Expected insert operation, got %s", event.OperationType)
		}
	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for change event")
	}
}

func TestE2E_GridFS(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	bucket, err := db.GridFSBucket("files")
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	ctx := context.Background()
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
	ctx := context.Background()

	// Concurrent inserts
	const numGoroutines = 10
	const docsPerGoroutine = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < docsPerGoroutine; j++ {
				doc := bson.NewDocument()
				doc.Set("goroutine", bson.VInt32(int32(id)))
				doc.Set("seq", bson.VInt32(int32(j)))
				doc.Set("value", bson.VString(fmt.Sprintf("value-%d-%d", id, j)))

				_, err := coll.InsertOne(ctx, doc)
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
	count, err := coll.CountDocuments(ctx, bson.NewDocument())
	if err != nil {
		t.Fatalf("CountDocuments failed: %v", err)
	}

	expected := int64(numGoroutines * docsPerGoroutine)
	if count != expected {
		t.Errorf("Expected %d documents, got %d", expected, count)
	}
}

func TestE2E_BackupRestore(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	coll, _ := db.Collection("backup_test")
	ctx := context.Background()

	// Insert data
	for i := 0; i < 100; i++ {
		doc := bson.NewDocument()
		doc.Set("id", bson.VInt32(int32(i)))
		doc.Set("data", bson.VString(fmt.Sprintf("data-%d", i)))
		coll.InsertOne(ctx, doc)
	}

	// Create backup directory
	backupDir := t.TempDir()

	// Backup
	backup := mammoth.NewBackup(db.Client())
	metadata, err := backup.Create(ctx, "testdb", mammoth.BackupOptions{
		OutputDir: backupDir,
	})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	if metadata.DocCount != 100 {
		t.Errorf("Expected 100 documents in backup, got %d", metadata.DocCount)
	}

	// Clear collection
	coll.DeleteMany(ctx, bson.NewDocument())

	// Restore
	restore := mammoth.NewRestore(db.Client())
	err = restore.RestoreFromDir(ctx, filepath.Join(backupDir, "testdb", metadata.Timestamp), mammoth.RestoreOptions{})
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify restoration
	count, _ := coll.CountDocuments(ctx, bson.NewDocument())
	if count != 100 {
		t.Errorf("Expected 100 documents after restore, got %d", count)
	}
}

func TestE2E_TextSearch(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	coll, _ := db.Collection("articles")
	ctx := context.Background()

	// Create text index
	indexModel := mongo.IndexModel{
		Keys: bson.NewDocument(),
		Options: &mongo.IndexOptions{
			TextIndexVersion: 3,
		},
	}
	indexModel.Keys.Set("$text", bson.VString("$content"))
	coll.CreateIndex(ctx, indexModel)

	// Insert articles
	articles := []map[string]string{
		{"title": "Go Programming", "content": "Go is a great programming language for concurrent systems."},
		{"title": "MongoDB Basics", "content": "MongoDB is a document database with flexible schema."},
		{"title": "Concurrent Programming", "content": "Concurrency patterns in Go make parallel processing easy."},
	}

	for _, article := range articles {
		doc := bson.NewDocument()
		for k, v := range article {
			doc.Set(k, bson.VString(v))
		}
		coll.InsertOne(ctx, doc)
	}

	// Search
	filter := bson.NewDocument()
	filter.Set("$text", bson.VDoc(bson.NewDocument()))
	textFilter, _ := filter.Get("$text")
	textDoc := textFilter.DocumentValue()
	textDoc.Set("$search", bson.VString("Go concurrent"))

	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		t.Fatalf("Text search failed: %v", err)
	}

	var results []map[string]interface{}
	for cursor.Next() {
		results = append(results, cursor.Current())
	}

	// Should find articles about Go and concurrency
	if len(results) == 0 {
		t.Error("Expected to find matching articles")
	}
}
