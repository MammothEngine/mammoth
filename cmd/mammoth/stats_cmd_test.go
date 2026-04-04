package main

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestGetCollStats(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert test documents
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		doc.Set("name", bson.VString("test"))
		key := mongo.EncodeDocumentKey("testdb", "testcoll", id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	cat := mongo.NewCatalog(eng)
	indexCat := mongo.NewIndexCatalog(eng, cat)

	stats := getCollStats(eng, indexCat, "testdb", "testcoll")

	if stats.DB != "testdb" {
		t.Errorf("expected DB=testdb, got %s", stats.DB)
	}
	if stats.Coll != "testcoll" {
		t.Errorf("expected Coll=testcoll, got %s", stats.Coll)
	}
	if stats.Docs != 10 {
		t.Errorf("expected Docs=10, got %d", stats.Docs)
	}
	if stats.DataSize <= 0 {
		t.Errorf("expected DataSize > 0, got %d", stats.DataSize)
	}
}

func TestGetCollStats_Empty(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	indexCat := mongo.NewIndexCatalog(eng, cat)

	stats := getCollStats(eng, indexCat, "testdb", "emptycoll")

	if stats.Docs != 0 {
		t.Errorf("expected Docs=0, got %d", stats.Docs)
	}
	if stats.DataSize != 0 {
		t.Errorf("expected DataSize=0, got %d", stats.DataSize)
	}
}

func TestFormatSize_GB(t *testing.T) {
	// Test GB formatting (not covered in cmd_utils_test.go)
	result := formatSize(2 * 1024 * 1024 * 1024)
	if result != "2.0 GB" {
		t.Errorf("formatSize(2GB) = %s, want 2.0 GB", result)
	}
}
