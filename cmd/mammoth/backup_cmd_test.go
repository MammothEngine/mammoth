package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestBackupCollection(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Create test data
	cat := mongo.NewCatalog(eng)
	if err := cat.EnsureDatabase("testdb"); err != nil {
		t.Fatalf("ensure db: %v", err)
	}
	if err := cat.EnsureCollection("testdb", "testcoll"); err != nil {
		t.Fatalf("ensure coll: %v", err)
	}

	// Insert documents
	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey("testdb", "testcoll", id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Create backup directory
	backupDir := t.TempDir()

	// Backup collection
	count, err := backupCollection(eng, "testdb", "testcoll", backupDir)
	if err != nil {
		t.Fatalf("backupCollection: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5 docs backed up, got %d", count)
	}

	// Verify backup file exists
	bakFile := filepath.Join(backupDir, "testdb.testcoll.bak")
	if _, err := os.Stat(bakFile); os.ErrNotExist == err {
		t.Error("backup file should exist")
	}

	// Verify count file exists
	countFile := filepath.Join(backupDir, "testdb.testcoll.count")
	if _, err := os.Stat(countFile); os.ErrNotExist == err {
		t.Error("count file should exist")
	}
}

func TestBackupCollection_Empty(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	backupDir := t.TempDir()

	// Backup empty collection
	count, err := backupCollection(eng, "testdb", "emptycoll", backupDir)
	if err != nil {
		t.Fatalf("backupCollection: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 docs backed up, got %d", count)
	}
}
