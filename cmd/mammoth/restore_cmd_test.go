package main

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestRestoreCollection(t *testing.T) {
	// Create source engine with data
	srcDir := t.TempDir()
	srcOpts := engine.DefaultOptions(srcDir)
	srcEng, err := engine.Open(srcOpts)
	if err != nil {
		t.Fatalf("open source engine: %v", err)
	}

	// Insert test documents
	ids := make([]bson.ObjectID, 3)
	for i := 0; i < 3; i++ {
		ids[i] = bson.NewObjectID()
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(ids[i]))
		doc.Set("value", bson.VInt32(int32(i*10)))
		key := mongo.EncodeDocumentKey("testdb", "testcoll", ids[i][:])
		if err := srcEng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	srcEng.Close()

	// Backup the collection
	backupDir := t.TempDir()
	srcEng, _ = engine.Open(srcOpts)
	_, err = backupCollection(srcEng, "testdb", "testcoll", backupDir)
	if err != nil {
		t.Fatalf("backupCollection: %v", err)
	}
	srcEng.Close()

	// Restore to new engine
	dstDir := t.TempDir()
	dstOpts := engine.DefaultOptions(dstDir)
	dstEng, err := engine.Open(dstOpts)
	if err != nil {
		t.Fatalf("open dest engine: %v", err)
	}
	defer dstEng.Close()

	cat := mongo.NewCatalog(dstEng)

	bakFile := filepath.Join(backupDir, "testdb.testcoll.bak")
	count, err := restoreCollection(dstEng, cat, "testdb", "testcoll", bakFile)
	if err != nil {
		t.Fatalf("restoreCollection: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 docs restored, got %d", count)
	}

	// Verify documents exist
	for i, id := range ids {
		key := mongo.EncodeDocumentKey("testdb", "testcoll", id[:])
		val, err := dstEng.Get(key)
		if err != nil {
			t.Errorf("failed to get restored doc %d: %v", i, err)
			continue
		}
		doc, err := bson.Decode(val)
		if err != nil {
			t.Errorf("failed to decode doc %d: %v", i, err)
			continue
		}
		v, _ := doc.Get("value")
		if v.Int32() != int32(i*10) {
			t.Errorf("doc %d: expected value=%d, got %d", i, i*10, v.Int32())
		}
	}
}

func TestRestoreCollection_EmptyFile(t *testing.T) {
	dstDir := t.TempDir()
	opts := engine.DefaultOptions(dstDir)
	dstEng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer dstEng.Close()

	// Create empty backup file
	backupDir := t.TempDir()
	emptyFile := filepath.Join(backupDir, "empty.bak")
	if err := os.WriteFile(emptyFile, []byte{}, 0644); err != nil {
		t.Fatalf("write empty file: %v", err)
	}

	cat := mongo.NewCatalog(dstEng)
	count, err := restoreCollection(dstEng, cat, "testdb", "testcoll", emptyFile)
	if err != nil {
		t.Fatalf("restoreCollection empty file: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 docs, got %d", count)
	}
}

func TestRestoreCollection_InvalidDocSize(t *testing.T) {
	dstDir := t.TempDir()
	opts := engine.DefaultOptions(dstDir)
	dstEng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer dstEng.Close()

	// Create backup file with invalid document size (>16MB)
	backupDir := t.TempDir()
	badFile := filepath.Join(backupDir, "bad.bak")
	f, _ := os.Create(badFile)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], 20*1024*1024) // 20MB - too large
	f.Write(lenBuf[:])
	f.Close()

	cat := mongo.NewCatalog(dstEng)
	_, err = restoreCollection(dstEng, cat, "testdb", "testcoll", badFile)
	if err == nil {
		t.Error("expected error for invalid doc size")
	}
}

func TestExtractDocKey_DoubleID(t *testing.T) {
	// Test with double _id (type 0x01)
	doc := bson.NewDocument()
	doc.Set("_id", bson.VDouble(3.14))
	doc.Set("name", bson.VString("test"))
	docBytes := bson.Encode(doc)

	key, err := extractDocKey("testdb", "users", docBytes)
	if err != nil {
		t.Fatalf("extractDocKey: %v", err)
	}

	if len(key) == 0 {
		t.Error("extractDocKey should return key for double _id")
	}
}

func TestExtractDocKey_BoolID(t *testing.T) {
	// Test with bool _id (type 0x08)
	doc := bson.NewDocument()
	doc.Set("_id", bson.VBool(true))
	doc.Set("name", bson.VString("test"))
	docBytes := bson.Encode(doc)

	key, err := extractDocKey("testdb", "users", docBytes)
	if err != nil {
		t.Fatalf("extractDocKey: %v", err)
	}

	if len(key) == 0 {
		t.Error("extractDocKey should return key for bool _id")
	}
}

func TestExtractDocKey_NullID(t *testing.T) {
	// Test with null _id (type 0x0A)
	doc := bson.NewDocument()
	doc.Set("_id", bson.VNull())
	doc.Set("name", bson.VString("test"))
	docBytes := bson.Encode(doc)

	key, err := extractDocKey("testdb", "users", docBytes)
	if err != nil {
		t.Fatalf("extractDocKey: %v", err)
	}

	if len(key) == 0 {
		t.Error("extractDocKey should return key for null _id")
	}
}
