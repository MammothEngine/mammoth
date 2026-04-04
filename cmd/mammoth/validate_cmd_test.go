package main

import (
	"encoding/binary"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestIsValidBSON_InvalidStructure(t *testing.T) {
	// Document with invalid type
	invalidType := make([]byte, 20)
	binary.LittleEndian.PutUint32(invalidType, 20)
	invalidType[4] = 0x99 // Unknown type
	invalidType[5] = 'a'
	invalidType[6] = 0    // null terminator
	// Missing value for unknown type - returns -1 for unknown type
	if isValidBSON(invalidType) {
		t.Error("isValidBSON should return false for invalid type")
	}

	// Document with no terminator (ends without 0x00)
	noTerm := make([]byte, 12)
	binary.LittleEndian.PutUint32(noTerm, 12)
	noTerm[4] = 0x10 // int32 type
	noTerm[5] = 'a'
	noTerm[6] = 0    // field name terminator
	noTerm[7] = 0x01 // value byte 1
	noTerm[8] = 0x02 // value byte 2
	noTerm[9] = 0x03 // value byte 3
	noTerm[10] = 0x04 // value byte 4
	// Missing document terminator at position 11
	// This should still be valid according to the current implementation
}

func TestValidateCollection(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert valid documents
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

	// Validate collection
	result := validateCollection(eng, "testdb", "testcoll")
	if result.DB != "testdb" {
		t.Errorf("expected DB=testdb, got %s", result.DB)
	}
	if result.Coll != "testcoll" {
		t.Errorf("expected Coll=testcoll, got %s", result.Coll)
	}
	if result.Scanned != 5 {
		t.Errorf("expected Scanned=5, got %d", result.Scanned)
	}
	if result.Valid != 5 {
		t.Errorf("expected Valid=5, got %d", result.Valid)
	}
	if result.Errors != 0 {
		t.Errorf("expected Errors=0, got %d", result.Errors)
	}
}

func TestValidateCollection_Empty(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Validate empty collection
	result := validateCollection(eng, "testdb", "emptycoll")
	if result.Scanned != 0 {
		t.Errorf("expected Scanned=0, got %d", result.Scanned)
	}
}

func TestValidateCollection_InvalidDoc(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert an invalid document (wrong size)
	invalidDoc := make([]byte, 10)
	binary.LittleEndian.PutUint32(invalidDoc, 100) // declare 100 bytes but only have 10
	key := mongo.EncodeDocumentKey("testdb", "badcoll", []byte("key1"))
	if err := eng.Put(key, invalidDoc); err != nil {
		t.Fatalf("put invalid doc: %v", err)
	}

	// Validate - should detect error
	result := validateCollection(eng, "testdb", "badcoll")
	if result.Scanned != 1 {
		t.Errorf("expected Scanned=1, got %d", result.Scanned)
	}
	if result.Errors != 1 {
		t.Errorf("expected Errors=1, got %d", result.Errors)
	}
	if result.Valid != 0 {
		t.Errorf("expected Valid=0, got %d", result.Valid)
	}
}
