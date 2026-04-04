package main

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// Test formatSize function
func TestFormatSize(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1024 * 1024, "1.0 MB"},
		{1536 * 1024, "1.5 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		{1536 * 1024 * 1024, "1.5 GB"},
	}

	for _, tt := range tests {
		result := formatSize(tt.bytes)
		if result != tt.expected {
			t.Errorf("formatSize(%d) = %s, want %s", tt.bytes, result, tt.expected)
		}
	}
}

// Test isValidBSON function
func TestIsValidBSON(t *testing.T) {
	// Valid BSON document
	validDoc := bson.D("name", bson.VString("test"), "value", bson.VInt32(42))
	validBytes := bson.Encode(validDoc)

	// Test valid document
	if !isValidBSON(validBytes) {
		t.Error("isValidBSON should return true for valid document")
	}

	// Test invalid - too short
	if isValidBSON([]byte{0x00, 0x00}) {
		t.Error("isValidBSON should return false for short buffer")
	}

	// Test invalid - mismatched size
	invalidSize := make([]byte, 10)
	invalidSize[0] = 0xFF // Declare large size
	invalidSize[1] = 0xFF
	invalidSize[2] = 0xFF
	invalidSize[3] = 0x7F
	if isValidBSON(invalidSize) {
		t.Error("isValidBSON should return false for mismatched size")
	}

	// Test minimal valid document
	// BSON: size(4) + type(1) + "a\0"(2) + int32(4) + terminator(1) = 12 bytes
	minimal := []byte{
		0x0C, 0x00, 0x00, 0x00, // size = 12
		0x10,                   // type = int32
		'a', 0x00,              // field name = "a"
		0x01, 0x00, 0x00, 0x00, // value = 1
		0x00, // terminator
	}
	if !isValidBSON(minimal) {
		t.Error("isValidBSON should return true for minimal valid document")
	}
}

// Test isValidBSON with malformed documents
func TestIsValidBSON_Malformed(t *testing.T) {
	// Truncated document - declares size but doesn't have enough bytes
	truncated := []byte{
		0xFF, 0x00, 0x00, 0x00, // size = 255
		0x10,                   // type = int32
	}
	if isValidBSON(truncated) {
		t.Error("isValidBSON should return false for truncated document")
	}

	// Document with invalid type
	invalidType := []byte{
		0x08, 0x00, 0x00, 0x00, // size = 8
		0xFF,                   // invalid type
		'a', 0x00,              // field name
		0x00,                   // terminator
	}
	if isValidBSON(invalidType) {
		t.Error("isValidBSON should return false for invalid type")
	}
}

// Test extractDocKey function
func TestExtractDocKey(t *testing.T) {
	// Create a document with ObjectID _id
	oid := bson.NewObjectID()
	doc := bson.D("_id", bson.VObjectID(oid), "name", bson.VString("alice"))
	docBytes := bson.Encode(doc)

	key, err := extractDocKey("testdb", "users", docBytes)
	if err != nil {
		t.Fatalf("extractDocKey failed: %v", err)
	}

	// Verify key contains expected components
	expectedKey := mongo.EncodeDocumentKey("testdb", "users", oid[:])
	if string(key) != string(expectedKey) {
		t.Error("extractDocKey returned unexpected key")
	}
}

// Test extractDocKey with string _id
func TestExtractDocKey_StringID(t *testing.T) {
	doc := bson.D("_id", bson.VString("user123"), "name", bson.VString("alice"))
	docBytes := bson.Encode(doc)

	key, err := extractDocKey("testdb", "users", docBytes)
	if err != nil {
		t.Fatalf("extractDocKey failed: %v", err)
	}

	if len(key) == 0 {
		t.Error("extractDocKey returned empty key")
	}
}

// Test extractDocKey with int32 _id
func TestExtractDocKey_Int32ID(t *testing.T) {
	doc := bson.D("_id", bson.VInt32(42), "name", bson.VString("alice"))
	docBytes := bson.Encode(doc)

	key, err := extractDocKey("testdb", "users", docBytes)
	if err != nil {
		t.Fatalf("extractDocKey failed: %v", err)
	}

	if len(key) == 0 {
		t.Error("extractDocKey returned empty key")
	}
}

// Test extractDocKey with int64 _id
func TestExtractDocKey_Int64ID(t *testing.T) {
	doc := bson.D("_id", bson.VInt64(9999999999), "name", bson.VString("alice"))
	docBytes := bson.Encode(doc)

	key, err := extractDocKey("testdb", "users", docBytes)
	if err != nil {
		t.Fatalf("extractDocKey failed: %v", err)
	}

	if len(key) == 0 {
		t.Error("extractDocKey returned empty key")
	}
}

// Test extractDocKey with missing _id
func TestExtractDocKey_MissingID(t *testing.T) {
	doc := bson.D("name", bson.VString("alice"))
	docBytes := bson.Encode(doc)

	key, err := extractDocKey("testdb", "users", docBytes)
	if err != nil {
		t.Fatalf("extractDocKey failed: %v", err)
	}

	// Should generate synthetic key
	if len(key) == 0 {
		t.Error("extractDocKey should generate synthetic key when _id is missing")
	}
}

// Test extractDocKey with too short document
func TestExtractDocKey_TooShort(t *testing.T) {
	_, err := extractDocKey("testdb", "users", []byte{0x00, 0x00})
	if err == nil {
		t.Error("extractDocKey should error for too short document")
	}
}

