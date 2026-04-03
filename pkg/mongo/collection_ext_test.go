package mongo

import (
	"bytes"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// Test encodeIDValue with all supported types
func TestEncodeIDValue_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		value    bson.Value
		expected int // expected length
	}{
		{"ObjectID", bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}), 12},
		{"String", bson.VString("test-id"), 7},
		{"StringEmpty", bson.VString(""), 0},
		{"Int32", bson.VInt32(12345), 4},
		{"Int32Zero", bson.VInt32(0), 4},
		{"Int32Negative", bson.VInt32(-1), 4},
		{"Int64", bson.VInt64(999999999999), 8},
		{"Int64Zero", bson.VInt64(0), 8},
		{"Int64Negative", bson.VInt64(-999999999999), 8},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := encodeIDValue(tc.value)
			if len(result) != tc.expected {
				t.Errorf("encodeIDValue(%v) length = %d, want %d", tc.value, len(result), tc.expected)
			}
		})
	}
}

// Test encodeIDValue with unsupported types (falls back to full BSON encoding)
func TestEncodeIDValue_UnsupportedTypes(t *testing.T) {
	tests := []struct {
		name  string
		value bson.Value
	}{
		{"Null", bson.VNull()},
		{"BoolTrue", bson.VBool(true)},
		{"BoolFalse", bson.VBool(false)},
		{"Double", bson.VDouble(3.14)},
		{"Array", bson.VArray(bson.A())},
		{"Document", bson.VDoc(bson.NewDocument())},
		{"DateTime", bson.VDateTime(12345)},
		{"Binary", bson.VBinary(bson.BinaryGeneric, []byte{1, 2, 3})},
		{"Regex", bson.VRegex("pattern", "i")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := encodeIDValue(tc.value)
			// For unsupported types, should return non-empty BSON-encoded data
			if len(result) == 0 {
				t.Errorf("encodeIDValue(%v) returned empty for unsupported type", tc.value)
			}
		})
	}
}

// Test encodeIDValue with ObjectID produces correct bytes
func TestEncodeIDValue_ObjectIDBytes(t *testing.T) {
	oid := bson.ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
	v := bson.VObjectID(oid)

	result := encodeIDValue(v)

	expected := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
	if !bytes.Equal(result, expected) {
		t.Errorf("encodeIDValue(ObjectID) = %v, want %v", result, expected)
	}
}

// Test encodeIDValue with Int32 produces big-endian bytes
func TestEncodeIDValue_Int32Bytes(t *testing.T) {
	v := bson.VInt32(0x12345678)

	result := encodeIDValue(v)

	// Should be big-endian: 0x12 0x34 0x56 0x78
	expected := []byte{0x12, 0x34, 0x56, 0x78}
	if !bytes.Equal(result, expected) {
		t.Errorf("encodeIDValue(Int32) = %v, want %v", result, expected)
	}
}

// Test encodeIDValue with Int64 produces big-endian bytes
func TestEncodeIDValue_Int64Bytes(t *testing.T) {
	v := bson.VInt64(0x123456789abcdef0)

	result := encodeIDValue(v)

	// Should be big-endian
	expected := []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	if !bytes.Equal(result, expected) {
		t.Errorf("encodeIDValue(Int64) = %v, want %v", result, expected)
	}
}

// Test encodeIDValue with String
func TestEncodeIDValue_StringBytes(t *testing.T) {
	v := bson.VString("hello")

	result := encodeIDValue(v)

	expected := []byte("hello")
	if !bytes.Equal(result, expected) {
		t.Errorf("encodeIDValue(String) = %v, want %v", result, expected)
	}
}

// Test ensureID generates new ID if missing
func TestEnsureID_GeneratesNew(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("Alice"))

	idBytes := ensureID(doc)

	if len(idBytes) != 12 {
		t.Errorf("ensureID generated id length = %d, want 12", len(idBytes))
	}

	// Document should now have _id
	if _, ok := doc.Get("_id"); !ok {
		t.Error("Document should have _id after ensureID")
	}
}

// Test ensureID preserves existing ID
func TestEnsureID_PreservesExisting(t *testing.T) {
	oid := bson.NewObjectID()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(oid))
	doc.Set("name", bson.VString("Alice"))

	idBytes := ensureID(doc)

	if !bytes.Equal(idBytes, oid[:]) {
		t.Error("ensureID should preserve existing ObjectID")
	}

	// Verify _id hasn't changed
	if v, ok := doc.Get("_id"); ok {
		if v.ObjectID() != oid {
			t.Error("_id was changed by ensureID")
		}
	}
}

// Test Collection InsertOne with different ID types
func TestCollection_InsertOne_DifferentIDTypes(t *testing.T) {
	tests := []struct {
		name string
		id   bson.Value
	}{
		{"ObjectID", bson.VObjectID(bson.NewObjectID())},
		{"String", bson.VString("custom-id-123")},
		{"Int32", bson.VInt32(12345)},
		{"Int64", bson.VInt64(9999999999)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer eng.Close()

			cat := NewCatalog(eng)
			cat.CreateDatabase("testdb")
			cat.CreateCollection("testdb", "testcoll")

			coll := NewCollection("testdb", "testcoll", eng, cat)

			doc := bson.NewDocument()
			doc.Set("_id", tc.id)
			doc.Set("data", bson.VString("test"))

			if err := coll.InsertOne(doc); err != nil {
				t.Fatalf("InsertOne: %v", err)
			}
		})
	}
}

// Test Collection FindOne success
func TestCollection_FindOne_Success(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "testcoll")

	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Insert a document
	oid := bson.NewObjectID()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(oid))
	doc.Set("name", bson.VString("Alice"))

	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Find the document
	found, err := coll.FindOne(oid)
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}

	if v, ok := found.Get("name"); !ok || v.String() != "Alice" {
		t.Error("Found document has wrong name")
	}
}

// Test Collection FindOne not found
func TestCollection_FindOne_NotFoundExt(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "testcoll")

	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Try to find non-existent document
	oid := bson.NewObjectID()
	_, err = coll.FindOne(oid)
	if err != ErrNotFound {
		t.Errorf("FindOne error = %v, want ErrNotFound", err)
	}
}

// Test Collection DeleteOne success
func TestCollection_DeleteOne_Success(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "testcoll")

	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Insert a document
	oid := bson.NewObjectID()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(oid))
	doc.Set("name", bson.VString("Alice"))

	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Delete the document
	if err := coll.DeleteOne(oid); err != nil {
		t.Fatalf("DeleteOne: %v", err)
	}

	// Should not be found anymore
	_, err = coll.FindOne(oid)
	if err != ErrNotFound {
		t.Error("Document should be deleted")
	}
}

// Test Collection ReplaceOne success
func TestCollection_ReplaceOne_Success(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "testcoll")

	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Insert a document
	oid := bson.NewObjectID()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(oid))
	doc.Set("name", bson.VString("Alice"))

	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Replace the document
	newDoc := bson.NewDocument()
	newDoc.Set("name", bson.VString("Bob"))

	if err := coll.ReplaceOne(oid, newDoc); err != nil {
		t.Fatalf("ReplaceOne: %v", err)
	}

	// Verify replacement
	found, err := coll.FindOne(oid)
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}

	if v, ok := found.Get("name"); !ok || v.String() != "Bob" {
		t.Errorf("Found document has wrong name: %v", v)
	}
	// _id should be preserved
	if v, ok := found.Get("_id"); !ok || v.ObjectID() != oid {
		t.Error("_id should be preserved after replace")
	}
}

// Test Collection Count
func TestCollection_Count_Ext(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "testcoll")

	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Count empty collection
	count, err := coll.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Errorf("Count = %d, want 0", count)
	}

	// Insert some documents
	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("idx", bson.VInt32(int32(i)))
		if err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne: %v", err)
		}
	}

	// Count again
	count, err = coll.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 5 {
		t.Errorf("Count = %d, want 5", count)
	}
}

// Test Collection ScanAll
func TestCollection_ScanAll_Ext(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "testcoll")

	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Insert documents
	for i := 0; i < 3; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("idx", bson.VInt32(int32(i)))
		if err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne: %v", err)
		}
	}

	// Scan all
	count := 0
	err = coll.ScanAll(func(key []byte, doc *bson.Document) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	if count != 3 {
		t.Errorf("ScanAll visited %d docs, want 3", count)
	}
}

// Test Collection ScanAll early stop
func TestCollection_ScanAll_EarlyStopExt(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "testcoll")

	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Insert documents
	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("idx", bson.VInt32(int32(i)))
		if err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne: %v", err)
		}
	}

	// Scan but stop early
	count := 0
	coll.ScanAll(func(key []byte, doc *bson.Document) bool {
		count++
		return count < 2
	})
	if count != 2 {
		t.Errorf("ScanAll early stop visited %d docs, want 2", count)
	}
}

// Test Collection InsertMany
func TestCollection_InsertMany_Ext(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "testcoll")

	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Insert many documents
	docs := []*bson.Document{
		bson.D("_id", bson.VObjectID(bson.NewObjectID()), "name", bson.VString("Alice")),
		bson.D("_id", bson.VObjectID(bson.NewObjectID()), "name", bson.VString("Bob")),
		bson.D("_id", bson.VObjectID(bson.NewObjectID()), "name", bson.VString("Charlie")),
	}

	if err := coll.InsertMany(docs); err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	// Verify count
	count, _ := coll.Count()
	if count != 3 {
		t.Errorf("Count = %d, want 3", count)
	}
}
