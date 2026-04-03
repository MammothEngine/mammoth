package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupHashIndexWithSpec(t *testing.T, spec *IndexSpec) (*engine.Engine, *HashIndex) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	return eng, NewHashIndex("testdb", "users", spec, eng)
}

// Test fnvHashValue with all supported types
func TestFnvHashValue_AllTypes(t *testing.T) {
	tests := []struct {
		name  string
		value bson.Value
	}{
		{"Null", bson.VNull()},
		{"BooleanTrue", bson.VBool(true)},
		{"BooleanFalse", bson.VBool(false)},
		{"Int32", bson.VInt32(42)},
		{"Int32Negative", bson.VInt32(-100)},
		{"Int64", bson.VInt64(9999999999)},
		{"Int64Negative", bson.VInt64(-9999999999)},
		{"Double", bson.VDouble(3.14159)},
		{"DoubleNegative", bson.VDouble(-2.718)},
		{"String", bson.VString("hello world")},
		{"StringEmpty", bson.VString("")},
		{"ObjectID", bson.VObjectID(bson.ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c})},
		{"Array", bson.VArray(bson.A(bson.VInt32(1), bson.VInt32(2)))},
		{"Document", bson.VDoc(bson.D("x", bson.VInt32(1)))},
		{"Binary", bson.VBinary(bson.BinaryGeneric, []byte{1, 2, 3})},
		{"DateTime", bson.VDateTime(1234567890)},
		{"Timestamp", bson.VTimestamp(12345)},
		{"Regex", bson.VRegex("pattern", "i")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hash := fnvHashValue(tc.value)
			if len(hash) != 8 {
				t.Errorf("fnvHashValue returned %d bytes, want 8", len(hash))
			}
		})
	}
}

// Test fnvHashValue produces consistent results
func TestFnvHashValue_Consistency(t *testing.T) {
	v1 := bson.VString("test")
	v2 := bson.VString("test")

	hash1 := fnvHashValue(v1)
	hash2 := fnvHashValue(v2)

	if string(hash1) != string(hash2) {
		t.Error("fnvHashValue should produce consistent hashes for same value")
	}
}

// Test fnvHashValue produces different results for different values
func TestFnvHashValue_DifferentValues(t *testing.T) {
	hash1 := fnvHashValue(bson.VString("value1"))
	hash2 := fnvHashValue(bson.VString("value2"))

	if string(hash1) == string(hash2) {
		t.Error("fnvHashValue should produce different hashes for different strings")
	}
}

// Test HashIndex with partial filter expression
func TestHashIndex_PartialFilter(t *testing.T) {
	filter := bson.D("active", bson.VBool(true))
	spec := &IndexSpec{
		Name:                    "active_email_hash",
		Key:                     []IndexKey{{Field: "email", Hashed: true}},
		PartialFilterExpression: filter,
	}
	eng, hi := setupHashIndexWithSpec(t, spec)
	_ = eng

	// Document that doesn't match filter
	docInactive := bson.NewDocument()
	docInactive.Set("_id", bson.VObjectID(bson.NewObjectID()))
	docInactive.Set("email", bson.VString("inactive@test.com"))
	docInactive.Set("active", bson.VBool(false))

	if err := hi.AddEntry(docInactive); err != nil {
		t.Fatalf("AddEntry (non-matching): %v", err)
	}

	// Should not be indexed
	ids := hi.LookupEqual(bson.VString("inactive@test.com"))
	if len(ids) != 0 {
		t.Errorf("partial filter: non-matching doc indexed, count = %d, want 0", len(ids))
	}

	// Document that matches filter
	docActive := bson.NewDocument()
	docActive.Set("_id", bson.VObjectID(bson.NewObjectID()))
	docActive.Set("email", bson.VString("active@test.com"))
	docActive.Set("active", bson.VBool(true))

	if err := hi.AddEntry(docActive); err != nil {
		t.Fatalf("AddEntry (matching): %v", err)
	}

	// Should be indexed
	ids = hi.LookupEqual(bson.VString("active@test.com"))
	if len(ids) != 1 {
		t.Errorf("partial filter: matching doc not indexed, count = %d, want 1", len(ids))
	}
}

// Test HashIndex AddEntry without _id
func TestHashIndex_AddEntry_NoID(t *testing.T) {
	_, hi := setupHashIndexTest(t)

	// Document without _id - should be skipped without error
	doc := bson.NewDocument()
	doc.Set("email", bson.VString("noid@test.com"))

	if err := hi.AddEntry(doc); err != nil {
		t.Errorf("AddEntry without _id should not error: %v", err)
	}
}

// Test HashIndex RemoveEntry without _id
func TestHashIndex_RemoveEntry_NoID(t *testing.T) {
	_, hi := setupHashIndexTest(t)

	// Document without _id - should be skipped without error
	doc := bson.NewDocument()
	doc.Set("email", bson.VString("noid@test.com"))

	if err := hi.RemoveEntry(doc); err != nil {
		t.Errorf("RemoveEntry without _id should not error: %v", err)
	}
}

// Test HashIndex RemoveEntry with sparse index
func TestHashIndex_RemoveEntry_Sparse(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name:   "sparse_hash",
		Key:    []IndexKey{{Field: "optional", Hashed: true}},
		Sparse: true,
	}
	hi := NewHashIndex("testdb", "coll", spec, eng)

	// Document without the indexed field
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("no-optional"))

	// Should not error for sparse index
	if err := hi.RemoveEntry(doc); err != nil {
		t.Errorf("RemoveEntry sparse miss should not error: %v", err)
	}
}

// Test HashIndex with compound hashed key
func TestHashIndex_CompoundKey(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "compound_hash",
		Key: []IndexKey{
			{Field: "email", Hashed: true},
			{Field: "name", Hashed: true},
		},
	}
	hi := NewHashIndex("testdb", "users", spec, eng)

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("email", bson.VString("test@example.com"))
	doc.Set("name", bson.VString("Test User"))

	if err := hi.AddEntry(doc); err != nil {
		t.Fatalf("AddEntry: %v", err)
	}

	// Both fields should be indexed
	ids1 := hi.LookupEqual(bson.VString("test@example.com"))
	if len(ids1) != 1 {
		t.Errorf("LookupEqual email = %d, want 1", len(ids1))
	}

	ids2 := hi.LookupEqual(bson.VString("Test User"))
	if len(ids2) != 1 {
		t.Errorf("LookupEqual name = %d, want 1", len(ids2))
	}
}

// Test HashIndex LookupEqual with missing values
func TestHashIndex_LookupEqual_Missing(t *testing.T) {
	_, hi := setupHashIndexTest(t)

	// Lookup without any entries
	ids := hi.LookupEqual(bson.VString("never-added@test.com"))
	if len(ids) != 0 {
		t.Errorf("LookupEqual for non-existent = %d, want 0", len(ids))
	}
}
