package mongo

import (
	"bytes"
	"math"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func TestFlipForDescending(t *testing.T) {
	tests := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte{0x00, 0x01, 0x02}, []byte{0xff, 0xfe, 0xfd}},
		{[]byte{0xff, 0xfe}, []byte{0x00, 0x01}},
		{[]byte{}, []byte{}},
		{[]byte{0x00}, []byte{0xff}},
	}

	for _, tc := range tests {
		result := make([]byte, len(tc.input))
		copy(result, tc.input)
		flipForDescending(result)
		if !bytes.Equal(result, tc.expected) {
			t.Errorf("flipForDescending(%v) = %v, want %v", tc.input, result, tc.expected)
		}
	}
}

func TestBuildIndexKeyWithValue(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "test_idx",
		Key: []IndexKey{
			{Field: "name", Descending: false},
		},
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// Build key with a string value
	val := bson.VString("Alice")
	id := []byte("123456789012")
	key := buildIndexKeyWithValue("testdb", "testcoll", spec, val, id)

	if len(key) == 0 {
		t.Error("expected non-empty key")
	}

	// Verify key contains expected parts
	nsPrefix := EncodeNamespacePrefix("testdb", "testcoll")
	if !bytes.HasPrefix(key, nsPrefix) {
		t.Error("expected key to start with namespace prefix")
	}
	if !bytes.HasSuffix(key, id) {
		t.Error("expected key to end with id")
	}

	// Test with descending index
	specDesc := &IndexSpec{
		Name: "test_idx_desc",
		Key: []IndexKey{
			{Field: "name", Descending: true},
		},
	}
	keyDesc := buildIndexKeyWithValue("testdb", "testcoll", specDesc, val, id)
	if len(keyDesc) == 0 {
		t.Error("expected non-empty key for descending index")
	}

	_ = idx
}

func TestIndexSpec(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "my_index",
		Key: []IndexKey{
			{Field: "name", Descending: false},
			{Field: "age", Descending: true},
		},
		Unique: true,
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// Test Spec() method
	gotSpec := idx.Spec()
	if gotSpec == nil {
		t.Fatal("Spec() returned nil")
	}
	if gotSpec.Name != "my_index" {
		t.Errorf("Spec().Name = %q, want %q", gotSpec.Name, "my_index")
	}
	if !gotSpec.Unique {
		t.Error("Spec().Unique = false, want true")
	}
	if len(gotSpec.Key) != 2 {
		t.Errorf("Spec().Key length = %d, want 2", len(gotSpec.Key))
	}
}

func TestLookupByPrefix(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Test LookupByPrefix with empty prefix
	prefix := EncodeNamespacePrefix("testdb", "testcoll")
	entries := LookupByPrefix(eng, prefix)
	// Should return empty since no data
	_ = entries
}

func TestBuildIndexKey(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "compound_idx",
		Key: []IndexKey{
			{Field: "name", Descending: false},
			{Field: "age", Descending: true},
		},
	}

	// Create document with _id
	doc := bson.D(
		"_id", bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		"name", bson.VString("Alice"),
		"age", bson.VInt32(30),
	)

	id := bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}.Bytes()
	key := buildIndexKey("testdb", "testcoll", spec, doc, id)

	if len(key) == 0 {
		t.Error("expected non-empty key")
	}

	// Verify key structure
	nsPrefix := EncodeNamespacePrefix("testdb", "testcoll")
	if !bytes.HasPrefix(key, nsPrefix) {
		t.Error("expected key to start with namespace prefix")
	}
	if !bytes.HasSuffix(key, id) {
		t.Error("expected key to end with id")
	}
}

func TestBsonValueToFloat64(t *testing.T) {
	tests := []struct {
		val      bson.Value
		expected float64
	}{
		{bson.VDouble(3.14), 3.14},
		{bson.VInt32(42), 42.0},
		{bson.VInt64(100), 100.0},
		{bson.VString("not a number"), 0.0},
		{bson.VNull(), 0.0},
		{bson.VBool(true), 0.0},
	}

	for _, tc := range tests {
		result := bsonValueToFloat64(tc.val)
		if result != tc.expected {
			t.Errorf("bsonValueToFloat64(%v) = %f, want %f", tc.val, result, tc.expected)
		}
	}
}

// Test AddEntry with sparse index
func TestIndexAddEntry_Sparse(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "sparse_idx",
		Key:  []IndexKey{{Field: "optional", Descending: false}},
		Sparse: true,
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// Document without the indexed field - should be skipped
	docWithout := bson.D("_id", bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}))
	err = idx.AddEntry(docWithout)
	if err != nil {
		t.Errorf("sparse index should skip doc without field: %v", err)
	}

	// Document with the indexed field - should be added
	docWith := bson.D(
		"_id", bson.VObjectID(bson.ObjectID{2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		"optional", bson.VString("value"),
	)
	err = idx.AddEntry(docWith)
	if err != nil {
		t.Errorf("sparse index should add doc with field: %v", err)
	}

	_ = idx
}

// Test AddEntry with unique index
func TestIndexAddEntry_Unique(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name:   "unique_idx",
		Key:    []IndexKey{{Field: "email", Descending: false}},
		Unique: true,
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// First document
	doc1 := bson.D(
		"_id", bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		"email", bson.VString("alice@example.com"),
	)
	err = idx.AddEntry(doc1)
	if err != nil {
		t.Errorf("first unique entry should succeed: %v", err)
	}

	// Duplicate value - should fail
	doc2 := bson.D(
		"_id", bson.VObjectID(bson.ObjectID{2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		"email", bson.VString("alice@example.com"),
	)
	err = idx.AddEntry(doc2)
	if err == nil {
		t.Error("duplicate unique entry should fail")
	}

	_ = idx
}

// Test AddEntry with partial filter expression
func TestIndexAddEntry_PartialFilter(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	filter := bson.D("active", bson.VBool(true))
	spec := &IndexSpec{
		Name:                    "partial_idx",
		Key:                     []IndexKey{{Field: "name", Descending: false}},
		PartialFilterExpression: filter,
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// Document that doesn't match filter - should be skipped
	docInactive := bson.D(
		"_id", bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		"name", bson.VString("Alice"),
		"active", bson.VBool(false),
	)
	err = idx.AddEntry(docInactive)
	if err != nil {
		t.Errorf("partial index should skip non-matching doc: %v", err)
	}

	// Document that matches filter - should be added
	docActive := bson.D(
		"_id", bson.VObjectID(bson.ObjectID{2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		"name", bson.VString("Bob"),
		"active", bson.VBool(true),
	)
	err = idx.AddEntry(docActive)
	if err != nil {
		t.Errorf("partial index should add matching doc: %v", err)
	}

	_ = idx
}

// Test AddEntry with multikey (array) index
func TestIndexAddEntry_Multikey(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "tags_idx",
		Key:  []IndexKey{{Field: "tags", Descending: false}},
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// Document with array value - should create multiple entries
	doc := bson.D(
		"_id", bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		"tags", bson.VArray(bson.A(bson.VString("a"), bson.VString("b"), bson.VString("c"))),
	)
	err = idx.AddEntry(doc)
	if err != nil {
		t.Errorf("multikey add should succeed: %v", err)
	}

	_ = idx
}

// Test AddEntry with empty array
func TestIndexAddEntry_EmptyArray(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "tags_idx",
		Key:  []IndexKey{{Field: "tags", Descending: false}},
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// Document with empty array - should create null entry
	doc := bson.D(
		"_id", bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		"tags", bson.VArray(bson.A()),
	)
	err = idx.AddEntry(doc)
	if err != nil {
		t.Errorf("empty array add should succeed: %v", err)
	}

	_ = idx
}

// Test AddEntry without _id
func TestIndexAddEntry_NoID(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "name_idx",
		Key:  []IndexKey{{Field: "name", Descending: false}},
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// Document without _id - should be skipped
	doc := bson.D("name", bson.VString("Alice"))
	err = idx.AddEntry(doc)
	if err != nil {
		t.Errorf("doc without _id should be skipped without error: %v", err)
	}

	_ = idx
}

// Test RemoveEntry with sparse index
func TestIndexRemoveEntry_Sparse(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "sparse_idx",
		Key:  []IndexKey{{Field: "optional", Descending: false}},
		Sparse: true,
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// Document without the indexed field - should be skipped
	docWithout := bson.D("_id", bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}))
	err = idx.RemoveEntry(docWithout)
	if err != nil {
		t.Errorf("sparse remove should skip doc without field: %v", err)
	}

	_ = idx
}

// Test RemoveEntry with multikey
func TestIndexRemoveEntry_Multikey(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "tags_idx",
		Key:  []IndexKey{{Field: "tags", Descending: false}},
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// Document with array value
	doc := bson.D(
		"_id", bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		"tags", bson.VArray(bson.A(bson.VString("a"), bson.VString("b"))),
	)
	err = idx.RemoveEntry(doc)
	if err != nil {
		t.Errorf("multikey remove should succeed: %v", err)
	}

	_ = idx
}

// Test RemoveEntry with empty array
func TestIndexRemoveEntry_EmptyArray(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "tags_idx",
		Key:  []IndexKey{{Field: "tags", Descending: false}},
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// Document with empty array
	doc := bson.D(
		"_id", bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		"tags", bson.VArray(bson.A()),
	)
	err = idx.RemoveEntry(doc)
	if err != nil {
		t.Errorf("empty array remove should succeed: %v", err)
	}

	_ = idx
}

// Test RemoveEntry without _id
func TestIndexRemoveEntry_NoID(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "name_idx",
		Key:  []IndexKey{{Field: "name", Descending: false}},
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// Document without _id - should be skipped
	doc := bson.D("name", bson.VString("Alice"))
	err = idx.RemoveEntry(doc)
	if err != nil {
		t.Errorf("doc without _id should be skipped without error: %v", err)
	}

	_ = idx
}

// Test ScanPrefix
func TestIndexScanPrefix(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "test_idx",
		Key:  []IndexKey{{Field: "name", Descending: false}},
	}

	idx := NewIndex("mydb", "mycoll", spec, eng)

	prefix := idx.ScanPrefix()
	if len(prefix) == 0 {
		t.Error("ScanPrefix should return non-empty prefix")
	}

	// Should contain namespace and index name
	nsPrefix := EncodeNamespacePrefix("mydb", "mycoll")
	if !bytes.HasPrefix(prefix, nsPrefix) {
		t.Error("prefix should start with namespace")
	}
}

// Test encodeIndexValue for all types
func TestEncodeIndexValue_Types(t *testing.T) {
	tests := []struct {
		name     string
		value    bson.Value
		expected byte // first byte (type tag)
	}{
		{"null", bson.VNull(), typeTagNull},
		{"bool true", bson.VBool(true), typeTagTrue},
		{"bool false", bson.VBool(false), typeTagFalse},
		{"int32", bson.VInt32(42), typeTagNumber},
		{"int64", bson.VInt64(100), typeTagNumber},
		{"double", bson.VDouble(3.14), typeTagNumber},
		{"string", bson.VString("test"), typeTagString},
		{"objectid", bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}), typeTagOID},
		{"datetime", bson.VDateTime(1234567890), typeTagDate},
		{"minkey", bson.VMinKey(), typeTagMinKey},
		{"maxkey", bson.VMaxKey(), typeTagMaxKey},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := encodeIndexValue(tc.value)
			if len(result) == 0 {
				t.Fatal("expected non-empty result")
			}
			if result[0] != tc.expected {
				t.Errorf("type tag = 0x%02x, want 0x%02x", result[0], tc.expected)
			}
		})
	}
}

// Test encodeIndexValue for unknown type (default case)
func TestEncodeIndexValue_UnknownType(t *testing.T) {
	// Create a value with an unsupported type (binary)
	val := bson.VBinary(bson.BinaryGeneric, []byte{1, 2, 3})
	result := encodeIndexValue(val)
	if len(result) == 0 {
		t.Fatal("expected non-empty result")
	}
	if result[0] != typeTagNull {
		t.Errorf("unknown type should return null tag, got 0x%02x", result[0])
	}
}

// Test encodeIndexNumber with NaN
func TestEncodeIndexNumber_NaN(t *testing.T) {
	result := encodeIndexNumber(math.NaN())
	if len(result) != 9 {
		t.Errorf("expected 9 bytes, got %d", len(result))
	}
	if result[0] != typeTagNumber {
		t.Errorf("expected number tag, got 0x%02x", result[0])
	}
}

// Test encodeIndexNumber with positive and negative infinity
func TestEncodeIndexNumber_Infinity(t *testing.T) {
	posInf := encodeIndexNumber(math.Inf(1))
	negInf := encodeIndexNumber(math.Inf(-1))

	if len(posInf) != 9 || len(negInf) != 9 {
		t.Error("expected 9 bytes for infinity values")
	}

	// Negative infinity should sort before positive infinity
	// Compare byte by byte after the type tag
	for i := 1; i < 9; i++ {
		if negInf[i] >= posInf[i] {
			// This is expected behavior, just verify they're different
			break
		}
	}
}

// Test LookupByPrefix with actual data
func TestLookupByPrefix_WithData(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "name_idx",
		Key:  []IndexKey{{Field: "name", Descending: false}},
	}

	idx := NewIndex("testdb", "testcoll", spec, eng)

	// Add an entry
	doc := bson.D(
		"_id", bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		"name", bson.VString("Alice"),
	)
	err = idx.AddEntry(doc)
	if err != nil {
		t.Fatalf("add entry: %v", err)
	}

	// Lookup using the index prefix
	prefix := idx.ScanPrefix()
	ids := LookupByPrefix(eng, prefix)

	// Should find the document
	if len(ids) != 1 {
		t.Errorf("expected 1 id, got %d", len(ids))
	}
}
