package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Test isZeroOrFalse with all value types
func TestIsZeroOrFalse_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		value    bson.Value
		expected bool
	}{
		// Int32 tests
		{"Int32Zero", bson.VInt32(0), true},
		{"Int32NonZero", bson.VInt32(42), false},
		{"Int32Negative", bson.VInt32(-1), false},

		// Int64 tests
		{"Int64Zero", bson.VInt64(0), true},
		{"Int64NonZero", bson.VInt64(123456789), false},
		{"Int64Negative", bson.VInt64(-1), false},

		// Double tests
		{"DoubleZero", bson.VDouble(0.0), true},
		{"DoubleNonZero", bson.VDouble(3.14), false},
		{"DoubleNegative", bson.VDouble(-0.1), false},

		// Boolean tests
		{"BoolFalse", bson.VBool(false), true},
		{"BoolTrue", bson.VBool(true), false},

		// Other types should return false
		{"StringEmpty", bson.VString(""), false},
		{"StringValue", bson.VString("hello"), false},
		{"Null", bson.VNull(), false},
		{"ObjectID", bson.VObjectID(bson.NewObjectID()), false},
		{"Array", bson.VArray(bson.A()), false},
		{"Document", bson.VDoc(bson.NewDocument()), false},
		{"DateTime", bson.VDateTime(0), false},
		{"Binary", bson.VBinary(bson.BinaryGeneric, []byte{}), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isZeroOrFalse(tc.value)
			if result != tc.expected {
				t.Errorf("isZeroOrFalse(%v) = %v, want %v", tc.value, result, tc.expected)
			}
		})
	}
}

// Test ApplyProjection with _id exclusion
func TestApplyProjection_ExcludeID(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("Alice"))
	doc.Set("age", bson.VInt32(30))

	// Projection to exclude _id
	proj := bson.NewDocument()
	proj.Set("_id", bson.VInt32(0))
	proj.Set("name", bson.VInt32(1))

	result := ApplyProjection(doc, proj)

	// Result should have name but no _id
	if _, ok := result.Get("_id"); ok {
		t.Error("_id should be excluded")
	}
	if _, ok := result.Get("name"); !ok {
		t.Error("name should be included")
	}
}

// Test ApplyProjection with boolean values
func TestApplyProjection_BooleanValues(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("Alice"))
	doc.Set("secret", bson.VString("hidden"))

	// Include mode with false for _id
	proj := bson.NewDocument()
	proj.Set("_id", bson.VBool(false))
	proj.Set("name", bson.VBool(true))

	result := ApplyProjection(doc, proj)

	if _, ok := result.Get("_id"); ok {
		t.Error("_id should be excluded with false")
	}
	if _, ok := result.Get("secret"); ok {
		t.Error("secret should be excluded in include mode")
	}
}

// Test projectionMode edge cases
func TestProjectionMode_Empty(t *testing.T) {
	// Empty projection defaults to include mode
	proj := bson.NewDocument()

	doc := bson.NewDocument()
	doc.Set("name", bson.VString("Alice"))

	result := ApplyProjection(doc, proj)
	if result.Len() != 1 {
		t.Errorf("empty projection should include all, got %d fields", result.Len())
	}
}

// Test ApplyProjection exclude mode with all false values
func TestApplyProjection_ExcludeAll(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("password", bson.VString("secret"))
	doc.Set("name", bson.VString("Alice"))

	// Exclude mode - exclude password only
	proj := bson.NewDocument()
	proj.Set("password", bson.VInt32(0))

	result := ApplyProjection(doc, proj)

	if _, ok := result.Get("password"); ok {
		t.Error("password should be excluded")
	}
	if _, ok := result.Get("name"); !ok {
		t.Error("name should be included")
	}
	if _, ok := result.Get("_id"); !ok {
		t.Error("_id should be included in exclude mode")
	}
}

// Test ApplyProjection include mode excludes unspecified fields
func TestApplyProjection_IncludeMode_ExcludesOthers(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("Alice"))
	doc.Set("secret", bson.VString("hidden"))

	// Include only name
	proj := bson.NewDocument()
	proj.Set("name", bson.VInt32(1))

	result := ApplyProjection(doc, proj)

	if _, ok := result.Get("name"); !ok {
		t.Error("name should be included")
	}
	if _, ok := result.Get("secret"); ok {
		t.Error("secret should be excluded in include mode")
	}
	// _id should be included by default
	if _, ok := result.Get("_id"); !ok {
		t.Error("_id should be included by default")
	}
}

// Test ApplyProjection with nil projection
func TestApplyProjection_NilProjection(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("Alice"))

	result := ApplyProjection(doc, nil)

	if result.Len() != 1 {
		t.Errorf("nil projection should return doc unchanged, got %d fields", result.Len())
	}
}

// Test ApplyProjection with projection containing only _id
func TestApplyProjection_OnlyID(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("Alice"))

	// Projection with only _id: 0 (defaults to include mode, excludes _id only)
	proj := bson.NewDocument()
	proj.Set("_id", bson.VInt32(0))

	result := ApplyProjection(doc, proj)

	if _, ok := result.Get("_id"); ok {
		t.Error("_id should be excluded")
	}
	// In include mode, only explicitly included fields are kept (none besides _id which is excluded)
	if _, ok := result.Get("name"); ok {
		t.Error("name should not be included when not explicitly included")
	}
}

// Test ApplyProjection include mode where field doesn't exist in doc
func TestApplyProjection_FieldNotInDoc(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("Alice"))

	// Include non-existent field
	proj := bson.NewDocument()
	proj.Set("nonexistent", bson.VInt32(1))

	result := ApplyProjection(doc, proj)

	// Should only have _id (default)
	if result.Len() != 1 {
		t.Errorf("expected only _id, got %d fields", result.Len())
	}
}
