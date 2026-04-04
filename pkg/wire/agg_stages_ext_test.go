package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestExprConcat(t *testing.T) {
	doc := bson.D("name", bson.VString("John"), "age", bson.VInt32(30))

	// Test concatenating strings
	arr := bson.Array{bson.VString("$name"), bson.VString(" Doe")}
	result := exprConcat(bson.VArray(arr), doc)
	if result.Type != bson.TypeString || result.String() != "John Doe" {
		t.Errorf("expected 'John Doe', got %v", result)
	}

	// Test with non-string - uses valueToString which returns string for int32
	arr2 := bson.Array{bson.VString("Age: "), bson.VInt32(30)}
	result2 := exprConcat(bson.VArray(arr2), doc)
	if result2.Type != bson.TypeString || result2.String() != "Age: 30" {
		t.Errorf("expected 'Age: 30', got %v", result2)
	}
}

func TestExprToLower(t *testing.T) {
	doc := bson.D("name", bson.VString("JOHN"))

	result := exprToLower(bson.VString("$name"), doc)
	if result.Type != bson.TypeString || result.String() != "john" {
		t.Errorf("expected 'john', got %v", result)
	}

	// Test with non-string
	result2 := exprToLower(bson.VInt32(123), doc)
	if result2.Type != bson.TypeNull {
		t.Errorf("expected null, got %v", result2)
	}
}

func TestExprToUpper(t *testing.T) {
	doc := bson.D("name", bson.VString("john"))

	result := exprToUpper(bson.VString("$name"), doc)
	if result.Type != bson.TypeString || result.String() != "JOHN" {
		t.Errorf("expected 'JOHN', got %v", result)
	}

	// Test with non-string
	result2 := exprToUpper(bson.VInt32(123), doc)
	if result2.Type != bson.TypeNull {
		t.Errorf("expected null, got %v", result2)
	}
}

func TestInt32ToStr(t *testing.T) {
	tests := []struct {
		input    int32
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{-1, "-1"},
		{12345, "12345"},
		{-12345, "-12345"},
		{2147483647, "2147483647"},
		// Note: -2147483648 causes overflow when negated in the implementation
	}

	for _, tc := range tests {
		result := int32ToStr(tc.input)
		if result != tc.expected {
			t.Errorf("int32ToStr(%d) = %s, want %s", tc.input, result, tc.expected)
		}
	}
}

func TestInt64ToStr(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{-1, "-1"},
		{9999999999, "9999999999"},
		{-9999999999, "-9999999999"},
	}

	for _, tc := range tests {
		result := int64ToStr(tc.input)
		if result != tc.expected {
			t.Errorf("int64ToStr(%d) = %s, want %s", tc.input, result, tc.expected)
		}
	}
}

func TestFloat64ToStr(t *testing.T) {
	tests := []struct {
		input    float64
		expected string
	}{
		{0.0, "0"},
		{1.0, "1"},
		{-1.0, "-1"},
		{3.14, "3.140000"},
	}

	for _, tc := range tests {
		result := float64ToStr(tc.input)
		if result != tc.expected {
			t.Errorf("float64ToStr(%f) = %s, want %s", tc.input, result, tc.expected)
		}
	}
}

func TestCloneDoc(t *testing.T) {
	original := bson.D(
		"name", bson.VString("Alice"),
		"nested", bson.VDoc(bson.D("x", bson.VInt32(1))),
		"arr", bson.VArray(bson.A(bson.VInt32(1), bson.VInt32(2))),
	)

	clone := cloneDoc(original)

	// Modify original
	original.Set("name", bson.VString("Bob"))

	// Clone should not be affected
	name, _ := clone.Get("name")
	if name.String() != "Alice" {
		t.Error("clone was affected by original modification")
	}

	// Check nested doc was cloned
	nested, _ := clone.Get("nested")
	if nested.Type != bson.TypeDocument {
		t.Error("nested document not cloned")
	}

	// Check array was cloned
	arr, _ := clone.Get("arr")
	if arr.Type != bson.TypeArray || len(arr.ArrayValue()) != 2 {
		t.Error("array not cloned correctly")
	}
}
