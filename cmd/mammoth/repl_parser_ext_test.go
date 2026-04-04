package main

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Test parseNumber with scientific notation
func TestParseNumber_ScientificNotation(t *testing.T) {
	val, pos, err := parseNumber(`1.5e10`, 0)
	if err != nil {
		t.Fatalf("parseNumber error: %v", err)
	}
	if val.Type != bson.TypeDouble {
		t.Errorf("expected double for scientific notation, got %v", val.Type)
	}
	if pos != 6 {
		t.Errorf("expected pos=6, got %d", pos)
	}
}

// Test parseNumber with explicit positive
func TestParseNumber_ExplicitPositive(t *testing.T) {
	val, pos, err := parseNumber(`+42`, 0)
	if err != nil {
		t.Fatalf("parseNumber error: %v", err)
	}
	if val.Type != bson.TypeInt32 || val.Int32() != 42 {
		t.Errorf("expected int32 42, got %v", val)
	}
	if pos != 3 {
		t.Errorf("expected pos=3, got %d", pos)
	}
}

// Test parseKey with special characters
func TestParseKey_SpecialChars(t *testing.T) {
	key, _, err := parseKey(`key_with-special.chars: value`, 0)
	if err != nil {
		t.Fatalf("parseKey error: %v", err)
	}
	if key != "key_with-special.chars" {
		t.Errorf("expected key='key_with-special.chars', got '%s'", key)
	}
}

// Test parseDocument with trailing comma
func TestParseDocument_TrailingComma(t *testing.T) {
	doc, err := parseDocument(`{a: 1, b: 2,}`)
	if err != nil {
		t.Fatalf("parseDocument error: %v", err)
	}
	if doc.Len() != 2 {
		t.Errorf("expected 2 fields, got %d", doc.Len())
	}
}

// Test parseDocument with nested arrays
func TestParseDocument_NestedArray(t *testing.T) {
	doc, err := parseDocument(`{matrix: [[1, 2], [3, 4]]}`)
	if err != nil {
		t.Fatalf("parseDocument error: %v", err)
	}
	matrix, ok := doc.Get("matrix")
	if !ok {
		t.Fatal("expected 'matrix' field")
	}
	if matrix.Type != bson.TypeArray {
		t.Errorf("expected array, got %v", matrix.Type)
	}
}

// Test parseValue with deeply nested document
func TestParseValue_DeepNesting(t *testing.T) {
	val, _, err := parseValue(`{a: {b: {c: {d: 1}}}}`, 0)
	if err != nil {
		t.Fatalf("parseValue error: %v", err)
	}
	if val.Type != bson.TypeDocument {
		t.Errorf("expected document, got %v", val.Type)
	}
}

// Test parseValue with multiple operators
func TestParseValue_MultipleOperators(t *testing.T) {
	doc, err := parseDocument(`{$and: [{a: 1}, {b: 2}], $or: [{c: 3}]}`)
	if err != nil {
		t.Fatalf("parseDocument error: %v", err)
	}
	if doc.Len() != 2 {
		t.Errorf("expected 2 fields, got %d", doc.Len())
	}
}

// Test findMatchingBrace with string containing braces
func TestFindMatchingBrace_StringWithBraces(t *testing.T) {
	s := `{key: "value with { and }"}`
	pos, err := findMatchingBrace(s, 0)
	if err != nil {
		t.Fatalf("findMatchingBrace error: %v", err)
	}
	// String is 27 chars (0-26), last valid index is 26
	if pos != 26 {
		t.Errorf("expected pos=26, got %d", pos)
	}
}

// Test findMatchingBracket with string containing brackets
func TestFindMatchingBracket_StringWithBrackets(t *testing.T) {
	s := `["value with [ and ]"]`
	pos, err := findMatchingBracket(s, 0)
	if err != nil {
		t.Fatalf("findMatchingBracket error: %v", err)
	}
	if pos != 21 {
		t.Errorf("expected pos=21, got %d", pos)
	}
}

// Test parseDocument with quoted keys
func TestParseDocument_QuotedKeys(t *testing.T) {
	doc, err := parseDocument(`{"key with spaces": 1, "key-with-dashes": 2}`)
	if err != nil {
		t.Fatalf("parseDocument error: %v", err)
	}
	if doc.Len() != 2 {
		t.Errorf("expected 2 fields, got %d", doc.Len())
	}
	v1, ok1 := doc.Get("key with spaces")
	if !ok1 || v1.Int32() != 1 {
		t.Error("expected 'key with spaces' = 1")
	}
	v2, ok2 := doc.Get("key-with-dashes")
	if !ok2 || v2.Int32() != 2 {
		t.Error("expected 'key-with-dashes' = 2")
	}
}

// Test parseDocument with extra whitespace
func TestParseDocument_ExtraWhitespace(t *testing.T) {
	doc, err := parseDocument(`{
		name:    "test"  ,
		value :  42
	}`)
	if err != nil {
		t.Fatalf("parseDocument error: %v", err)
	}
	if doc.Len() != 2 {
		t.Errorf("expected 2 fields, got %d", doc.Len())
	}
}

// Test parseArray with nested document
func TestParseArray_WithDoc(t *testing.T) {
	// parseArray takes inner content without brackets
	arr, err := parseArray(`1, {a: 2}, 3`)
	if err != nil {
		t.Fatalf("parseArray error: %v", err)
	}
	if len(arr) != 3 {
		t.Errorf("expected 3 elements, got %d", len(arr))
	}
	if arr[1].Type != bson.TypeDocument {
		t.Errorf("expected second element to be document, got %v", arr[1].Type)
	}
}
