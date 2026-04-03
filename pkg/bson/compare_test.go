package bson

import "testing"

func TestCompareTypeOrder(t *testing.T) {
	tests := []struct {
		a, b Value
		want int
	}{
		{VMinKey(), VNull(), -1},
		{VNull(), VInt32(0), -1},
		{VInt32(1), VInt32(2), -1},
		{VInt32(1), VInt32(1), 0},
		{VInt32(2), VInt32(1), 1},
		{VDouble(1.5), VDouble(2.5), -1},
		{VInt32(1), VDouble(1.0), 0},
		{VString("a"), VString("b"), -1},
		{VString("b"), VString("a"), 1},
		{VString("a"), VString("a"), 0},
		{VBool(false), VBool(true), -1},
		{VBool(true), VBool(false), 1},
		{VMaxKey(), VMinKey(), 1},
	}

	for _, tt := range tests {
		got := CompareValues(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("CompareValues(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestCompareCrossType(t *testing.T) {
	// Null < Numbers < String < Object < Array < BinData < ObjectId < Boolean < Date
	if CompareValues(VNull(), VInt32(0)) >= 0 {
		t.Error("null should be < number")
	}
	if CompareValues(VInt32(0), VString("")) >= 0 {
		t.Error("number should be < string")
	}
	if CompareValues(VString(""), VBool(false)) >= 0 {
		t.Error("string should be < boolean")
	}
	if CompareValues(VBool(false), VDateTime(0)) >= 0 {
		t.Error("boolean should be < datetime")
	}
}

func TestCompareDocuments(t *testing.T) {
	d1 := NewDocument()
	d1.Set("a", VInt32(1))
	d1.Set("b", VInt32(2))

	d2 := NewDocument()
	d2.Set("a", VInt32(1))
	d2.Set("b", VInt32(3))

	if CompareDocuments(d1, d2) >= 0 {
		t.Error("d1 should be < d2")
	}
}

func TestCompareArrays(t *testing.T) {
	a1 := A(VInt32(1), VInt32(2))
	a2 := A(VInt32(1), VInt32(3))

	if CompareValues(Value{Type: TypeArray, value: a1}, Value{Type: TypeArray, value: a2}) >= 0 {
		t.Error("a1 should be < a2")
	}
}

// Test typeOrder function with all BSON types
func TestTypeOrderAllTypes(t *testing.T) {
	tests := []struct {
		val  Value
		name string
	}{
		{VMinKey(), "MinKey"},
		{VNull(), "Null"},
		{VInt32(1), "Int32"},
		{VInt64(1), "Int64"},
		{VDouble(1), "Double"},
		{VString("test"), "String"},
		{VDoc(NewDocument()), "Document"},
		{VArray(A()), "Array"},
		{VBinary(BinaryGeneric, []byte{}), "Binary"},
		{VObjectID(NewObjectID()), "ObjectID"},
		{VBool(true), "Boolean"},
		{VDateTime(0), "DateTime"},
		{VTimestamp(0), "Timestamp"},
		{VRegex("pattern", "i"), "Regex"},
		{VJavaScript("function() {}"), "JavaScript"},
		{VSymbol("symbol"), "Symbol"},
		{VMaxKey(), "MaxKey"},
	}

	// Test that each type has a unique order
	orders := make(map[int]string)
	for _, tt := range tests {
		order := typeOrder(tt.val.Type)
		if other, ok := orders[order]; ok && other != tt.name {
			// Some types share order (like numeric types), that's expected
			if order != 1 { // order 1 is for all numeric types
				t.Errorf("typeOrder conflict: %s and %s both have order %d", tt.name, other, order)
			}
		}
		orders[order] = tt.name
	}

	// Test unknown type (use a type that's not in the switch)
	unknownType := typeOrder(TypeMaxKey + 100)
	if unknownType != 100 {
		t.Errorf("typeOrder for unknown type: expected 100, got %d", unknownType)
	}
}

// Test CompareDocuments edge cases
func TestCompareDocumentsEdgeCases(t *testing.T) {
	// Empty documents
	d1 := NewDocument()
	d2 := NewDocument()
	if CompareDocuments(d1, d2) != 0 {
		t.Error("two empty documents should be equal")
	}

	// Different lengths
	d3 := NewDocument()
	d3.Set("a", VInt32(1))
	if CompareDocuments(d1, d3) >= 0 {
		t.Error("empty doc should be < doc with fields")
	}
	if CompareDocuments(d3, d1) <= 0 {
		t.Error("doc with fields should be > empty doc")
	}

	// Same keys, different values
	d4 := NewDocument()
	d4.Set("a", VInt32(2))
	if CompareDocuments(d3, d4) >= 0 {
		t.Error("{a:1} should be < {a:2}")
	}

	// Different keys
	d5 := NewDocument()
	d5.Set("b", VInt32(1))
	if CompareDocuments(d3, d5) >= 0 {
		t.Error("{a:1} should be < {b:1} (a < b)")
	}

	// Multiple keys
	d6 := NewDocument()
	d6.Set("a", VInt32(1))
	d6.Set("b", VInt32(1))

	d7 := NewDocument()
	d7.Set("a", VInt32(1))
	d7.Set("b", VInt32(2))

	if CompareDocuments(d6, d7) >= 0 {
		t.Error("docs should differ at second key")
	}
}

// Test compareArrays edge cases
func TestCompareArraysEdgeCases(t *testing.T) {
	// Empty arrays
	a1 := A()
	a2 := A()
	v1 := Value{Type: TypeArray, value: a1}
	v2 := Value{Type: TypeArray, value: a2}
	if CompareValues(v1, v2) != 0 {
		t.Error("two empty arrays should be equal")
	}

	// Different lengths
	a3 := A(VInt32(1))
	v3 := Value{Type: TypeArray, value: a3}
	if CompareValues(v1, v3) >= 0 {
		t.Error("empty array should be < non-empty array")
	}
	if CompareValues(v3, v1) <= 0 {
		t.Error("non-empty array should be > empty array")
	}

	// Same length, different values
	a4 := A(VInt32(1), VInt32(2))
	a5 := A(VInt32(1), VInt32(3))
	v4 := Value{Type: TypeArray, value: a4}
	v5 := Value{Type: TypeArray, value: a5}
	if CompareValues(v4, v5) >= 0 {
		t.Error("[1,2] should be < [1,3]")
	}

	// Different lengths with same prefix
	a6 := A(VInt32(1), VInt32(2))
	a7 := A(VInt32(1), VInt32(2), VInt32(3))
	v6 := Value{Type: TypeArray, value: a6}
	v7 := Value{Type: TypeArray, value: a7}
	if CompareValues(v6, v7) >= 0 {
		t.Error("[1,2] should be < [1,2,3]")
	}
}

// Test compareBytes
func TestCompareBytes(t *testing.T) {
	tests := []struct {
		a, b []byte
		want int
	}{
		{[]byte{}, []byte{}, 0},
		{[]byte{1}, []byte{1}, 0},
		{[]byte{1, 2}, []byte{1, 2}, 0},
		{[]byte{1}, []byte{2}, -1},
		{[]byte{2}, []byte{1}, 1},
		{[]byte{}, []byte{1}, -1},
		{[]byte{1}, []byte{}, 1},
		{[]byte{1, 2}, []byte{1, 3}, -1},
		{[]byte{1, 3}, []byte{1, 2}, 1},
		{[]byte{1, 2, 3}, []byte{1, 2}, 1},
		{[]byte{1, 2}, []byte{1, 2, 3}, -1},
	}

	for _, tt := range tests {
		got := compareBytes(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("compareBytes(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

// Test compareStrings
func TestCompareStrings(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"a", "a", 0},
		{"a", "b", -1},
		{"b", "a", 1},
		{"", "a", -1},
		{"a", "", 1},
		{"abc", "abd", -1},
		{"abd", "abc", 1},
		{"abc", "abcd", -1},
		{"abcd", "abc", 1},
	}

	for _, tt := range tests {
		got := compareStrings(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("compareStrings(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

// Test CompareValues with Binary
func TestCompareValuesBinary(t *testing.T) {
	b1 := VBinary(BinaryGeneric, []byte{1, 2, 3})
	b2 := VBinary(BinaryGeneric, []byte{1, 2, 4})
	b3 := VBinary(BinaryFunction, []byte{1, 2, 3})

	// Same subtype, different data
	if CompareValues(b1, b2) >= 0 {
		t.Error("binary with smaller data should be <")
	}
	// Different subtype (BinaryFunction=0x01 > BinaryGeneric=0x00)
	// b3 (Function) should be > b1 (Generic)
	if CompareValues(b3, b1) <= 0 {
		t.Errorf("binary with higher subtype should be greater: got %d", CompareValues(b3, b1))
	}
}

// Test CompareValues with Regex
func TestCompareValuesRegex(t *testing.T) {
	r1 := VRegex("abc", "i")
	r2 := VRegex("def", "i")
	r3 := VRegex("abc", "m")

	if CompareValues(r1, r2) >= 0 {
		t.Error("regex with pattern 'abc' should be < 'def'")
	}
	if CompareValues(r1, r3) >= 0 {
		t.Error("regex with options 'i' should be < 'm' when pattern same")
	}
}

// Test CompareValues with ObjectID
func TestCompareValuesObjectID(t *testing.T) {
	// Create ObjectIDs with known byte values
	oid1Bytes := ObjectID{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b}
	oid2Bytes := ObjectID{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0c}

	v1 := VObjectID(oid1Bytes)
	v2 := VObjectID(oid2Bytes)

	if CompareValues(v1, v2) >= 0 {
		t.Error("ObjectID with smaller last byte should be <")
	}
	if CompareValues(v2, v1) <= 0 {
		t.Error("ObjectID with larger last byte should be >")
	}

	// Same ObjectID
	v3 := VObjectID(oid1Bytes)
	if CompareValues(v1, v3) != 0 {
		t.Error("same ObjectIDs should be equal")
	}
}

// Test compareNumbers edge cases
func TestCompareNumbers(t *testing.T) {
	tests := []struct {
		a, b Value
		want int
	}{
		{VInt32(1), VInt64(2), -1},
		{VInt64(2), VInt32(1), 1},
		{VInt32(1), VDouble(1.0), 0},
		{VInt32(1), VDouble(2.0), -1},
		{VDouble(2.0), VInt32(1), 1},
		{VInt64(100), VDouble(100.0), 0},
		{VDouble(3.14), VDouble(2.71), 1},
	}

	for _, tt := range tests {
		got := CompareValues(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("CompareValues(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}
