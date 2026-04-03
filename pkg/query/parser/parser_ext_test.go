package parser

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Test parseInOperator error case
func TestParseInOperatorError(t *testing.T) {
	// Test $in with non-array value
	doc := bson.NewDocument()
	opDoc := bson.NewDocument()
	opDoc.Set("$in", bson.VString("not an array"))
	doc.Set("field", bson.VDoc(opDoc))

	_, err := Parse(doc)
	// Should error since $in requires array
	if err == nil {
		t.Error("expected error for $in with non-array")
	}
}

// Test parseAllOperator error case
func TestParseAllOperatorError(t *testing.T) {
	// Test with non-array value
	doc := bson.NewDocument()
	opDoc := bson.NewDocument()
	opDoc.Set("$all", bson.VString("not an array"))
	doc.Set("field", bson.VDoc(opDoc))

	_, err := Parse(doc)
	// Should error since $all requires array
	if err == nil {
		t.Error("expected error for $all with non-array")
	}
}

// Test parseElemMatchOperator error case
func TestParseElemMatchOperatorError(t *testing.T) {
	// Test with non-document value
	doc := bson.NewDocument()
	opDoc := bson.NewDocument()
	opDoc.Set("$elemMatch", bson.VString("not a document"))
	doc.Set("field", bson.VDoc(opDoc))

	_, err := Parse(doc)
	// Should error since $elemMatch requires document
	if err == nil {
		t.Error("expected error for $elemMatch with non-document")
	}
}

// Test parseModOperator
func TestParseModOperator(t *testing.T) {
	// Valid $mod
	doc := bson.NewDocument()
	opDoc := bson.NewDocument()
	opDoc.Set("$mod", bson.VArray(bson.A(bson.VInt32(10), bson.VInt32(1))))
	doc.Set("field", bson.VDoc(opDoc))

	node, err := Parse(doc)
	if err != nil {
		t.Errorf("unexpected error for valid $mod: %v", err)
	}
	if node == nil {
		t.Error("expected node for valid $mod")
	}
}

// Test toBool, toString, toInt
func TestTypeConverters(t *testing.T) {
	tests := []struct {
		name     string
		val      bson.Value
		boolVal  bool
		strVal   string
		intVal   int
	}{
		{"bool true", bson.VBool(true), true, "", -1},
		{"bool false", bson.VBool(false), false, "", -1},
		{"int32", bson.VInt32(42), true, "42", 42},
		{"int32 zero", bson.VInt32(0), false, "0", 0},
		{"int64", bson.VInt64(100), true, "100", 100},
		{"string true", bson.VString("true"), true, "true", -1},
		{"string false", bson.VString("false"), true, "false", -1}, // strings are truthy
		{"string number", bson.VString("123"), true, "123", -1},
		{"double", bson.VDouble(3.14), true, "3.14", 3},
		{"null", bson.VNull(), false, "", -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Test toBool
			boolResult := toBool(tc.val)
			if boolResult != tc.boolVal {
				t.Errorf("toBool(%v) = %v, want %v", tc.val, boolResult, tc.boolVal)
			}

			// Test toString
			strResult := toString(tc.val)
			if strResult != tc.strVal {
				t.Errorf("toString(%v) = %q, want %q", tc.val, strResult, tc.strVal)
			}

			// Test toInt
			intResult := toInt(tc.val)
			if intResult != tc.intVal {
				t.Errorf("toInt(%v) = %d, want %d", tc.val, intResult, tc.intVal)
			}
		})
	}
}

// Test geospatial operators
func TestParseGeoOperators(t *testing.T) {
	tests := []struct {
		name     string
		operator string
		nodeType NodeType
	}{
		{"$near", "$near", NodeNear},
		{"$nearSphere", "$nearSphere", NodeNearSphere},
		{"$geoWithin", "$geoWithin", NodeGeoWithin},
		{"$geoIntersects", "$geoIntersects", NodeGeoIntersects},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			doc := bson.NewDocument()
			opDoc := bson.NewDocument()
			opDoc.Set("$geometry", bson.VDoc(bson.D(
				"type", bson.VString("Point"),
				"coordinates", bson.VArray(bson.A(bson.VDouble(0), bson.VDouble(0))),
			)))
			doc.Set("location", bson.VDoc(opDoc))

			// Modify operator in document based on test case
			if tc.operator == "$near" {
				doc = bson.NewDocument()
				opDoc := bson.NewDocument()
				opDoc.Set("$near", bson.VDoc(bson.D(
					"$geometry", bson.VDoc(bson.D(
						"type", bson.VString("Point"),
						"coordinates", bson.VArray(bson.A(bson.VDouble(0), bson.VDouble(0))),
					)),
				)))
				doc.Set("location", bson.VDoc(opDoc))
			}

			// Test that parsing doesn't panic
			_, err := Parse(doc)
			_ = err
		})
	}
}

// Test parseTextSearch
func TestParseTextSearch(t *testing.T) {
	// Valid $text
	doc := bson.NewDocument()
	textDoc := bson.NewDocument()
	textDoc.Set("$search", bson.VString("hello world"))
	doc.Set("$text", bson.VDoc(textDoc))

	node, err := Parse(doc)
	if err != nil {
		t.Errorf("unexpected error for $text: %v", err)
	}
	if node == nil {
		t.Error("expected node for $text")
	}
}

// Test parseTopLevelOperator with $text
func TestParseTopLevelOperator_Text(t *testing.T) {
	doc := bson.NewDocument()
	textDoc := bson.NewDocument()
	textDoc.Set("$search", bson.VString("search term"))
	doc.Set("$text", bson.VDoc(textDoc))

	node, err := Parse(doc)
	if err != nil {
		t.Errorf("Parse with $text: %v", err)
	}
	if node == nil {
		t.Error("expected node for $text")
	}
}

// Test compareValues with more types
func TestCompareValuesMoreTypes(t *testing.T) {
	tests := []struct {
		a        bson.Value
		b        bson.Value
		expected int
	}{
		// Binary comparisons
		{bson.VBinary(bson.BinaryGeneric, []byte{1, 2}), bson.VBinary(bson.BinaryGeneric, []byte{1, 2}), 0},
		{bson.VBinary(bson.BinaryGeneric, []byte{1, 2}), bson.VBinary(bson.BinaryGeneric, []byte{1, 3}), -1},
		{bson.VBinary(bson.BinaryGeneric, []byte{1, 3}), bson.VBinary(bson.BinaryGeneric, []byte{1, 2}), 1},

		// ObjectID comparisons
		{bson.VObjectID(bson.ObjectID{1, 2, 3}), bson.VObjectID(bson.ObjectID{1, 2, 3}), 0},
		{bson.VObjectID(bson.ObjectID{1, 2, 3}), bson.VObjectID(bson.ObjectID{1, 2, 4}), -1},

		// Array comparisons
		{bson.VArray(bson.A(bson.VInt32(1))), bson.VArray(bson.A(bson.VInt32(1))), 0},
		{bson.VArray(bson.A(bson.VInt32(1))), bson.VArray(bson.A(bson.VInt32(2))), -1},

		// Document comparisons
		{bson.VDoc(bson.D("a", bson.VInt32(1))), bson.VDoc(bson.D("a", bson.VInt32(1))), 0},
		{bson.VDoc(bson.D("a", bson.VInt32(1))), bson.VDoc(bson.D("b", bson.VInt32(1))), -1},

		// Different type comparisons (type ordering)
		{bson.VInt32(100), bson.VString("abc"), -1}, // numbers < strings
		{bson.VString("abc"), bson.VBool(true), -1}, // strings < bools
	}

	for _, tc := range tests {
		result := compareValues(tc.a, tc.b)
		if result != tc.expected {
			t.Errorf("compareValues(%v, %v) = %d, want %d", tc.a, tc.b, result, tc.expected)
		}
	}
}

// Test compareBytes
func TestCompareBytes(t *testing.T) {
	tests := []struct {
		a        []byte
		b        []byte
		expected int
	}{
		{[]byte{1, 2, 3}, []byte{1, 2, 3}, 0},
		{[]byte{1, 2, 3}, []byte{1, 2, 4}, -1},
		{[]byte{1, 2, 4}, []byte{1, 2, 3}, 1},
		{[]byte{1, 2}, []byte{1, 2, 3}, -1},
		{[]byte{1, 2, 3}, []byte{1, 2}, 1},
		{[]byte{}, []byte{}, 0},
		{[]byte{}, []byte{1}, -1},
		{[]byte{1}, []byte{}, 1},
	}

	for _, tc := range tests {
		result := compareBytes(tc.a, tc.b)
		if result != tc.expected {
			t.Errorf("compareBytes(%v, %v) = %d, want %d", tc.a, tc.b, result, tc.expected)
		}
	}
}
