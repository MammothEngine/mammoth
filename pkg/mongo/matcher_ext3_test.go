package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Test matchTopLevelOperator for $and, $or, $nor, $not
func TestMatchTopLevelOperator(t *testing.T) {
	tests := []struct {
		name     string
		op       string
		val      bson.Value
		doc      *bson.Document
		expected bool
	}{
		{
			name: "$and - all match",
			op:   "$and",
			val: bson.VArray(bson.A(
				bson.VDoc(bson.D("name", bson.VString("alice"))),
				bson.VDoc(bson.D("age", bson.VDoc(bson.D("$gte", bson.VInt32(25))))),
			)),
			doc:      bson.D("name", bson.VString("alice"), "age", bson.VInt32(30)),
			expected: true,
		},
		{
			name: "$and - one fails",
			op:   "$and",
			val: bson.VArray(bson.A(
				bson.VDoc(bson.D("name", bson.VString("alice"))),
				bson.VDoc(bson.D("age", bson.VDoc(bson.D("$gte", bson.VInt32(35))))),
			)),
			doc:      bson.D("name", bson.VString("alice"), "age", bson.VInt32(30)),
			expected: false,
		},
		{
			name: "$or - one matches",
			op:   "$or",
			val: bson.VArray(bson.A(
				bson.VDoc(bson.D("name", bson.VString("bob"))),
				bson.VDoc(bson.D("age", bson.VDoc(bson.D("$gte", bson.VInt32(25))))),
			)),
			doc:      bson.D("name", bson.VString("alice"), "age", bson.VInt32(30)),
			expected: true,
		},
		{
			name: "$or - none match",
			op:   "$or",
			val: bson.VArray(bson.A(
				bson.VDoc(bson.D("name", bson.VString("bob"))),
				bson.VDoc(bson.D("age", bson.VDoc(bson.D("$gte", bson.VInt32(35))))),
			)),
			doc:      bson.D("name", bson.VString("alice"), "age", bson.VInt32(30)),
			expected: false,
		},
		{
			name: "$nor - none match (should match)",
			op:   "$nor",
			val: bson.VArray(bson.A(
				bson.VDoc(bson.D("name", bson.VString("bob"))),
				bson.VDoc(bson.D("age", bson.VDoc(bson.D("$gte", bson.VInt32(35))))),
			)),
			doc:      bson.D("name", bson.VString("alice"), "age", bson.VInt32(30)),
			expected: true,
		},
		{
			name: "$nor - one matches (should not match)",
			op:   "$nor",
			val: bson.VArray(bson.A(
				bson.VDoc(bson.D("name", bson.VString("alice"))),
				bson.VDoc(bson.D("age", bson.VDoc(bson.D("$gte", bson.VInt32(35))))),
			)),
			doc:      bson.D("name", bson.VString("alice"), "age", bson.VInt32(30)),
			expected: false,
		},
		{
			name:     "$not - condition false",
			op:       "$not",
			val:      bson.VDoc(bson.D("name", bson.VString("bob"))),
			doc:      bson.D("name", bson.VString("alice")),
			expected: true,
		},
		{
			name:     "$not - condition true",
			op:       "$not",
			val:      bson.VDoc(bson.D("name", bson.VString("alice"))),
			doc:      bson.D("name", bson.VString("alice")),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := matchTopLevelOperator(tc.op, tc.val, tc.doc)
			if result != tc.expected {
				t.Errorf("matchTopLevelOperator() = %v, want %v", result, tc.expected)
			}
		})
	}
}

// Test matchTopLevelOperator with unknown operator
func TestMatchTopLevelOperator_Unknown(t *testing.T) {
	doc := bson.D("name", bson.VString("alice"))
	val := bson.VDoc(bson.D("name", bson.VString("alice")))

	// Unknown operator returns true (matches all) by default
	result := matchTopLevelOperator("$unknown", val, doc)
	if result != true {
		t.Errorf("unknown operator should return true, got %v", result)
	}
}
