package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Test bsonTypeName with all BSON types
func TestBsonTypeName_AllTypes(t *testing.T) {
	tests := []struct {
		typ      bson.BSONType
		expected string
	}{
		{bson.TypeDouble, "double"},
		{bson.TypeString, "string"},
		{bson.TypeDocument, "object"},
		{bson.TypeArray, "array"},
		{bson.TypeBinary, "binData"},
		{bson.TypeObjectID, "objectId"},
		{bson.TypeBoolean, "bool"},
		{bson.TypeDateTime, "date"},
		{bson.TypeNull, "null"},
		{bson.TypeRegex, "regex"},
		{bson.TypeInt32, "int"},
		{bson.TypeInt64, "long"},
		{bson.TypeTimestamp, "timestamp"},
		{bson.BSONType(0xFF), "unknown"}, // Unknown type
	}

	for _, tc := range tests {
		result := bsonTypeName(tc.typ)
		if result != tc.expected {
			t.Errorf("bsonTypeName(%v) = %q, want %q", tc.typ, result, tc.expected)
		}
	}
}

// Test bsonTypeAlias with all BSON types
func TestBsonTypeAlias_AllTypes(t *testing.T) {
	tests := []struct {
		typ      bson.BSONType
		expected string
	}{
		{bson.TypeDouble, "number"},
		{bson.TypeInt32, "number"},
		{bson.TypeInt64, "number"},
		{bson.TypeString, ""},
		{bson.TypeBoolean, ""},
		{bson.TypeNull, ""},
		{bson.TypeObjectID, ""},
		{bson.TypeDocument, ""},
		{bson.TypeArray, ""},
	}

	for _, tc := range tests {
		result := bsonTypeAlias(tc.typ)
		if result != tc.expected {
			t.Errorf("bsonTypeAlias(%v) = %q, want %q", tc.typ, result, tc.expected)
		}
	}
}
