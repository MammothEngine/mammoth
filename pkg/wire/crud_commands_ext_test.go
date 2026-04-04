package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestDistinctKey(t *testing.T) {
	tests := []struct {
		name     string
		value    bson.Value
		expected string
	}{
		{
			name:     "string",
			value:    bson.VString("hello"),
			expected: "s:hello",
		},
		{
			name:     "int32",
			value:    bson.VInt32(42),
			expected: "i:42",
		},
		{
			name:     "int64",
			value:    bson.VInt64(9999999999),
			expected: "l:9999999999",
		},
		{
			name:     "double",
			value:    bson.VDouble(3.14),
			expected: "d:3.14",
		},
		{
			name:     "bool true",
			value:    bson.VBool(true),
			expected: "b:true",
		},
		{
			name:     "bool false",
			value:    bson.VBool(false),
			expected: "b:false",
		},
		{
			name:     "null",
			value:    bson.VNull(),
			expected: "n:",
		},
		{
			name:     "objectid",
			value:    bson.VObjectID(bson.NewObjectID()),
			expected: "o:", // will check prefix only
		},
		{
			name:     "int32 zero",
			value:    bson.VInt32(0),
			expected: "i:0",
		},
		{
			name:     "int32 negative",
			value:    bson.VInt32(-100),
			expected: "i:-100",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := distinctKey(tc.value)
			if tc.value.Type == bson.TypeObjectID {
				// ObjectID comparison - just check prefix
				if len(result) < 2 || result[:2] != "o:" {
					t.Errorf("distinctKey() = %v, expected 'o:' prefix", result)
				}
			} else if result != tc.expected {
				t.Errorf("distinctKey() = %v, want %v", result, tc.expected)
			}
		})
	}
}

func TestDistinctKey_DefaultCase(t *testing.T) {
	// Test with a type not explicitly handled
	arr := bson.VArray([]bson.Value{bson.VInt32(1)})
	result := distinctKey(arr)
	// Should use default case with type number
	if result == "" {
		t.Error("distinctKey() returned empty string for array")
	}
}
