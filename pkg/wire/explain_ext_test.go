package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestIsSortCoveredByIndex(t *testing.T) {
	tests := []struct {
		name     string
		spec     *mongo.IndexSpec
		sortDoc  *bson.Document
		expected bool
	}{
		{
			name: "exact match ascending",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{{Field: "name", Descending: false}},
			},
			sortDoc:  bson.D("name", bson.VInt32(1)),
			expected: true,
		},
		{
			name: "exact match descending",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{{Field: "name", Descending: true}},
			},
			sortDoc:  bson.D("name", bson.VInt32(-1)),
			expected: true,
		},
		{
			name: "mismatch direction",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{{Field: "name", Descending: false}},
			},
			sortDoc:  bson.D("name", bson.VInt32(-1)),
			expected: false,
		},
		{
			name: "mismatch field",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{{Field: "name", Descending: false}},
			},
			sortDoc:  bson.D("age", bson.VInt32(1)),
			expected: false,
		},
		{
			name: "index has more fields",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{
					{Field: "name", Descending: false},
					{Field: "age", Descending: false},
				},
			},
			sortDoc:  bson.D("name", bson.VInt32(1)),
			expected: true,
		},
		{
			name: "sort has more fields than index",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{{Field: "name", Descending: false}},
			},
			sortDoc: bson.D(
				"name", bson.VInt32(1),
				"age", bson.VInt32(1),
			),
			expected: false,
		},
		{
			name: "empty sort",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{{Field: "name", Descending: false}},
			},
			sortDoc:  bson.NewDocument(),
			expected: true,
		},
		{
			name: "empty index",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{},
			},
			sortDoc:  bson.D("name", bson.VInt32(1)),
			expected: false,
		},
		{
			name: "compound index match",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{
					{Field: "a", Descending: false},
					{Field: "b", Descending: true},
				},
			},
			sortDoc: bson.D(
				"a", bson.VInt32(1),
				"b", bson.VInt32(-1),
			),
			expected: true,
		},
		{
			name: "compound index prefix",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{
					{Field: "a", Descending: false},
					{Field: "b", Descending: false},
					{Field: "c", Descending: false},
				},
			},
			sortDoc: bson.D(
				"a", bson.VInt32(1),
				"b", bson.VInt32(1),
			),
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isSortCoveredByIndex(tc.spec, tc.sortDoc)
			if result != tc.expected {
				t.Errorf("isSortCoveredByIndex() = %v, want %v", result, tc.expected)
			}
		})
	}
}
