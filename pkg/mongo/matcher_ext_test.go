package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestMatchTextSearch(t *testing.T) {
	tests := []struct {
		name     string
		search   string
		doc      *bson.Document
		expected bool
	}{
		{
			name:     "simple match",
			search:   "hello",
			doc:      bson.D("message", bson.VString("hello world")),
			expected: true,
		},
		{
			name:     "no match",
			search:   "goodbye",
			doc:      bson.D("message", bson.VString("hello world")),
			expected: false,
		},
		{
			name:     "multiple tokens - all match",
			search:   "hello world",
			doc:      bson.D("message", bson.VString("hello there world")),
			expected: true,
		},
		{
			name:     "multiple tokens - partial match",
			search:   "hello goodbye",
			doc:      bson.D("message", bson.VString("hello world")),
			expected: false,
		},
		{
			name:     "empty search",
			search:   "",
			doc:      bson.D("message", bson.VString("hello")),
			expected: true,
		},
		{
			name:     "multiple string fields",
			search:   "alice bob",
			doc:      bson.D("name", bson.VString("alice"), "friend", bson.VString("bob")),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			searchDoc := bson.NewDocument()
			searchDoc.Set("$search", bson.VString(tt.search))
			result := matchTextSearch(bson.VDoc(searchDoc), tt.doc)
			if result != tt.expected {
				t.Errorf("matchTextSearch() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestTokenizeText(t *testing.T) {
	stemmer := NewPorterStemmer()

	tests := []struct {
		name     string
		text     string
		expected []string
	}{
		{
			name:     "simple words",
			text:     "hello world test",
			expected: []string{"hello", "world", "test"},
		},
		{
			name:     "mixed case",
			text:     "Hello World",
			expected: []string{"hello", "world"},
		},
		{
			name:     "with punctuation",
			text:     "hello, world! test.",
			expected: []string{"hello", "world", "test"},
		},
		{
			name:     "single character ignored",
			text:     "a b c test",
			expected: []string{"test"},
		},
		{
			name:     "empty string",
			text:     "",
			expected: nil,
		},
		{
			name:     "numbers",
			text:     "test123 hello456",
			expected: []string{"test123", "hello456"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tokenizeText(tt.text, stemmer)
			if len(result) != len(tt.expected) {
				t.Errorf("tokenizeText() = %v, want %v", result, tt.expected)
				return
			}
			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("tokenizeText()[%d] = %v, want %v", i, v, tt.expected[i])
				}
			}
		})
	}
}

func TestExtractBoundingBox(t *testing.T) {
	tests := []struct {
		name     string
		geomDoc  *bson.Document
		hasBBox  bool
		expected *geoBBox
	}{
		{
			name: "valid polygon",
			geomDoc: func() *bson.Document {
				// Polygon: [ [ [0,0], [0,1], [1,1], [1,0], [0,0] ] ]
				// Outer ring with points
				ring := bson.A(
					bson.VArray(bson.A(
						bson.VArray(bson.A(bson.VDouble(0), bson.VDouble(0))),
						bson.VArray(bson.A(bson.VDouble(0), bson.VDouble(1))),
						bson.VArray(bson.A(bson.VDouble(1), bson.VDouble(1))),
						bson.VArray(bson.A(bson.VDouble(1), bson.VDouble(0))),
						bson.VArray(bson.A(bson.VDouble(0), bson.VDouble(0))),
					)),
				)
				d := bson.NewDocument()
				d.Set("coordinates", bson.VArray(ring))
				return d
			}(),
			hasBBox: true,
			expected: &geoBBox{
				MinLon: 0, MaxLon: 1,
				MinLat: 0, MaxLat: 1,
			},
		},
		{
			name:     "no coordinates",
			geomDoc:  bson.NewDocument(),
			hasBBox:  false,
			expected: nil,
		},
		{
			name: "empty coordinates",
			geomDoc: func() *bson.Document {
				d := bson.NewDocument()
				d.Set("coordinates", bson.VArray(bson.A()))
				return d
			}(),
			hasBBox:  false,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractBoundingBox(tt.geomDoc)
			if tt.hasBBox {
				if result == nil {
					t.Fatal("expected non-nil bbox")
				}
				if result.MinLon != tt.expected.MinLon || result.MaxLon != tt.expected.MaxLon {
					t.Errorf("bbox lon = [%f, %f], want [%f, %f]",
						result.MinLon, result.MaxLon, tt.expected.MinLon, tt.expected.MaxLon)
				}
				if result.MinLat != tt.expected.MinLat || result.MaxLat != tt.expected.MaxLat {
					t.Errorf("bbox lat = [%f, %f], want [%f, %f]",
						result.MinLat, result.MaxLat, tt.expected.MinLat, tt.expected.MaxLat)
				}
			} else {
				if result != nil {
					t.Error("expected nil bbox")
				}
			}
		})
	}
}
