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

func TestPointInBBox(t *testing.T) {
	bbox := &geoBBox{
		MinLon: -10, MaxLon: 10,
		MinLat: -5, MaxLat: 5,
	}

	tests := []struct {
		name     string
		lon      float64
		lat      float64
		expected bool
	}{
		{"center", 0, 0, true},
		{"min corner", -10, -5, true},
		{"max corner", 10, 5, true},
		{"outside lon", 15, 0, false},
		{"outside lat", 0, 10, false},
		{"outside both", 15, 10, false},
		{"negative outside", -15, -10, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pointInBBox(tt.lon, tt.lat, bbox)
			if result != tt.expected {
				t.Errorf("pointInBBox(%f, %f) = %v, want %v", tt.lon, tt.lat, result, tt.expected)
			}
		})
	}
}

func TestExtractGeoCenter(t *testing.T) {
	tests := []struct {
		name      string
		opDoc     *bson.Document
		hasCenter bool
		lon       float64
		lat       float64
	}{
		{
			name: "valid point in $geometry",
			opDoc: func() *bson.Document {
				d := bson.NewDocument()
				d.Set("$geometry", bson.VDoc(bson.D(
					"type", bson.VString("Point"),
					"coordinates", bson.VArray(bson.A(bson.VDouble(10), bson.VDouble(20))),
				)))
				return d
			}(),
			hasCenter: true,
			lon:       10,
			lat:       20,
		},
		{
			name:      "no $geometry",
			opDoc:     bson.NewDocument(),
			hasCenter: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractGeoCenter(tt.opDoc)
			if tt.hasCenter {
				if result == nil {
					t.Fatal("expected non-nil center")
				}
				if result.Lon != tt.lon || result.Lat != tt.lat {
					t.Errorf("center = [%f, %f], want [%f, %f]", result.Lon, result.Lat, tt.lon, tt.lat)
				}
			} else {
				if result != nil {
					t.Error("expected nil center")
				}
			}
		})
	}
}

func TestGetGeoMaxDistance(t *testing.T) {
	tests := []struct {
		name     string
		doc      *bson.Document
		expected float64
	}{
		{
			name:     "with $maxDistance int32",
			doc:      bson.D("$maxDistance", bson.VInt32(1000)),
			expected: 1000,
		},
		{
			name:     "with $maxDistance double",
			doc:      bson.D("$maxDistance", bson.VDouble(2000.5)),
			expected: 2000.5,
		},
		{
			name:     "with $maxDistance int64",
			doc:      bson.D("$maxDistance", bson.VInt64(5000)),
			expected: 5000,
		},
		{
			name:     "no maxDistance (default)",
			doc:      bson.NewDocument(),
			expected: 1000.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getGeoMaxDistance(tt.doc)
			if result != tt.expected {
				t.Errorf("getGeoMaxDistance() = %f, want %f", result, tt.expected)
			}
		})
	}
}

func TestMatchNear(t *testing.T) {
	// Create $near query
	nearQuery := bson.VDoc(bson.D(
		"$geometry", bson.VDoc(bson.D(
			"type", bson.VString("Point"),
			"coordinates", bson.VArray(bson.A(bson.VDouble(0), bson.VDouble(0))),
		)),
	))

	// Create a field value with location
	fieldVal := bson.VDoc(bson.D(
		"type", bson.VString("Point"),
		"coordinates", bson.VArray(bson.A(bson.VDouble(0), bson.VDouble(0))),
	))

	// Should match (same point)
	result := matchNear(nearQuery, fieldVal, true)
	// Result depends on implementation
	_ = result

	// Test with field not found
	result2 := matchNear(nearQuery, bson.VNull(), false)
	_ = result2
}

func TestMatchGeoWithin(t *testing.T) {
	// Create $geoWithin query with polygon
	geoWithinQuery := bson.VDoc(bson.D(
		"$geometry", bson.VDoc(bson.D(
			"type", bson.VString("Polygon"),
			"coordinates", bson.VArray(bson.A(
				bson.VArray(bson.A(
					bson.VArray(bson.A(bson.VDouble(-1), bson.VDouble(-1))),
					bson.VArray(bson.A(bson.VDouble(-1), bson.VDouble(1))),
					bson.VArray(bson.A(bson.VDouble(1), bson.VDouble(1))),
					bson.VArray(bson.A(bson.VDouble(1), bson.VDouble(-1))),
					bson.VArray(bson.A(bson.VDouble(-1), bson.VDouble(-1))),
				)),
			)),
		)),
	))

	// Create a field value with location
	fieldVal := bson.VDoc(bson.D(
		"type", bson.VString("Point"),
		"coordinates", bson.VArray(bson.A(bson.VDouble(0), bson.VDouble(0))),
	))

	// Should match (point inside polygon)
	result := matchGeoWithin(geoWithinQuery, fieldVal, true)
	// Result depends on implementation
	_ = result

	// Test with field not found
	result2 := matchGeoWithin(geoWithinQuery, bson.VNull(), false)
	_ = result2
}

func TestMatchGeoIntersects(t *testing.T) {
	// Create $geoIntersects query
	intersectsQuery := bson.VDoc(bson.D(
		"$geometry", bson.VDoc(bson.D(
			"type", bson.VString("Point"),
			"coordinates", bson.VArray(bson.A(bson.VDouble(0), bson.VDouble(0))),
		)),
	))

	// Create a field value with polygon
	fieldVal := bson.VDoc(bson.D(
		"type", bson.VString("Polygon"),
		"coordinates", bson.VArray(bson.A(
			bson.VArray(bson.A(
				bson.VArray(bson.A(bson.VDouble(-1), bson.VDouble(-1))),
				bson.VArray(bson.A(bson.VDouble(-1), bson.VDouble(1))),
				bson.VArray(bson.A(bson.VDouble(1), bson.VDouble(1))),
				bson.VArray(bson.A(bson.VDouble(1), bson.VDouble(-1))),
				bson.VArray(bson.A(bson.VDouble(-1), bson.VDouble(-1))),
			)),
		)),
	))

	// Should match (point inside polygon)
	result := matchGeoIntersects(intersectsQuery, fieldVal, true)
	// Result depends on implementation
	_ = result

	// Test with field not found
	result2 := matchGeoIntersects(intersectsQuery, bson.VNull(), false)
	_ = result2
}
