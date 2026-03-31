package mongo

import (
	"math"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupGeoIndexTest(t *testing.T) (*engine.Engine, *GeoIndex) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { eng.Close() })

	spec := &IndexSpec{
		Name:      "location_2dsphere",
		Key:       []IndexKey{{Field: "location"}},
		IndexType: "2dsphere",
	}
	return eng, NewGeoIndex("testdb", "places", spec, eng)
}

func makeGeoDoc(lon, lat float64) *bson.Document {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))

	loc := bson.NewDocument()
	loc.Set("type", bson.VString("Point"))
	arr := make(bson.Array, 2)
	arr[0] = bson.VDouble(lon)
	arr[1] = bson.VDouble(lat)
	loc.Set("coordinates", bson.VArray(arr))
	doc.Set("location", bson.VDoc(loc))

	return doc
}

func TestGeoIndex_AddAndNearQuery(t *testing.T) {
	_, gi := setupGeoIndexTest(t)

	// Istanbul: ~28.98, 41.01
	gi.AddEntry(makeGeoDoc(28.9784, 41.0082))
	// Ankara: ~32.86, 39.92
	gi.AddEntry(makeGeoDoc(32.8597, 39.9334))
	// Izmir: ~27.14, 38.42
	gi.AddEntry(makeGeoDoc(27.1428, 38.4237))

	// Search within 100km of Istanbul
	results := gi.NearQuery(28.9784, 41.0082, 100000)
	if len(results) < 1 {
		t.Fatalf("NearQuery(100km) = %d results, want at least 1", len(results))
	}

	// Istanbul should be the closest (near 0 distance)
	if results[0].Score > 1000 {
		t.Errorf("closest result distance = %.0f, want < 1000m", results[0].Score)
	}

	// Results should be sorted by distance ascending
	for i := 1; i < len(results); i++ {
		if results[i].Score < results[i-1].Score {
			t.Errorf("results not sorted by distance at index %d", i)
		}
	}
}

func TestGeoIndex_NearQuerySmallRadius(t *testing.T) {
	_, gi := setupGeoIndexTest(t)

	gi.AddEntry(makeGeoDoc(28.9784, 41.0082)) // Istanbul
	gi.AddEntry(makeGeoDoc(32.8597, 39.9334)) // Ankara (~350km away)

	// 10km radius from Istanbul should only find Istanbul
	results := gi.NearQuery(28.9784, 41.0082, 10000)
	if len(results) != 1 {
		t.Errorf("NearQuery(10km) = %d results, want 1", len(results))
	}
}

func TestGeoIndex_RemoveEntry(t *testing.T) {
	eng, gi := setupGeoIndexTest(t)

	doc := makeGeoDoc(28.9784, 41.0082)
	gi.AddEntry(doc)

	results := gi.NearQuery(28.9784, 41.0082, 100000)
	if len(results) != 1 {
		t.Fatalf("before remove: results = %d, want 1", len(results))
	}

	gi.RemoveEntry(doc)

	// Verify index entries are removed
	prefix := gi.geoKeyPrefix()
	count := 0
	eng.Scan(prefix, func(_, _ []byte) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("after remove: entries = %d, want 0", count)
	}
}

func TestGeoIndex_ExtractCoordinatesGeoJSON(t *testing.T) {
	loc := bson.NewDocument()
	loc.Set("type", bson.VString("Point"))
	arr := make(bson.Array, 2)
	arr[0] = bson.VDouble(29.0)
	arr[1] = bson.VDouble(41.0)
	loc.Set("coordinates", bson.VArray(arr))

	pt := extractCoordinates(bson.VDoc(loc))
	if pt == nil {
		t.Fatal("extractCoordinates returned nil")
	}
	if math.Abs(pt.Lon-29.0) > 0.001 || math.Abs(pt.Lat-41.0) > 0.001 {
		t.Errorf("got lon=%.3f lat=%.3f, want 29.0, 41.0", pt.Lon, pt.Lat)
	}
}

func TestGeoIndex_ExtractCoordinatesArray(t *testing.T) {
	arr := make(bson.Array, 2)
	arr[0] = bson.VDouble(27.0)
	arr[1] = bson.VDouble(38.0)

	pt := extractCoordinates(bson.VArray(arr))
	if pt == nil {
		t.Fatal("extractCoordinates returned nil for array")
	}
	if math.Abs(pt.Lon-27.0) > 0.001 || math.Abs(pt.Lat-38.0) > 0.001 {
		t.Errorf("got lon=%.3f lat=%.3f, want 27.0, 38.0", pt.Lon, pt.Lat)
	}
}

func TestHaversineDistance(t *testing.T) {
	// Istanbul to Ankara: ~350km
	dist := haversineDistance(28.9784, 41.0082, 32.8597, 39.9334)
	if dist < 300000 || dist > 400000 {
		t.Errorf("Istanbul-Ankara distance = %.0f, expected ~350000m", dist)
	}

	// Same point should be 0
	dist = haversineDistance(28.9784, 41.0082, 28.9784, 41.0082)
	if dist > 1 {
		t.Errorf("same point distance = %.2f, expected ~0", dist)
	}
}
