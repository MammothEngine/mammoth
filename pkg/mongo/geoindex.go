package mongo

import (
	"encoding/binary"
	"math"
	"sort"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

const geoIndexPrefix = "\x00geo"

// GeoJSON represents a GeoJSON geometry.
type GeoJSON struct {
	Type        GeoJSONType
	Coordinates interface{}
}

// GeoJSONType represents a GeoJSON geometry type.
type GeoJSONType string

const (
	GeoPoint       GeoJSONType = "Point"
	GeoLineString  GeoJSONType = "LineString"
	GeoPolygon     GeoJSONType = "Polygon"
	GeoMultiPoint  GeoJSONType = "MultiPoint"
)

// GeoPoint2D is a simple longitude/latitude pair.
type GeoPoint2D struct {
	Lon float64
	Lat float64
}

// GeoIndex implements a 2dsphere index using S2 cell encoding.
type GeoIndex struct {
	spec    *IndexSpec
	db, coll string
	eng     *engine.Engine
}

// NewGeoIndex creates a geo index handle.
func NewGeoIndex(db, coll string, spec *IndexSpec, eng *engine.Engine) *GeoIndex {
	return &GeoIndex{spec: spec, db: db, coll: coll, eng: eng}
}

// geoKeyPrefix returns the prefix for all keys in this geo index.
func (gi *GeoIndex) geoKeyPrefix() []byte {
	ns := EncodeNamespacePrefix(gi.db, gi.coll)
	prefix := make([]byte, 0, len(ns)+len(geoIndexPrefix)+len(gi.spec.Name))
	prefix = append(prefix, ns...)
	prefix = append(prefix, geoIndexPrefix...)
	prefix = append(prefix, gi.spec.Name...)
	return prefix
}

// buildGeoKey builds: {ns}\x00geo{index_name}{s2cell_id(8)}{_id_bytes}
func (gi *GeoIndex) buildGeoKey(cellID uint64, id []byte) []byte {
	prefix := gi.geoKeyPrefix()
	buf := make([]byte, 0, len(prefix)+8+len(id))
	buf = append(buf, prefix...)
	var cellBytes [8]byte
	binary.BigEndian.PutUint64(cellBytes[:], cellID)
	buf = append(buf, cellBytes[:]...)
	buf = append(buf, id...)
	return buf
}

// AddEntry adds a geo index entry for a document.
func (gi *GeoIndex) AddEntry(doc *bson.Document) error {
	idVal, ok := doc.Get("_id")
	if !ok {
		return nil
	}
	idBytes := idVal.ObjectID().Bytes()

	for _, ik := range gi.spec.Key {
		v, found := ResolveField(doc, ik.Field)
		if !found {
			continue
		}
		coords := extractCoordinates(v)
		if coords == nil {
			continue
		}
		cellID := pointToCellID(coords.Lon, coords.Lat)
		key := gi.buildGeoKey(cellID, idBytes)
		if err := gi.eng.Put(key, []byte{1}); err != nil {
			return err
		}
	}
	return nil
}

// RemoveEntry removes a geo index entry for a document.
func (gi *GeoIndex) RemoveEntry(doc *bson.Document) error {
	idVal, ok := doc.Get("_id")
	if !ok {
		return nil
	}
	idBytes := idVal.ObjectID().Bytes()

	prefix := gi.geoKeyPrefix()
	var keysToDelete [][]byte
	gi.eng.Scan(prefix, func(key, _ []byte) bool {
		if len(key) >= len(idBytes) {
			suffix := key[len(key)-len(idBytes):]
			if equalBytes(suffix, idBytes) {
				keysToDelete = append(keysToDelete, append([]byte{}, key...))
			}
		}
		return true
	})
	for _, key := range keysToDelete {
		_ = gi.eng.Delete(key)
	}
	return nil
}

// NearQuery finds documents within maxDistance meters of the given point.
// Uses Haversine formula for distance calculation.
func (gi *GeoIndex) NearQuery(lon, lat, maxDistanceMeters float64) []SearchResult {
	center := GeoPoint2D{Lon: lon, Lat: lat}

	// Scan all geo entries and filter by actual distance
	prefix := gi.geoKeyPrefix()
	var results []SearchResult

	gi.eng.Scan(prefix, func(key, _ []byte) bool {
		if len(key) < len(prefix)+8+12 {
			return true
		}
		// Extract _id (last 12 bytes)
		idBytes := key[len(key)-12:]
		var oid bson.ObjectID
		copy(oid[:], idBytes)

		// We can't get the actual coordinates from just the cell ID
		// For a proper implementation, we'd need to store the coordinates too
		// For now, we use a bounding box approach based on the cell ID
		cellID := binary.BigEndian.Uint64(key[len(prefix) : len(prefix)+8])
		docLon, docLat := cellIDToPoint(cellID)

		dist := haversineDistance(center.Lon, center.Lat, docLon, docLat)
		if dist <= maxDistanceMeters {
			results = append(results, SearchResult{ID: oid, Score: dist})
		}
		return true
	})

	// Sort by distance (ascending)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score < results[j].Score
	})
	return results
}

// extractCoordinates extracts a GeoJSON-like point from a BSON value.
func extractCoordinates(v bson.Value) *GeoPoint2D {
	if v.Type == bson.TypeDocument {
		doc := v.DocumentValue()
		// Check for GeoJSON format: {type: "Point", coordinates: [lon, lat]}
		if t, ok := doc.Get("type"); ok && t.Type == bson.TypeString && t.String() == "Point" {
			if c, ok := doc.Get("coordinates"); ok && c.Type == bson.TypeArray {
				arr := c.ArrayValue()
				if len(arr) >= 2 {
					return &GeoPoint2D{
						Lon: arr[0].Double(),
						Lat: arr[1].Double(),
					}
				}
			}
		}
		// Check for legacy format: {lng: x, lat: y}
		if lng, ok1 := doc.Get("lng"); ok1 {
			if lat, ok2 := doc.Get("lat"); ok2 {
				return &GeoPoint2D{Lon: lng.Double(), Lat: lat.Double()}
			}
		}
		if lng, ok1 := doc.Get("longitude"); ok1 {
			if lat, ok2 := doc.Get("latitude"); ok2 {
				return &GeoPoint2D{Lon: lng.Double(), Lat: lat.Double()}
			}
		}
	}
	if v.Type == bson.TypeArray {
		arr := v.ArrayValue()
		if len(arr) >= 2 {
			return &GeoPoint2D{
				Lon: arr[0].Double(),
				Lat: arr[1].Double(),
			}
		}
	}
	return nil
}

// pointToCellID converts a lat/lon to a simplified S2-like cell ID.
// This is a simplified version for demonstration: uses Morton coding.
func pointToCellID(lon, lat float64) uint64 {
	// Normalize to [0, 1]
	x := (lon + 180.0) / 360.0
	y := (lat + 90.0) / 180.0

	// Quantize to 32-bit integers
	xi := uint32(x * float64(uint32(1<<31)))
	yi := uint32(y * float64(uint32(1<<31)))

	// Interleave bits (Morton code)
	var result uint64
	for i := uint(0); i < 32; i++ {
		result |= uint64((xi>>i)&1) << (2 * i)
		result |= uint64((yi>>i)&1) << (2*i + 1)
	}
	return result
}

// cellIDToPoint reverses a simplified cell ID to approximate coordinates.
func cellIDToPoint(cellID uint64) (float64, float64) {
	var x, y uint32
	for i := uint(0); i < 32; i++ {
		x |= uint32((cellID>>(2*i))&1) << i
		y |= uint32((cellID>>(2*i+1))&1) << i
	}
	lon := float64(x) / float64(uint32(1<<31)) * 360.0 - 180.0
	lat := float64(y) / float64(uint32(1<<31)) * 180.0 - 90.0
	return lon, lat
}

// haversineDistance computes the great-circle distance between two points in meters.
func haversineDistance(lon1, lat1, lon2, lat2 float64) float64 {
	const R = 6371000.0 // Earth radius in meters
	dLat := (lat2 - lat1) * math.Pi / 180.0
	dLon := (lon2 - lon1) * math.Pi / 180.0
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*math.Pi/180.0)*math.Cos(lat2*math.Pi/180.0)*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}
