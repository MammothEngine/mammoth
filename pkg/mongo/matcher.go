package mongo

import (
	"regexp"
	"strings"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Matcher evaluates a BSON filter against documents.
type Matcher struct {
	filter *bson.Document
}

// NewMatcher creates a matcher from a BSON filter document.
func NewMatcher(filter *bson.Document) *Matcher {
	return &Matcher{filter: filter}
}

// Match returns true if the document matches the filter.
func (m *Matcher) Match(doc *bson.Document) bool {
	if m.filter == nil || m.filter.Len() == 0 {
		return true // empty filter matches everything
	}
	return matchDocument(m.filter, doc)
}

// matchDocument checks if all filter conditions are satisfied.
func matchDocument(filter, doc *bson.Document) bool {
	for _, e := range filter.Elements() {
		if strings.HasPrefix(e.Key, "$") {
			// Top-level operator
			if !matchTopLevelOperator(e.Key, e.Value, doc) {
				return false
			}
		} else {
			// Field condition: {field: value} or {field: {$gt: 5, ...}}
			fieldVal, found := ResolveField(doc, e.Key)
			if !matchFieldCondition(e.Value, fieldVal, found) {
				return false
			}
		}
	}
	return true
}

func matchTopLevelOperator(op string, val bson.Value, doc *bson.Document) bool {
	switch op {
	case "$and":
		arr := val.ArrayValue()
		for _, v := range arr {
			if !matchDocument(v.DocumentValue(), doc) {
				return false
			}
		}
		return true
	case "$or":
		arr := val.ArrayValue()
		for _, v := range arr {
			if matchDocument(v.DocumentValue(), doc) {
				return true
			}
		}
		return false
	case "$nor":
		arr := val.ArrayValue()
		for _, v := range arr {
			if matchDocument(v.DocumentValue(), doc) {
				return false
			}
		}
		return true
	case "$not":
		if val.Type == bson.TypeDocument {
			return !matchDocument(val.DocumentValue(), doc)
		}
		return true
	case "$text":
		return matchTextSearch(val, doc)
	}
	return true
}

// matchFieldCondition handles both implicit $eq and explicit operators.
func matchFieldCondition(condVal bson.Value, fieldVal bson.Value, fieldFound bool) bool {
	if condVal.Type == bson.TypeDocument {
		// Operator document: {field: {$gt: 5, $lt: 10}}
		ops := condVal.DocumentValue()
		for _, op := range ops.Elements() {
			if !matchOperator(op.Key, op.Value, fieldVal, fieldFound) {
				return false
			}
		}
		return true
	}
	// Implicit $eq: {field: value}
	if !fieldFound {
		// null filter matches missing field
		return condVal.Type == bson.TypeNull
	}
	return bson.CompareValues(condVal, fieldVal) == 0
}

func matchOperator(op string, opVal bson.Value, fieldVal bson.Value, fieldFound bool) bool {
	switch op {
	case "$eq":
		if !fieldFound {
			return opVal.Type == bson.TypeNull
		}
		return bson.CompareValues(opVal, fieldVal) == 0
	case "$ne":
		if !fieldFound {
			return opVal.Type != bson.TypeNull
		}
		return bson.CompareValues(opVal, fieldVal) != 0
	case "$gt":
		return fieldFound && bson.CompareValues(fieldVal, opVal) > 0
	case "$gte":
		return fieldFound && bson.CompareValues(fieldVal, opVal) >= 0
	case "$lt":
		return fieldFound && bson.CompareValues(fieldVal, opVal) < 0
	case "$lte":
		return fieldFound && bson.CompareValues(fieldVal, opVal) <= 0
	case "$in":
		arr := opVal.ArrayValue()
		if !fieldFound {
			for _, v := range arr {
				if v.Type == bson.TypeNull {
					return true
				}
			}
			return false
		}
		for _, v := range arr {
			if bson.CompareValues(fieldVal, v) == 0 {
				return true
			}
			// If field is array, check if any element matches
			if fieldVal.Type == bson.TypeArray {
				for _, elem := range fieldVal.ArrayValue() {
					if bson.CompareValues(elem, v) == 0 {
						return true
					}
				}
			}
		}
		return false
	case "$nin":
		arr := opVal.ArrayValue()
		for _, v := range arr {
			if fieldFound && bson.CompareValues(fieldVal, v) == 0 {
				return false
			}
		}
		return true
	case "$exists":
		wantExists := opVal.Boolean()
		return wantExists == fieldFound
	case "$type":
		if !fieldFound {
			return false
		}
		wantType := opVal.String()
		return bsonTypeName(fieldVal.Type) == wantType || bsonTypeAlias(fieldVal.Type) == wantType
	case "$regex":
		if !fieldFound || fieldVal.Type != bson.TypeString {
			return false
		}
		pattern := opVal.String()
		re, err := regexp.Compile(pattern)
		if err != nil {
			return false
		}
		return re.MatchString(fieldVal.String())
	case "$size":
		if !fieldFound || fieldVal.Type != bson.TypeArray {
			return false
		}
		return len(fieldVal.ArrayValue()) == int(opVal.Int32())
	case "$all":
		if !fieldFound || fieldVal.Type != bson.TypeArray {
			return false
		}
		arr := opVal.ArrayValue()
		fieldArr := fieldVal.ArrayValue()
		for _, v := range arr {
			found := false
			for _, fv := range fieldArr {
				if bson.CompareValues(v, fv) == 0 {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	case "$elemMatch":
		if !fieldFound || fieldVal.Type != bson.TypeArray {
			return false
		}
		matchDoc := opVal.DocumentValue()
		for _, elem := range fieldVal.ArrayValue() {
			if elem.Type == bson.TypeDocument {
				if matchDocument(matchDoc, elem.DocumentValue()) {
					return true
				}
			}
		}
		return false
	case "$not":
		// $not inverts the inner operator document
		if opVal.Type == bson.TypeDocument {
			return !matchFieldCondition(opVal, fieldVal, fieldFound)
		}
		return true
	case "$near":
		return matchNear(opVal, fieldVal, fieldFound)
	case "$geoWithin":
		return matchGeoWithin(opVal, fieldVal, fieldFound)
	case "$geoIntersects":
		return matchGeoIntersects(opVal, fieldVal, fieldFound)
	case "$mod":
		return matchMod(opVal, fieldVal, fieldFound)
	}
	return true
}

// matchMod handles $mod: [divisor, remainder] - matches if field % divisor == remainder.
func matchMod(opVal bson.Value, fieldVal bson.Value, fieldFound bool) bool {
	if !fieldFound {
		return false
	}
	if opVal.Type != bson.TypeArray {
		return false
	}
	arr := opVal.ArrayValue()
	if len(arr) != 2 {
		return false
	}

	divisor := bsonValueToFloat64(arr[0])
	remainder := bsonValueToFloat64(arr[1])
	fieldNum := bsonValueToFloat64(fieldVal)

	if divisor == 0 {
		return false
	}

	return int64(fieldNum)%int64(divisor) == int64(remainder)
}

// bsonValueToFloat64 converts a bson.Value to float64.
func bsonValueToFloat64(v bson.Value) float64 {
	switch v.Type {
	case bson.TypeDouble:
		return v.Double()
	case bson.TypeInt32:
		return float64(v.Int32())
	case bson.TypeInt64:
		return float64(v.Int64())
	default:
		return 0
	}
}

// ResolveField resolves a dot-notation path like "a.b.c" in a document.
func ResolveField(doc *bson.Document, path string) (bson.Value, bool) {
	parts := strings.SplitN(path, ".", 2)
	v, ok := doc.Get(parts[0])
	if !ok {
		return bson.Value{}, false
	}
	if len(parts) == 1 {
		return v, true
	}
	// Nested: value must be a document
	if v.Type == bson.TypeDocument {
		return ResolveField(v.DocumentValue(), parts[1])
	}
	// Array index
	if v.Type == bson.TypeArray {
		return bson.Value{}, false
	}
	return bson.Value{}, false
}

func bsonTypeName(t bson.BSONType) string {
	switch t {
	case bson.TypeDouble:
		return "double"
	case bson.TypeString:
		return "string"
	case bson.TypeDocument:
		return "object"
	case bson.TypeArray:
		return "array"
	case bson.TypeBinary:
		return "binData"
	case bson.TypeObjectID:
		return "objectId"
	case bson.TypeBoolean:
		return "bool"
	case bson.TypeDateTime:
		return "date"
	case bson.TypeNull:
		return "null"
	case bson.TypeRegex:
		return "regex"
	case bson.TypeInt32:
		return "int"
	case bson.TypeInt64:
		return "long"
	case bson.TypeTimestamp:
		return "timestamp"
	default:
		return "unknown"
	}
}

func bsonTypeAlias(t bson.BSONType) string {
	switch t {
	case bson.TypeDouble, bson.TypeInt32, bson.TypeInt64:
		return "number"
	default:
		return ""
	}
}

// matchTextSearch handles $text: {$search: "query"}.
// Checks if any string field in the document contains all query tokens (stemmed).
func matchTextSearch(val bson.Value, doc *bson.Document) bool {
	if val.Type != bson.TypeDocument {
		return false
	}
	textDoc := val.DocumentValue()
	searchVal, ok := textDoc.Get("$search")
	if !ok || searchVal.Type != bson.TypeString {
		return false
	}

	stemmer := NewPorterStemmer()
	queryTokens := tokenizeText(searchVal.String(), stemmer)
	if len(queryTokens) == 0 {
		return true
	}

	// Collect all string fields from the document
	var docText string
	for _, e := range doc.Elements() {
		if e.Value.Type == bson.TypeString {
			docText += " " + e.Value.String()
		}
	}
	docTokens := tokenizeText(docText, stemmer)

	tokenSet := make(map[string]bool, len(docTokens))
	for _, t := range docTokens {
		tokenSet[t] = true
	}
	for _, qt := range queryTokens {
		if !tokenSet[qt] {
			return false
		}
	}
	return true
}

// tokenizeText is a simple tokenizer for matcher-level text matching.
func tokenizeText(text string, stemmer *PorterStemmer) []string {
	text = strings.ToLower(text)
	var tokens []string
	var current []rune
	for _, r := range text {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			current = append(current, r)
		} else {
			if len(current) > 1 {
				word := string(current)
				if !stopWords[word] {
					tokens = append(tokens, stemmer.Stem(word))
				}
			}
			current = nil
		}
	}
	if len(current) > 1 {
		word := string(current)
		if !stopWords[word] {
			tokens = append(tokens, stemmer.Stem(word))
		}
	}
	return tokens
}

// matchNear handles $near: {$geometry: {type:"Point",coordinates:[lon,lat]}, $maxDistance: meters}.
func matchNear(opVal bson.Value, fieldVal bson.Value, fieldFound bool) bool {
	if !fieldFound || opVal.Type != bson.TypeDocument {
		return false
	}
	center := extractGeoCenter(opVal.DocumentValue())
	if center == nil {
		return false
	}
	maxDist := getGeoMaxDistance(opVal.DocumentValue())

	point := extractCoordinates(fieldVal)
	if point == nil {
		return false
	}
	dist := haversineDistance(center.Lon, center.Lat, point.Lon, point.Lat)
	return dist <= maxDist
}

// matchGeoWithin handles $geoWithin: {$geometry: {type:"Polygon", coordinates:[...]}}.
func matchGeoWithin(opVal bson.Value, fieldVal bson.Value, fieldFound bool) bool {
	if !fieldFound || opVal.Type != bson.TypeDocument {
		return false
	}
	opDoc := opVal.DocumentValue()
	geomVal, ok := opDoc.Get("$geometry")
	if !ok || geomVal.Type != bson.TypeDocument {
		return false
	}
	bbox := extractBoundingBox(geomVal.DocumentValue())
	if bbox == nil {
		return false
	}
	point := extractCoordinates(fieldVal)
	if point == nil {
		return false
	}
	return point.Lon >= bbox.MinLon && point.Lon <= bbox.MaxLon &&
		point.Lat >= bbox.MinLat && point.Lat <= bbox.MaxLat
}

// matchGeoIntersects checks if a point intersects a geometry (simplified: same as point-in-bbox).
func matchGeoIntersects(opVal bson.Value, fieldVal bson.Value, fieldFound bool) bool {
	return matchGeoWithin(opVal, fieldVal, fieldFound)
}

// geoBBox represents a bounding box.
type geoBBox struct {
	MinLon, MaxLon float64
	MinLat, MaxLat float64
}

// extractGeoCenter extracts center point from $near operator document.
func extractGeoCenter(doc *bson.Document) *GeoPoint2D {
	geomVal, ok := doc.Get("$geometry")
	if !ok || geomVal.Type != bson.TypeDocument {
		return nil
	}
	return extractCoordinates(geomVal)
}

// getGeoMaxDistance extracts $maxDistance in meters (default 1000).
func getGeoMaxDistance(doc *bson.Document) float64 {
	md, ok := doc.Get("$maxDistance")
	if !ok {
		return 1000.0
	}
	switch md.Type {
	case bson.TypeDouble:
		return md.Double()
	case bson.TypeInt32:
		return float64(md.Int32())
	case bson.TypeInt64:
		return float64(md.Int64())
	}
	return 1000.0
}

// extractBoundingBox extracts a bounding box from a Polygon GeoJSON geometry.
func extractBoundingBox(geomDoc *bson.Document) *geoBBox {
	coordsVal, ok := geomDoc.Get("coordinates")
	if !ok || coordsVal.Type != bson.TypeArray {
		return nil
	}
	// Polygon coordinates: [ [ [lon,lat], [lon,lat], ... ] ]
	// We extract all points and compute min/max
	ringArr := coordsVal.ArrayValue()
	if len(ringArr) == 0 {
		return nil
	}

	var minLon, maxLon, minLat, maxLat float64
	first := true
	for _, ring := range ringArr {
		if ring.Type != bson.TypeArray {
			continue
		}
		for _, pt := range ring.ArrayValue() {
			if pt.Type != bson.TypeArray {
				continue
			}
			arr := pt.ArrayValue()
			if len(arr) < 2 {
				continue
			}
			lon := arr[0].Double()
			lat := arr[1].Double()
			if first {
				minLon, maxLon = lon, lon
				minLat, maxLat = lat, lat
				first = false
			} else {
				if lon < minLon {
					minLon = lon
				}
				if lon > maxLon {
					maxLon = lon
				}
				if lat < minLat {
					minLat = lat
				}
				if lat > maxLat {
					maxLat = lat
				}
			}
		}
	}
	if first {
		return nil
	}
	return &geoBBox{MinLon: minLon, MaxLon: maxLon, MinLat: minLat, MaxLat: maxLat}
}

