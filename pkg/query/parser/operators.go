package parser

import (
	"regexp"
	"strings"
	"sync"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// regexCache caches compiled regex patterns for performance.
var regexCache = struct {
	sync.RWMutex
	patterns map[string]*regexp.Regexp
}{
	patterns: make(map[string]*regexp.Regexp),
}

// getCachedRegex gets or compiles a regex pattern.
func getCachedRegex(pattern, options string) *regexp.Regexp {
	// Create cache key from pattern + options
	cacheKey := pattern + "\x00" + options

	// Try read lock first
	regexCache.RLock()
	re, ok := regexCache.patterns[cacheKey]
	regexCache.RUnlock()
	if ok {
		return re
	}

	// Compile new regex
	flags := ""
	if strings.Contains(options, "i") {
		flags = "(?i)"
	}
	if strings.Contains(options, "m") {
		flags += "(?m)"
	}
	if strings.Contains(options, "s") {
		flags += "(?s)"
	}

	re, err := regexp.Compile(flags + pattern)
	if err != nil {
		return nil
	}

	// Store in cache
	regexCache.Lock()
	regexCache.patterns[cacheKey] = re
	regexCache.Unlock()

	return re
}

// resolveField resolves a dot-notation field path in a document.
// Supports nested fields like "address.city" and array indices like "tags.0".
func resolveField(doc *bson.Document, path string) (bson.Value, bool) {
	parts := strings.Split(path, ".")
	current := doc

	for i, part := range parts {
		v, ok := current.Get(part)
		if !ok {
			return bson.Value{}, false
		}

		// Last part - return the value
		if i == len(parts)-1 {
			return v, true
		}

		// Need to traverse deeper - value must be a document or array
		switch v.Type {
		case bson.TypeDocument:
			current = v.DocumentValue()
		case bson.TypeArray:
			// Check if next part is an array index
			if i+1 < len(parts) {
				if idx, err := parseArrayIndex(parts[i+1]); err == nil {
					arr := v.ArrayValue()
					if idx >= 0 && idx < len(arr) {
						return arr[idx], true
					}
					return bson.Value{}, false
				}
			}
			// Not an index, can't traverse further
			return bson.Value{}, false
		default:
			// Can't traverse into scalar values
			return bson.Value{}, false
		}
	}

	return bson.Value{}, false
}

// parseArrayIndex parses an array index from a string.
func parseArrayIndex(s string) (int, error) {
	idx := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return -1, nil // Not a number
		}
		idx = idx*10 + int(c-'0')
	}
	return idx, nil
}

// compareValues compares two BSON values.
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
// Follows MongoDB comparison order.
func compareValues(a, b bson.Value) int {
	// Handle null comparisons
	if a.Type == bson.TypeNull && b.Type == bson.TypeNull {
		return 0
	}
	if a.Type == bson.TypeNull {
		return -1 // Null is less than everything
	}
	if b.Type == bson.TypeNull {
		return 1
	}

	// Compare by type first (MongoDB type ordering)
	typeOrderA := bsonTypeOrder(a.Type)
	typeOrderB := bsonTypeOrder(b.Type)
	if typeOrderA != typeOrderB {
		if typeOrderA < typeOrderB {
			return -1
		}
		return 1
	}

	// Same type - compare values
	switch a.Type {
	case bson.TypeBoolean:
		av, bv := a.Boolean(), b.Boolean()
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1 // false < true
		}
		return 1

	case bson.TypeInt32:
		av := float64(a.Int32())
		var bv float64
		switch b.Type {
		case bson.TypeInt32:
			bv = float64(b.Int32())
		case bson.TypeInt64:
			bv = float64(b.Int64())
		case bson.TypeDouble:
			bv = b.Double()
		}
		return compareFloats(av, bv)

	case bson.TypeInt64:
		av := float64(a.Int64())
		var bv float64
		switch b.Type {
		case bson.TypeInt32:
			bv = float64(b.Int32())
		case bson.TypeInt64:
			bv = float64(b.Int64())
		case bson.TypeDouble:
			bv = b.Double()
		}
		return compareFloats(av, bv)

	case bson.TypeDouble:
		av := a.Double()
		var bv float64
		switch b.Type {
		case bson.TypeInt32:
			bv = float64(b.Int32())
		case bson.TypeInt64:
			bv = float64(b.Int64())
		case bson.TypeDouble:
			bv = b.Double()
		}
		return compareFloats(av, bv)

	case bson.TypeString:
		av, bv := a.String(), b.String()
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0

	case bson.TypeObjectID:
		av, bv := a.ObjectID().Bytes(), b.ObjectID().Bytes()
		return compareBytes(av, bv)

	case bson.TypeDateTime:
		av, bv := a.DateTime(), b.DateTime()
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0

	case bson.TypeTimestamp:
		av, bv := a.Timestamp(), b.Timestamp()
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0

	case bson.TypeBinary:
		av, bv := a.Binary().Data, b.Binary().Data
		return compareBytes(av, bv)

	case bson.TypeRegex:
		av, bv := a.Regex().Pattern, b.Regex().Pattern
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0

	case bson.TypeDocument:
		// Document comparison is complex - compare by key count first
		ad, bd := a.DocumentValue(), b.DocumentValue()
		lenA, lenB := ad.Len(), bd.Len()
		if lenA != lenB {
			if lenA < lenB {
				return -1
			}
			return 1
		}
		// Compare each field recursively
		keysA, keysB := ad.Keys(), bd.Keys()
		for i, keyA := range keysA {
			if keyA != keysB[i] {
				if keyA < keysB[i] {
					return -1
				}
				return 1
			}
			valA, _ := ad.Get(keyA)
			valB, _ := bd.Get(keysB[i])
			cmp := compareValues(valA, valB)
			if cmp != 0 {
				return cmp
			}
		}
		return 0

	case bson.TypeArray:
		// Array comparison - compare element by element
		arrA, arrB := a.ArrayValue(), b.ArrayValue()
		minLen := len(arrA)
		if len(arrB) < minLen {
			minLen = len(arrB)
		}
		for i := 0; i < minLen; i++ {
			cmp := compareValues(arrA[i], arrB[i])
			if cmp != 0 {
				return cmp
			}
		}
		if len(arrA) < len(arrB) {
			return -1
		}
		if len(arrA) > len(arrB) {
			return 1
		}
		return 0

	default:
		// For other types, compare raw bytes
		return 0
	}
}

// compareFloats compares two float64 values with NaN handling.
func compareFloats(a, b float64) int {
	// Handle NaN
	if isNaN(a) && isNaN(b) {
		return 0
	}
	if isNaN(a) {
		return -1 // NaN is less than everything (MongoDB behavior)
	}
	if isNaN(b) {
		return 1
	}

	// Normal comparison
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// isNaN checks if a float is NaN.
func isNaN(f float64) bool {
	return f != f
}

// compareBytes compares two byte slices.
func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// bsonTypeOrder returns the comparison order for BSON types.
// Follows MongoDB's type ordering for comparisons.
// https://docs.mongodb.com/manual/reference/bson-type-comparison-order/
func bsonTypeOrder(t bson.BSONType) int {
	switch t {
	case bson.TypeMinKey:
		return 1
	case bson.TypeNull:
		return 2
	case bson.TypeDouble, bson.TypeInt32, bson.TypeInt64:
		return 3 // Numbers are compared as doubles
	case bson.TypeString:
		return 4
	case bson.TypeDocument:
		return 5
	case bson.TypeArray:
		return 6
	case bson.TypeBinary:
		return 7
	case bson.TypeObjectID:
		return 8
	case bson.TypeBoolean:
		return 9
	case bson.TypeDateTime:
		return 10
	case bson.TypeTimestamp:
		return 11
	case bson.TypeRegex:
		return 12
	case bson.TypeMaxKey:
		return 13
	default:
		return 100 // Unknown types
	}
}

// bsonTypeName returns the BSON type name as a string.
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
	case bson.TypeTimestamp:
		return "timestamp"
	case bson.TypeInt64:
		return "long"
	case bson.TypeMinKey:
		return "minKey"
	case bson.TypeMaxKey:
		return "maxKey"
	default:
		return "unknown"
	}
}

// bsonTypeAlias returns the alias type name for BSON types.
// Used for $type operator which accepts aliases like "number" for all numeric types.
func bsonTypeAlias(t bson.BSONType) string {
	switch t {
	case bson.TypeDouble, bson.TypeInt32, bson.TypeInt64:
		return "number"
	default:
		return ""
	}
}
