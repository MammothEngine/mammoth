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
			fieldVal, found := resolveField(doc, e.Key)
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
	}
	return true
}

// resolveField resolves a dot-notation path like "a.b.c" in a document.
func resolveField(doc *bson.Document, path string) (bson.Value, bool) {
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
		return resolveField(v.DocumentValue(), parts[1])
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
