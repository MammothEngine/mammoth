package mongo

import (
	"github.com/mammothengine/mammoth/pkg/bson"
)

// ApplyProjection filters document fields based on a projection spec.
//
// Include mode: only listed fields are returned (+ _id by default).
// Exclude mode: listed fields are removed from the result.
//
// The mode is determined by the first non-_id field value:
//   - 1 or true  = include mode
//   - 0 or false = exclude mode
func ApplyProjection(doc *bson.Document, proj *bson.Document) *bson.Document {
	if proj == nil || proj.Len() == 0 {
		return doc
	}

	// Determine mode from first non-_id field
	mode := projectionMode(proj)

	result := bson.NewDocument()

	if mode == includeMode {
		// Include mode: copy only specified fields, _id included by default
		// Always include _id unless explicitly excluded
		idIncluded := true
		if v, ok := proj.Get("_id"); ok {
			if isZeroOrFalse(v) {
				idIncluded = false
			}
		}
		if idIncluded {
			if v, ok := doc.Get("_id"); ok {
				result.Set("_id", v)
			}
		}
		// Copy specified fields
		for _, e := range proj.Elements() {
			if e.Key == "_id" {
				continue
			}
			if !isZeroOrFalse(e.Value) {
				if v, ok := doc.Get(e.Key); ok {
					result.Set(e.Key, v)
				}
			}
		}
	} else {
		// Exclude mode: copy everything except excluded fields
		excluded := make(map[string]bool)
		for _, e := range proj.Elements() {
			if isZeroOrFalse(e.Value) {
				excluded[e.Key] = true
			}
		}
		for _, e := range doc.Elements() {
			if !excluded[e.Key] {
				result.Set(e.Key, e.Value)
			}
		}
	}

	return result
}

type projMode int

const (
	includeMode projMode = iota
	excludeMode
)

func projectionMode(proj *bson.Document) projMode {
	for _, e := range proj.Elements() {
		if e.Key == "_id" {
			continue
		}
		if isZeroOrFalse(e.Value) {
			return excludeMode
		}
		return includeMode
	}
	return includeMode // default: include all
}

func isZeroOrFalse(v bson.Value) bool {
	switch v.Type {
	case bson.TypeInt32:
		return v.Int32() == 0
	case bson.TypeInt64:
		return v.Int64() == 0
	case bson.TypeDouble:
		return v.Double() == 0
	case bson.TypeBoolean:
		return !v.Boolean()
	}
	return false
}
