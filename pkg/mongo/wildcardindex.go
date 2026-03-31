package mongo

import (
	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

const wildcardIndexPrefix = "\x00wc"

// WildcardIndex indexes all fields in every document automatically.
// Supports the "$**" field pattern.
type WildcardIndex struct {
	spec *IndexSpec
	db   string
	coll string
	eng  *engine.Engine
}

// NewWildcardIndex creates a wildcard index handle.
func NewWildcardIndex(db, coll string, spec *IndexSpec, eng *engine.Engine) *WildcardIndex {
	return &WildcardIndex{spec: spec, db: db, coll: coll, eng: eng}
}

// wcIndexPrefix returns the prefix for all keys in this wildcard index.
func (wi *WildcardIndex) wcIndexPrefix() []byte {
	ns := EncodeNamespacePrefix(wi.db, wi.coll)
	prefix := make([]byte, 0, len(ns)+len(wildcardIndexPrefix)+len(wi.spec.Name))
	prefix = append(prefix, ns...)
	prefix = append(prefix, wildcardIndexPrefix...)
	prefix = append(prefix, wi.spec.Name...)
	return prefix
}

// buildWildcardKey builds the engine key for a wildcard index entry.
// Format: {ns_prefix}\x00wc{index_name}{field_name}\x00{encoded_value}{_id_bytes}
func (wi *WildcardIndex) buildWildcardKey(fieldName string, val bson.Value, id []byte) []byte {
	prefix := wi.wcIndexPrefix()
	encoded := encodeIndexValue(val)

	buf := make([]byte, 0, len(prefix)+len(fieldName)+1+len(encoded)+len(id))
	buf = append(buf, prefix...)
	buf = append(buf, fieldName...)
	buf = append(buf, 0x00) // separator
	buf = append(buf, encoded...)
	buf = append(buf, id...)
	return buf
}

// AddEntry adds wildcard index entries for all fields in a document.
func (wi *WildcardIndex) AddEntry(doc *bson.Document) error {
	idVal, ok := doc.Get("_id")
	if !ok {
		return nil
	}
	idBytes := idVal.ObjectID().Bytes()

	if wi.spec.PartialFilterExpression != nil {
		m := NewMatcher(wi.spec.PartialFilterExpression)
		if !m.Match(doc) {
			return nil
		}
	}

	return wi.indexDocFields(doc, "", idBytes)
}

// indexDocFields recursively indexes all fields in a document.
func (wi *WildcardIndex) indexDocFields(doc *bson.Document, parentPrefix string, idBytes []byte) error {
	for _, e := range doc.Elements() {
		fieldName := e.Key
		if parentPrefix != "" {
			fieldName = parentPrefix + "." + fieldName
		}

		v := e.Value

		// Skip internal fields
		if fieldName == "_id" {
			continue
		}

		switch v.Type {
		case bson.TypeDocument:
			// Index the document value itself
			key := wi.buildWildcardKey(fieldName, v, idBytes)
			if err := wi.eng.Put(key, []byte{1}); err != nil {
				return err
			}
			// Recurse into sub-documents
			if err := wi.indexDocFields(v.DocumentValue(), fieldName, idBytes); err != nil {
				return err
			}
		case bson.TypeArray:
			// Index the array as a whole
			key := wi.buildWildcardKey(fieldName, v, idBytes)
			if err := wi.eng.Put(key, []byte{1}); err != nil {
				return err
			}
			// Index each array element
			arr := v.ArrayValue()
			for i, elem := range arr {
				arrField := fieldName + "." + itoa(i)
				if elem.Type == bson.TypeDocument {
					subKey := wi.buildWildcardKey(arrField, elem, idBytes)
					if err := wi.eng.Put(subKey, []byte{1}); err != nil {
						return err
					}
					if err := wi.indexDocFields(elem.DocumentValue(), arrField, idBytes); err != nil {
						return err
					}
				} else {
					subKey := wi.buildWildcardKey(arrField, elem, idBytes)
					if err := wi.eng.Put(subKey, []byte{1}); err != nil {
						return err
					}
				}
			}
		default:
			key := wi.buildWildcardKey(fieldName, v, idBytes)
			if err := wi.eng.Put(key, []byte{1}); err != nil {
				return err
			}
		}
	}
	return nil
}

// RemoveEntry removes all wildcard index entries for a document.
func (wi *WildcardIndex) RemoveEntry(doc *bson.Document) error {
	idVal, ok := doc.Get("_id")
	if !ok {
		return nil
	}
	idBytes := idVal.ObjectID().Bytes()

	// Collect keys to delete during scan (can't delete while holding read lock)
	prefix := wi.wcIndexPrefix()
	var keysToDelete [][]byte
	wi.eng.Scan(prefix, func(key, _ []byte) bool {
		if len(key) >= len(idBytes) {
			suffix := key[len(key)-len(idBytes):]
			if equalBytes(suffix, idBytes) {
				keysToDelete = append(keysToDelete, append([]byte{}, key...))
			}
		}
		return true
	})
	for _, key := range keysToDelete {
		_ = wi.eng.Delete(key)
	}
	return nil
}

// LookupField finds document IDs where the given field has the given value.
func (wi *WildcardIndex) LookupField(field string, val bson.Value) [][]byte {
	prefix := wi.wcIndexPrefix()
	encoded := encodeIndexValue(val)
	scanKey := make([]byte, 0, len(prefix)+len(field)+1+len(encoded))
	scanKey = append(scanKey, prefix...)
	scanKey = append(scanKey, field...)
	scanKey = append(scanKey, 0x00)
	scanKey = append(scanKey, encoded...)

	var ids [][]byte
	wi.eng.Scan(scanKey, func(key, _ []byte) bool {
		if len(key) > len(scanKey) {
			idBytes := key[len(scanKey):]
			ids = append(ids, append([]byte{}, idBytes...))
		}
		return true
	})
	return ids
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := make([]byte, 0, 12)
	for n > 0 {
		digits = append(digits, byte('0'+n%10))
		n /= 10
	}
	// Reverse
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	return string(digits)
}
