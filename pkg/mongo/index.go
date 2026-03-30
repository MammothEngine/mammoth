package mongo

import (
	"encoding/binary"
	"math"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// IndexSpec describes a secondary index.
type IndexSpec struct {
	Name                    string         `json:"name"`
	Key                     []IndexKey     `json:"key"`
	Unique                  bool           `json:"unique"`
	Sparse                  bool           `json:"sparse,omitempty"`
	ExpireAfterSeconds      int32          `json:"expireAfterSeconds,omitempty"`
	PartialFilterExpression *bson.Document `json:"partialFilterExpression,omitempty"`
}

// IndexKey describes one component of a compound index key.
type IndexKey struct {
	Field      string `json:"field"`
	Descending bool   `json:"descending"`
}

// Type-preserving encoding tags for index keys.
const (
	typeTagNull    = 0x00
	typeTagFalse   = 0x01
	typeTagTrue    = 0x02
	typeTagNumber  = 0x03
	typeTagString  = 0x04
	typeTagObject  = 0x05
	typeTagArray   = 0x06
	typeTagBinData = 0x07
	typeTagOID     = 0x08
	typeTagBool    = 0x09
	typeTagDate    = 0x0A
	typeTagRegex   = 0x0B
	typeTagMinKey  = 0x10
	typeTagMaxKey  = 0x1F
)

// encodeIndexValue converts a BSON value to a type-tagged, order-preserving byte slice.
func encodeIndexValue(v bson.Value) []byte {
	switch v.Type {
	case bson.TypeNull:
		return []byte{typeTagNull}
	case bson.TypeBoolean:
		if v.Boolean() {
			return []byte{typeTagTrue}
		}
		return []byte{typeTagFalse}
	case bson.TypeInt32:
		return encodeIndexNumber(float64(v.Int32()))
	case bson.TypeInt64:
		return encodeIndexNumber(float64(v.Int64()))
	case bson.TypeDouble:
		return encodeIndexNumber(v.Double())
	case bson.TypeString:
		s := v.String()
		buf := make([]byte, 1+len(s))
		buf[0] = typeTagString
		copy(buf[1:], s)
		return buf
	case bson.TypeObjectID:
		oid := v.ObjectID()
		buf := make([]byte, 1+12)
		buf[0] = typeTagOID
		copy(buf[1:], oid[:])
		return buf
	case bson.TypeDateTime:
		buf := make([]byte, 9)
		buf[0] = typeTagDate
		binary.BigEndian.PutUint64(buf[1:], uint64(v.DateTime()))
		return buf
	case bson.TypeMinKey:
		return []byte{typeTagMinKey}
	case bson.TypeMaxKey:
		return []byte{typeTagMaxKey}
	default:
		return []byte{typeTagNull}
	}
}

// encodeIndexNumber encodes a float64 in a way that preserves ordering.
// NaN maps to -Infinity.
func encodeIndexNumber(f float64) []byte {
	buf := make([]byte, 9)
	buf[0] = typeTagNumber
	if math.IsNaN(f) {
		f = math.Inf(-1)
	}
	bits := math.Float64bits(f)
	// Flip sign bit for proper ordering (negatives < positives)
	if bits&(1<<63) != 0 {
		bits = ^bits // flip all bits for negative numbers
	} else {
		bits ^= (1 << 63) // flip only sign bit for positive numbers
	}
	binary.BigEndian.PutUint64(buf[1:], bits)
	return buf
}

// flipForDescending reverses all bytes for descending order.
func flipForDescending(b []byte) {
	for i := range b {
		b[i] = ^b[i]
	}
}

// buildIndexKey builds the engine key for an index entry.
// Format: {ns_prefix}\x00idx{index_name}{encoded_values}{_id_bytes}
func buildIndexKey(db, coll string, spec *IndexSpec, doc *bson.Document, id []byte) []byte {
	ns := EncodeNamespacePrefix(db, coll)
	encodedVals := make([][]byte, 0, len(spec.Key))
	for _, ik := range spec.Key {
		v, found := ResolveField(doc, ik.Field)
		var encoded []byte
		if found {
			encoded = encodeIndexValue(v)
		} else {
			encoded = []byte{typeTagNull}
		}
		if ik.Descending {
			flipped := make([]byte, len(encoded))
			copy(flipped, encoded)
			flipForDescending(flipped)
			encoded = flipped
		}
		encodedVals = append(encodedVals, encoded)
	}

	// Build the full key
	var totalLen int
	for _, ev := range encodedVals {
		totalLen += len(ev)
	}
	totalLen += len(id)

	buf := make([]byte, 0, len(ns)+len(indexSeparator)+len(spec.Name)+totalLen)
	buf = append(buf, ns...)
	buf = append(buf, indexSeparator...)
	buf = append(buf, spec.Name...)
	for _, ev := range encodedVals {
		buf = append(buf, ev...)
	}
	buf = append(buf, id...)
	return buf
}

// buildIndexKeyWithValue builds an index key for a single scalar value (used for multikey).
func buildIndexKeyWithValue(db, coll string, spec *IndexSpec, val bson.Value, id []byte) []byte {
	ns := EncodeNamespacePrefix(db, coll)
	encoded := encodeIndexValue(val)
	if spec.Key[0].Descending {
		flipped := make([]byte, len(encoded))
		copy(flipped, encoded)
		flipForDescending(flipped)
		encoded = flipped
	}

	buf := make([]byte, 0, len(ns)+len(indexSeparator)+len(spec.Name)+len(encoded)+len(id))
	buf = append(buf, ns...)
	buf = append(buf, indexSeparator...)
	buf = append(buf, spec.Name...)
	buf = append(buf, encoded...)
	buf = append(buf, id...)
	return buf
}

// Index manages a single secondary index.
type Index struct {
	spec *IndexSpec
	db   string
	coll string
	eng  *engine.Engine
}

// NewIndex creates an index handle.
func NewIndex(db, coll string, spec *IndexSpec, eng *engine.Engine) *Index {
	return &Index{spec: spec, db: db, coll: coll, eng: eng}
}

// Spec returns the index specification.
func (idx *Index) Spec() *IndexSpec { return idx.spec }

// AddEntry adds an index entry for a document.
func (idx *Index) AddEntry(doc *bson.Document) error {
	idVal, ok := doc.Get("_id")
	if !ok {
		return nil
	}
	idBytes := idVal.ObjectID().Bytes()

	// Partial index: skip if document doesn't match filter
	if idx.spec.PartialFilterExpression != nil {
		m := NewMatcher(idx.spec.PartialFilterExpression)
		if !m.Match(doc) {
			return nil
		}
	}

	for _, ik := range idx.spec.Key {
		v, found := ResolveField(doc, ik.Field)

		// Sparse index: skip documents missing the indexed field
		if !found && idx.spec.Sparse {
			return nil
		}

		if !found {
			v = bson.VNull()
		}

		// Multikey: if value is an array, create one entry per element
		if v.Type == bson.TypeArray && len(idx.spec.Key) == 1 {
			arr := v.ArrayValue()
			if len(arr) == 0 {
				// Empty array: store null entry
				key := buildIndexKeyWithValue(idx.db, idx.coll, idx.spec, bson.VNull(), idBytes)
				if err := idx.putEntry(key); err != nil {
					return err
				}
			} else {
				for _, elem := range arr {
					key := buildIndexKeyWithValue(idx.db, idx.coll, idx.spec, elem, idBytes)
					if err := idx.putEntry(key); err != nil {
						return err
					}
				}
			}
			return nil
		}
	}

	// Non-multikey path (single value or compound key)
	key := buildIndexKey(idx.db, idx.coll, idx.spec, doc, idBytes)
	return idx.putEntry(key)
}

// putEntry writes an index entry, checking unique constraints.
func (idx *Index) putEntry(key []byte) error {
	if idx.spec.Unique {
		prefix := idx.uniquePrefixFromKey(key)
		found := false
		idx.eng.Scan(prefix, func(_, _ []byte) bool {
			found = true
			return false
		})
		if found {
			return ErrDuplicateKey
		}
	}
	return idx.eng.Put(key, []byte{1})
}

// uniquePrefixFromKey extracts the unique prefix (without _id suffix) from a full index key.
func (idx *Index) uniquePrefixFromKey(fullKey []byte) []byte {
	// The _id suffix is always 12 bytes (ObjectID). For unique check, we scan the prefix
	// without those last 12 bytes.
	if len(fullKey) > 12 {
		return fullKey[:len(fullKey)-12]
	}
	return fullKey
}

// RemoveEntry removes an index entry for a document.
// For multikey indexes, removes all entries created for the document's array elements.
func (idx *Index) RemoveEntry(doc *bson.Document) error {
	idVal, ok := doc.Get("_id")
	if !ok {
		return nil
	}
	idBytes := idVal.ObjectID().Bytes()

	// For multikey, scan the index prefix with _id suffix to find all entries
	for _, ik := range idx.spec.Key {
		v, found := ResolveField(doc, ik.Field)
		if !found && idx.spec.Sparse {
			return nil
		}

		if found && v.Type == bson.TypeArray && len(idx.spec.Key) == 1 {
			arr := v.ArrayValue()
			if len(arr) == 0 {
				key := buildIndexKeyWithValue(idx.db, idx.coll, idx.spec, bson.VNull(), idBytes)
				_ = idx.eng.Delete(key)
			} else {
				for _, elem := range arr {
					key := buildIndexKeyWithValue(idx.db, idx.coll, idx.spec, elem, idBytes)
					_ = idx.eng.Delete(key)
				}
			}
			return nil
		}
	}

	key := buildIndexKey(idx.db, idx.coll, idx.spec, doc, idBytes)
	return idx.eng.Delete(key)
}

// ScanPrefix returns the key prefix for this index (all entries).
func (idx *Index) ScanPrefix() []byte {
	ns := EncodeNamespacePrefix(idx.db, idx.coll)
	prefix := make([]byte, 0, len(ns)+len(indexSeparator)+len(idx.spec.Name))
	prefix = append(prefix, ns...)
	prefix = append(prefix, indexSeparator...)
	prefix = append(prefix, idx.spec.Name...)
	return prefix
}

// LookupByPrefix scans the engine for index entries matching a prefix key
// and returns the document _id values found.
func LookupByPrefix(eng *engine.Engine, prefixKey []byte) [][]byte {
	var ids [][]byte
	_ = eng.Scan(prefixKey, func(key, _ []byte) bool {
		if len(key) > len(prefixKey) {
			idBytes := key[len(prefixKey):]
			ids = append(ids, append([]byte{}, idBytes...))
		}
		return true
	})
	return ids
}
