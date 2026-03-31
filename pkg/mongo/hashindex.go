package mongo

import (
	"encoding/binary"
	"hash/fnv"
	"math"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

const hashIndexPrefix = "\x00hsh"

// HashIndex implements a hash-based index for equality lookups.
// Field value → FNV-1a hash → 8-byte key.
// Faster equality lookup than B-tree but does not support range queries.
type HashIndex struct {
	spec *IndexSpec
	db   string
	coll string
	eng  *engine.Engine
}

// NewHashIndex creates a hash index handle.
func NewHashIndex(db, coll string, spec *IndexSpec, eng *engine.Engine) *HashIndex {
	return &HashIndex{spec: spec, db: db, coll: coll, eng: eng}
}

// hashIndexPrefix returns the prefix for all keys in this hash index.
func (hi *HashIndex) hashIndexPrefix() []byte {
	ns := EncodeNamespacePrefix(hi.db, hi.coll)
	prefix := make([]byte, 0, len(ns)+len(hashIndexPrefix)+len(hi.spec.Name))
	prefix = append(prefix, ns...)
	prefix = append(prefix, hashIndexPrefix...)
	prefix = append(prefix, hi.spec.Name...)
	return prefix
}

// buildHashKey builds the engine key for a hash index entry.
// Format: {ns_prefix}\x00hsh{index_name}{hash_bytes(8)}{_id_bytes}
func (hi *HashIndex) buildHashKey(val bson.Value, id []byte) []byte {
	prefix := hi.hashIndexPrefix()
	hash := fnvHashValue(val)
	buf := make([]byte, 0, len(prefix)+8+len(id))
	buf = append(buf, prefix...)
	buf = append(buf, hash...)
	buf = append(buf, id...)
	return buf
}

// AddEntry adds a hash index entry for a document.
func (hi *HashIndex) AddEntry(doc *bson.Document) error {
	idVal, ok := doc.Get("_id")
	if !ok {
		return nil
	}
	idBytes := idVal.ObjectID().Bytes()

	if hi.spec.PartialFilterExpression != nil {
		m := NewMatcher(hi.spec.PartialFilterExpression)
		if !m.Match(doc) {
			return nil
		}
	}

	for _, ik := range hi.spec.Key {
		v, found := ResolveField(doc, ik.Field)
		if !found && hi.spec.Sparse {
			return nil
		}
		if !found {
			v = bson.VNull()
		}
		key := hi.buildHashKey(v, idBytes)
		if err := hi.eng.Put(key, []byte{1}); err != nil {
			return err
		}
	}
	return nil
}

// RemoveEntry removes a hash index entry for a document.
func (hi *HashIndex) RemoveEntry(doc *bson.Document) error {
	idVal, ok := doc.Get("_id")
	if !ok {
		return nil
	}
	idBytes := idVal.ObjectID().Bytes()

	for _, ik := range hi.spec.Key {
		v, found := ResolveField(doc, ik.Field)
		if !found && hi.spec.Sparse {
			return nil
		}
		if !found {
			v = bson.VNull()
		}
		key := hi.buildHashKey(v, idBytes)
		_ = hi.eng.Delete(key)
	}
	return nil
}

// LookupEqual finds document IDs where the indexed field equals the given value.
func (hi *HashIndex) LookupEqual(val bson.Value) [][]byte {
	hash := fnvHashValue(val)
	prefix := hi.hashIndexPrefix()
	scanKey := make([]byte, 0, len(prefix)+8)
	scanKey = append(scanKey, prefix...)
	scanKey = append(scanKey, hash...)

	var ids [][]byte
	hi.eng.Scan(scanKey, func(key, _ []byte) bool {
		// Key format: prefix + hash(8) + _id
		if len(key) > len(scanKey) {
			idBytes := key[len(scanKey):]
			ids = append(ids, append([]byte{}, idBytes...))
		}
		return true
	})
	return ids
}

// fnvHashValue computes an 8-byte FNV-1a hash of a BSON value.
func fnvHashValue(v bson.Value) []byte {
	h := fnv.New64a()
	// Write type byte
	h.Write([]byte{byte(v.Type)})
	// Write value bytes
	switch v.Type {
	case bson.TypeNull:
		// nothing
	case bson.TypeBoolean:
		if v.Boolean() {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	case bson.TypeInt32:
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(v.Int32()))
		h.Write(buf[:])
	case bson.TypeInt64:
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(v.Int64()))
		h.Write(buf[:])
	case bson.TypeDouble:
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v.Double()))
		h.Write(buf[:])
	case bson.TypeString:
		h.Write([]byte(v.String()))
	case bson.TypeObjectID:
		oid := v.ObjectID()
		h.Write(oid[:])
	default:
		// Fallback: hash the encoded value
		h.Write(encodeIndexValue(v))
	}
	var result [8]byte
	binary.BigEndian.PutUint64(result[:], h.Sum64())
	return result[:]
}
