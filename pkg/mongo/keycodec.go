package mongo

import "encoding/binary"

// Key encoding scheme for the KV engine:
//
// Document key:  {4-byte-len-db}{db}{4-byte-len-coll}{coll}{_id_bytes}
// Index key:     {ns_prefix}\x00idx{index_name}{type_tag}{encoded_value}{_id_bytes}
// Catalog key:   \x00cat{type_byte}{name}
//
// The \x00 prefix for catalog keys ensures they sort before all document keys.

const (
	catalogPrefix    = "\x00cat"
	catalogTypeDB    = byte(0x01)
	catalogTypeColl  = byte(0x02)
	catalogTypeIndex = byte(0x03)
	indexSeparator   = "\x00idx"
)

// EncodeNamespacePrefix returns the prefix for all document keys of a collection:
//
//	{4-byte-len-db}{db}{4-byte-len-coll}{coll}
func EncodeNamespacePrefix(db, coll string) []byte {
	buf := make([]byte, 0, 4+len(db)+4+len(coll))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(db)))
	buf = append(buf, db...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(coll)))
	buf = append(buf, coll...)
	return buf
}

// encodeDocKey returns the full engine key for a document.
func encodeDocKey(db, coll string, id []byte) []byte {
	prefix := EncodeNamespacePrefix(db, coll)
	key := make([]byte, 0, len(prefix)+len(id))
	key = append(key, prefix...)
	key = append(key, id...)
	return key
}

// decodeNamespaceFromKey extracts db and collection name from a document key.
func decodeNamespaceFromKey(key []byte) (db, coll string, ok bool) {
	pos := 0
	if len(key) < 4 {
		return "", "", false
	}
	dbLen := int(binary.BigEndian.Uint32(key[pos:]))
	pos += 4
	if len(key) < pos+dbLen {
		return "", "", false
	}
	db = string(key[pos : pos+dbLen])
	pos += dbLen
	if len(key) < pos+4 {
		return "", "", false
	}
	collLen := int(binary.BigEndian.Uint32(key[pos:]))
	pos += 4
	if len(key) < pos+collLen {
		return "", "", false
	}
	coll = string(key[pos : pos+collLen])
	return db, coll, true
}

// encodeIndexKey returns an engine key for an index entry.
// Format: {ns_prefix}\x00idx{index_name}{type_tag}{encoded_value}{_id_bytes}
func encodeIndexKey(db, coll, indexName string, typeTag byte, encodedValue, id []byte) []byte {
	ns := EncodeNamespacePrefix(db, coll)
	key := make([]byte, 0, len(ns)+len(indexSeparator)+len(indexName)+1+len(encodedValue)+len(id))
	key = append(key, ns...)
	key = append(key, indexSeparator...)
	key = append(key, indexName...)
	key = append(key, typeTag)
	key = append(key, encodedValue...)
	key = append(key, id...)
	return key
}

// encodeCatalogKeyDB returns a catalog key for a database entry.
func encodeCatalogKeyDB(name string) []byte {
	key := make([]byte, 0, len(catalogPrefix)+1+len(name))
	key = append(key, catalogPrefix...)
	key = append(key, catalogTypeDB)
	key = append(key, name...)
	return key
}

// encodeCatalogKeyColl returns a catalog key for a collection entry.
func encodeCatalogKeyColl(db, coll string) []byte {
	name := db + "." + coll
	key := make([]byte, 0, len(catalogPrefix)+1+len(name))
	key = append(key, catalogPrefix...)
	key = append(key, catalogTypeColl)
	key = append(key, name...)
	return key
}

// encodeCatalogKeyIndex returns a catalog key for an index entry.
func encodeCatalogKeyIndex(db, coll, indexName string) []byte {
	name := db + "." + coll + "." + indexName
	key := make([]byte, 0, len(catalogPrefix)+1+len(name))
	key = append(key, catalogPrefix...)
	key = append(key, catalogTypeIndex)
	key = append(key, name...)
	return key
}

// isCatalogKey returns true if the key is a catalog entry.
func isCatalogKey(key []byte) bool {
	return len(key) > len(catalogPrefix) && string(key[:len(catalogPrefix)]) == catalogPrefix
}
