package mongo

import (
	"bytes"
	"testing"
)

func TestEncodeNamespacePrefix(t *testing.T) {
	prefix := EncodeNamespacePrefix("testdb", "testcoll")
	if len(prefix) == 0 {
		t.Error("expected non-empty prefix")
	}

	// Should be deterministic
	prefix2 := EncodeNamespacePrefix("testdb", "testcoll")
	if !bytes.Equal(prefix, prefix2) {
		t.Error("expected deterministic prefix")
	}

	// Different db/coll should produce different prefixes
	prefix3 := EncodeNamespacePrefix("otherdb", "testcoll")
	if bytes.Equal(prefix, prefix3) {
		t.Error("expected different prefix for different db")
	}
}

func TestEncodeDocumentKey(t *testing.T) {
	id := []byte("doc123")
	key := EncodeDocumentKey("mydb", "mycoll", id)

	if len(key) == 0 {
		t.Error("expected non-empty key")
	}

	// Key should contain the id
	if !bytes.HasSuffix(key, id) {
		t.Error("expected key to end with id")
	}
}

func TestDecodeNamespaceFromKey(t *testing.T) {
	// Create a key and decode it
	id := []byte("doc123")
	key := EncodeDocumentKey("mydb", "mycoll", id)

	db, coll, ok := decodeNamespaceFromKey(key)
	if !ok {
		t.Fatal("expected successful decode")
	}
	if db != "mydb" {
		t.Errorf("expected db='mydb', got '%s'", db)
	}
	if coll != "mycoll" {
		t.Errorf("expected coll='mycoll', got '%s'", coll)
	}

	// Test with invalid key (too short)
	_, _, ok = decodeNamespaceFromKey([]byte{1, 2, 3})
	if ok {
		t.Error("expected decode to fail for short key")
	}
}

func TestIsCatalogKey(t *testing.T) {
	// Catalog key
	catKey := encodeCatalogKeyDB("testdb")
	if !isCatalogKey(catKey) {
		t.Error("expected catalog key to be recognized")
	}

	// Document key (not catalog)
	docKey := EncodeDocumentKey("testdb", "testcoll", []byte("id"))
	if isCatalogKey(docKey) {
		t.Error("expected document key not to be catalog key")
	}

	// Empty key
	if isCatalogKey([]byte{}) {
		t.Error("expected empty key not to be catalog key")
	}

	// Short key
	if isCatalogKey([]byte{0x00, 0x01, 0x02}) {
		t.Error("expected short key not to be catalog key")
	}
}

func TestEncodeCatalogKeyDB(t *testing.T) {
	key := encodeCatalogKeyDB("mydb")

	// Should start with catalog prefix
	if !bytes.HasPrefix(key, []byte(catalogPrefix)) {
		t.Error("expected catalog prefix")
	}

	// Should contain db type
	if len(key) < len(catalogPrefix)+1 {
		t.Fatal("key too short")
	}
	if key[len(catalogPrefix)] != catalogTypeDB {
		t.Error("expected catalogTypeDB")
	}

	// Should contain db name
	if !bytes.Contains(key, []byte("mydb")) {
		t.Error("expected db name in key")
	}
}

func TestEncodeCatalogKeyColl(t *testing.T) {
	key := encodeCatalogKeyColl("mydb", "mycoll")

	// Should start with catalog prefix
	if !bytes.HasPrefix(key, []byte(catalogPrefix)) {
		t.Error("expected catalog prefix")
	}

	// Should contain collection type
	if len(key) < len(catalogPrefix)+1 {
		t.Fatal("key too short")
	}
	if key[len(catalogPrefix)] != catalogTypeColl {
		t.Error("expected catalogTypeColl")
	}

	// Should contain db.coll
	if !bytes.Contains(key, []byte("mydb.mycoll")) {
		t.Error("expected 'mydb.mycoll' in key")
	}
}

func TestEncodeCatalogKeyIndex(t *testing.T) {
	key := encodeCatalogKeyIndex("mydb", "mycoll", "myindex")

	// Should start with catalog prefix
	if !bytes.HasPrefix(key, []byte(catalogPrefix)) {
		t.Error("expected catalog prefix")
	}

	// Should contain index type
	if len(key) < len(catalogPrefix)+1 {
		t.Fatal("key too short")
	}
	if key[len(catalogPrefix)] != catalogTypeIndex {
		t.Error("expected catalogTypeIndex")
	}

	// Should contain full index name
	if !bytes.Contains(key, []byte("mydb.mycoll.myindex")) {
		t.Error("expected 'mydb.mycoll.myindex' in key")
	}
}

func TestEncodeCatalogKeyUser(t *testing.T) {
	key := EncodeCatalogKeyUser("admin", "root")

	// Should start with catalog prefix
	if !bytes.HasPrefix(key, []byte(catalogPrefix)) {
		t.Error("expected catalog prefix")
	}

	// Should contain user type
	if len(key) < len(catalogPrefix)+1 {
		t.Fatal("key too short")
	}
	if key[len(catalogPrefix)] != catalogTypeUser {
		t.Error("expected catalogTypeUser")
	}

	// Should contain db and username separated by null byte
	expectedContent := []byte("admin\x00root")
	if !bytes.Contains(key, expectedContent) {
		t.Errorf("expected 'admin\\x00root' in key, got %v", key)
	}
}

func TestEncodeCatalogKeyUserPrefix(t *testing.T) {
	key := EncodeCatalogKeyUserPrefix()

	// Should start with catalog prefix + user type
	expectedPrefix := append([]byte(catalogPrefix), catalogTypeUser)
	if !bytes.HasPrefix(key, expectedPrefix) {
		t.Error("expected catalog prefix + user type")
	}
}

func TestEncodeCatalogKeyUserDBPrefix(t *testing.T) {
	key := EncodeCatalogKeyUserDBPrefix("mydb")

	// Should start with catalog prefix + user type
	expectedPrefix := append([]byte(catalogPrefix), catalogTypeUser)
	if !bytes.HasPrefix(key, expectedPrefix) {
		t.Error("expected catalog prefix + user type")
	}

	// Should contain db name + null byte
	if !bytes.Contains(key, []byte("mydb\x00")) {
		t.Error("expected 'mydb\\x00' in key")
	}
}

func TestEncodeCatalogKeyRole(t *testing.T) {
	key := EncodeCatalogKeyRole("admin", "readWrite")

	// Should start with catalog prefix
	if !bytes.HasPrefix(key, []byte(catalogPrefix)) {
		t.Error("expected catalog prefix")
	}

	// Should contain role type
	if len(key) < len(catalogPrefix)+1 {
		t.Fatal("key too short")
	}
	if key[len(catalogPrefix)] != catalogTypeRole {
		t.Error("expected catalogTypeRole")
	}

	// Should contain db and role name separated by null byte
	expectedContent := []byte("admin\x00readWrite")
	if !bytes.Contains(key, expectedContent) {
		t.Errorf("expected 'admin\\x00readWrite' in key, got %v", key)
	}
}

func TestEncodeCatalogKeyRolePrefix(t *testing.T) {
	key := EncodeCatalogKeyRolePrefix("mydb")

	// Should start with catalog prefix + role type
	expectedPrefix := append([]byte(catalogPrefix), catalogTypeRole)
	if !bytes.HasPrefix(key, expectedPrefix) {
		t.Error("expected catalog prefix + role type")
	}

	// Should contain db name + null byte
	if !bytes.Contains(key, []byte("mydb\x00")) {
		t.Error("expected 'mydb\\x00' in key")
	}
}

func TestEncodeCatalogKeyValidator(t *testing.T) {
	key := EncodeCatalogKeyValidator("mydb", "mycoll")

	// Should start with catalog prefix
	if !bytes.HasPrefix(key, []byte(catalogPrefix)) {
		t.Error("expected catalog prefix")
	}

	// Should contain validator type
	if len(key) < len(catalogPrefix)+1 {
		t.Fatal("key too short")
	}
	if key[len(catalogPrefix)] != catalogTypeValid {
		t.Error("expected catalogTypeValid")
	}

	// Should contain db.coll
	if !bytes.Contains(key, []byte("mydb.mycoll")) {
		t.Error("expected 'mydb.mycoll' in key")
	}
}

func TestEncodeIndexKey(t *testing.T) {
	id := []byte("docid")
	encodedValue := []byte{0x01, 0x02, 0x03}
	key := encodeIndexKey("mydb", "mycoll", "idx_name", 0x42, encodedValue, id)

	// Should contain namespace prefix
	nsPrefix := EncodeNamespacePrefix("mydb", "mycoll")
	if !bytes.HasPrefix(key, nsPrefix) {
		t.Error("expected namespace prefix")
	}

	// Should contain index separator
	if !bytes.Contains(key, []byte(indexSeparator)) {
		t.Error("expected index separator")
	}

	// Should contain index name
	if !bytes.Contains(key, []byte("idx_name")) {
		t.Error("expected index name")
	}

	// Should contain type tag
	if !bytes.Contains(key, []byte{0x42}) {
		t.Error("expected type tag")
	}

	// Should end with id
	if !bytes.HasSuffix(key, id) {
		t.Error("expected key to end with id")
	}
}
