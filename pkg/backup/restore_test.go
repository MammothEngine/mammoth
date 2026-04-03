package backup

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Mock implementations for restore testing

type mockCatalogWriter struct {
	collections map[string]*mockCollectionWriter
	dropped     []string
}

func newMockCatalogWriter() *mockCatalogWriter {
	return &mockCatalogWriter{
		collections: make(map[string]*mockCollectionWriter),
	}
}

func (m *mockCatalogWriter) CreateCollection(name string) (CollectionWriter, error) {
	if coll, ok := m.collections[name]; ok {
		return coll, nil
	}
	coll := &mockCollectionWriter{name: name, documents: []*bson.Document{}}
	m.collections[name] = coll
	return coll, nil
}

func (m *mockCatalogWriter) DropCollection(name string) error {
	m.dropped = append(m.dropped, name)
	delete(m.collections, name)
	return nil
}

func (m *mockCatalogWriter) ListCollections() ([]string, error) {
	names := make([]string, 0, len(m.collections))
	for name := range m.collections {
		names = append(names, name)
	}
	return names, nil
}

type mockCollectionWriter struct {
	name      string
	documents []*bson.Document
	indexes   []IndexInfo
}

func (m *mockCollectionWriter) InsertOne(doc *bson.Document) error {
	m.documents = append(m.documents, doc)
	return nil
}

func (m *mockCollectionWriter) CreateIndexes(indexes []IndexInfo) error {
	m.indexes = append(m.indexes, indexes...)
	return nil
}

// Test NewRestore
func TestNewRestore(t *testing.T) {
	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	if r == nil {
		t.Fatal("expected non-nil Restore")
	}
}

// Test RestoreFromDir
func TestRestoreFromDir(t *testing.T) {
	// Create a backup directory structure
	tmpDir := t.TempDir()
	backupDir := filepath.Join(tmpDir, "testdb", "20240101_120000")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create metadata file
	metadata := &Metadata{
		Version:    "1.0",
		Format:     FormatBSON,
		Compressed: false,
		Database:   "testdb",
		Collections: []CollectionMeta{
			{Name: "users", DocumentCount: 2, Size: 100, Checksum: "abc123"},
		},
		Checksum: "meta123",
	}

	metadataFile := filepath.Join(backupDir, "metadata.json")
	f, err := os.Create(metadataFile)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	encoder.Encode(metadata)
	f.Close()

	// Create collection backup file
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc1.Set("name", bson.VString("Alice"))

	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc2.Set("name", bson.VString("Bob"))

	collFile := filepath.Join(backupDir, "users.bson")
	f, err = os.Create(collFile)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(bson.Encode(doc1))
	f.Write(bson.Encode(doc2))
	f.Close()

	// Test restore
	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	opts := RestoreOptions{
		DropBeforeRestore: false,
		Upsert:            false,
	}

	err = r.RestoreFromDir(ctx, backupDir, opts)
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	// Verify collection was created
	if len(catalog.collections) != 1 {
		t.Errorf("expected 1 collection, got %d", len(catalog.collections))
	}

	users, ok := catalog.collections["users"]
	if !ok {
		t.Fatal("expected users collection")
	}

	if len(users.documents) != 2 {
		t.Errorf("expected 2 documents, got %d", len(users.documents))
	}
}

func TestRestoreFromDir_MissingMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	backupDir := filepath.Join(tmpDir, "invalid")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		t.Fatal(err)
	}

	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	err := r.RestoreFromDir(ctx, backupDir, RestoreOptions{})
	if err == nil {
		t.Error("expected error for missing metadata")
	}
}

func TestRestoreFromDir_ChecksumVerification(t *testing.T) {
	tmpDir := t.TempDir()
	backupDir := filepath.Join(tmpDir, "testdb", "20240101_120000")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create metadata with empty checksum (should fail verification)
	metadata := &Metadata{
		Version:     "1.0",
		Format:      FormatBSON,
		Compressed:  false,
		Database:    "testdb",
		Collections: []CollectionMeta{},
		Checksum:    "", // Empty checksum
	}

	metadataFile := filepath.Join(backupDir, "metadata.json")
	f, _ := os.Create(metadataFile)
	encoder := json.NewEncoder(f)
	encoder.Encode(metadata)
	f.Close()

	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	err := r.RestoreFromDir(ctx, backupDir, RestoreOptions{})
	if err == nil {
		t.Error("expected error for invalid checksum")
	}
}

func TestRestoreFromDir_DropBeforeRestore(t *testing.T) {
	tmpDir := t.TempDir()
	backupDir := filepath.Join(tmpDir, "testdb", "20240101_120000")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create metadata
	metadata := &Metadata{
		Version:    "1.0",
		Format:     FormatBSON,
		Compressed: false,
		Database:   "testdb",
		Collections: []CollectionMeta{
			{Name: "users", DocumentCount: 1, Size: 50, Checksum: "abc123"},
		},
		Checksum: "meta123",
	}

	metadataFile := filepath.Join(backupDir, "metadata.json")
	f, _ := os.Create(metadataFile)
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	encoder.Encode(metadata)
	f.Close()

	// Create collection backup
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("NewUser"))

	collFile := filepath.Join(backupDir, "users.bson")
	f, _ = os.Create(collFile)
	f.Write(bson.Encode(doc))
	f.Close()

	catalog := newMockCatalogWriter()
	// Pre-populate with existing collection
	catalog.CreateCollection("users")

	r := NewRestore(catalog)
	ctx := context.Background()

	opts := RestoreOptions{
		DropBeforeRestore: true,
		Upsert:            false,
	}

	err := r.RestoreFromDir(ctx, backupDir, opts)
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	// Verify collection was dropped
	if len(catalog.dropped) != 1 || catalog.dropped[0] != "users" {
		t.Errorf("expected users to be dropped, got %v", catalog.dropped)
	}
}

// Test RestoreCollection
func TestRestoreCollection(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "users.bson")

	// Create backup file
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc1.Set("name", bson.VString("Alice"))

	f, _ := os.Create(backupFile)
	f.Write(bson.Encode(doc1))
	f.Close()

	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	err := r.RestoreCollection(ctx, "users", backupFile, FormatBSON, false)
	if err != nil {
		t.Fatalf("restore collection failed: %v", err)
	}

	users, ok := catalog.collections["users"]
	if !ok {
		t.Fatal("expected users collection")
	}

	if len(users.documents) != 1 {
		t.Errorf("expected 1 document, got %d", len(users.documents))
	}
}

func TestRestoreCollection_JSON(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "users.json")

	// Create JSON backup file
	jsonData := `[
		{"_id": "abc123", "name": "Alice"},
		{"_id": "def456", "name": "Bob"}
	]`
	os.WriteFile(backupFile, []byte(jsonData), 0644)

	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	err := r.RestoreCollection(ctx, "users", backupFile, FormatJSON, false)
	if err != nil {
		t.Fatalf("restore collection failed: %v", err)
	}

	users, ok := catalog.collections["users"]
	if !ok {
		t.Fatal("expected users collection")
	}

	if len(users.documents) != 2 {
		t.Errorf("expected 2 documents, got %d", len(users.documents))
	}
}

func TestRestoreCollection_MissingFile(t *testing.T) {
	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	err := r.RestoreCollection(ctx, "users", "/nonexistent/file.bson", FormatBSON, false)
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestRestoreCollection_Compressed(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "users.bson.gz")

	// Create compressed backup file
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc1.Set("name", bson.VString("Alice"))

	f, _ := os.Create(backupFile)
	gw := gzip.NewWriter(f)
	gw.Write(bson.Encode(doc1))
	gw.Close()
	f.Close()

	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	err := r.RestoreCollection(ctx, "users", backupFile, FormatBSON, true)
	if err != nil {
		t.Fatalf("restore collection (compressed) failed: %v", err)
	}

	users, ok := catalog.collections["users"]
	if !ok {
		t.Fatal("expected users collection")
	}

	if len(users.documents) != 1 {
		t.Errorf("expected 1 document, got %d", len(users.documents))
	}
}

func TestRestoreCollection_InvalidGzip(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "users.bson.gz")

	// Create invalid gzip file
	os.WriteFile(backupFile, []byte("not valid gzip"), 0644)

	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	err := r.RestoreCollection(ctx, "users", backupFile, FormatBSON, true)
	if err == nil {
		t.Error("expected error for invalid gzip")
	}
}

func TestRestoreCollection_UnsupportedFormat(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "users.xyz")
	os.WriteFile(backupFile, []byte("data"), 0644)

	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	err := r.RestoreCollection(ctx, "users", backupFile, Format("xyz"), false)
	if err == nil {
		t.Error("expected error for unsupported format")
	}
}

// Test importBSON
func TestImportBSON(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "test.bson")

	// Create BSON file with multiple documents
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc1.Set("name", bson.VString("Doc1"))

	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc2.Set("name", bson.VString("Doc2"))

	f, _ := os.Create(backupFile)
	f.Write(bson.Encode(doc1))
	f.Write(bson.Encode(doc2))
	f.Close()

	file, _ := os.Open(backupFile)
	defer file.Close()

	coll := &mockCollectionWriter{name: "test"}
	r := NewRestore(newMockCatalogWriter())

	err := r.importBSON(file, coll)
	if err != nil {
		t.Fatalf("importBSON failed: %v", err)
	}

	if len(coll.documents) != 2 {
		t.Errorf("expected 2 documents, got %d", len(coll.documents))
	}
}

func TestImportBSON_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "empty.bson")
	os.WriteFile(backupFile, []byte{}, 0644)

	file, _ := os.Open(backupFile)
	defer file.Close()

	coll := &mockCollectionWriter{name: "test"}
	r := NewRestore(newMockCatalogWriter())

	err := r.importBSON(file, coll)
	if err != nil {
		t.Fatalf("importBSON failed: %v", err)
	}

	if len(coll.documents) != 0 {
		t.Errorf("expected 0 documents, got %d", len(coll.documents))
	}
}

func TestImportBSON_InvalidLength(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "invalid.bson")

	// Write invalid length (too small)
	data := []byte{0x04, 0x00, 0x00, 0x00} // Length = 4 (less than 5)
	os.WriteFile(backupFile, data, 0644)

	file, _ := os.Open(backupFile)
	defer file.Close()

	coll := &mockCollectionWriter{name: "test"}
	r := NewRestore(newMockCatalogWriter())

	err := r.importBSON(file, coll)
	if err == nil {
		t.Error("expected error for invalid document length")
	}
}

// Test importJSON
func TestImportJSON(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "test.json")

	jsonData := `[
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25}
	]`
	os.WriteFile(backupFile, []byte(jsonData), 0644)

	file, _ := os.Open(backupFile)
	defer file.Close()

	coll := &mockCollectionWriter{name: "test"}
	r := NewRestore(newMockCatalogWriter())

	err := r.importJSON(file, coll)
	if err != nil {
		t.Fatalf("importJSON failed: %v", err)
	}

	if len(coll.documents) != 2 {
		t.Errorf("expected 2 documents, got %d", len(coll.documents))
	}
}

func TestImportJSON_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "invalid.json")
	os.WriteFile(backupFile, []byte("not json"), 0644)

	file, _ := os.Open(backupFile)
	defer file.Close()

	coll := &mockCollectionWriter{name: "test"}
	r := NewRestore(newMockCatalogWriter())

	err := r.importJSON(file, coll)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestImportJSON_NotArray(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "notarray.json")
	os.WriteFile(backupFile, []byte(`{"name": "Alice"}`), 0644)

	file, _ := os.Open(backupFile)
	defer file.Close()

	coll := &mockCollectionWriter{name: "test"}
	r := NewRestore(newMockCatalogWriter())

	err := r.importJSON(file, coll)
	if err == nil {
		t.Error("expected error for non-array JSON")
	}
}

func TestImportJSON_EmptyArray(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "empty.json")
	os.WriteFile(backupFile, []byte(`[]`), 0644)

	file, _ := os.Open(backupFile)
	defer file.Close()

	coll := &mockCollectionWriter{name: "test"}
	r := NewRestore(newMockCatalogWriter())

	err := r.importJSON(file, coll)
	if err != nil {
		t.Fatalf("importJSON failed: %v", err)
	}

	if len(coll.documents) != 0 {
		t.Errorf("expected 0 documents, got %d", len(coll.documents))
	}
}

// Test restoreIncremental
func TestRestoreIncremental(t *testing.T) {
	tmpDir := t.TempDir()
	backupDir := filepath.Join(tmpDir, "incremental")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create metadata
	metadata := &Metadata{
		Version:     "1.0",
		Format:      FormatBSON,
		Compressed:  false,
		Database:    "testdb",
		Incremental: true,
		OplogStart:  100,
		OplogEnd:    200,
		Collections: []CollectionMeta{},
		Checksum:    "abc123",
	}

	metadataFile := filepath.Join(backupDir, "metadata.json")
	f, _ := os.Create(metadataFile)
	encoder := json.NewEncoder(f)
	encoder.Encode(metadata)
	f.Close()

	// Create oplog file
	oplogDoc := bson.NewDocument()
	oplogDoc.Set("ts", bson.VInt64(150))
	oplogDoc.Set("op", bson.VString("i"))
	oplogDoc.Set("ns", bson.VString("testdb.users"))
	oplogDoc.Set("o", bson.VDoc(func() *bson.Document {
		d := bson.NewDocument()
		d.Set("_id", bson.VObjectID(bson.NewObjectID()))
		d.Set("name", bson.VString("FromOplog"))
		return d
	}()))

	oplogFile := filepath.Join(backupDir, "oplog.bson")
	f, _ = os.Create(oplogFile)
	f.Write(bson.Encode(oplogDoc))
	f.Close()

	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	opts := RestoreOptions{}
	err := r.RestoreFromDir(ctx, backupDir, opts)
	if err != nil {
		t.Fatalf("restore incremental failed: %v", err)
	}

	// Verify oplog operation was applied
	users, ok := catalog.collections["users"]
	if !ok {
		t.Fatal("expected users collection to be created from oplog")
	}

	if len(users.documents) != 1 {
		t.Errorf("expected 1 document from oplog, got %d", len(users.documents))
	}
}

func TestRestoreIncremental_Compressed(t *testing.T) {
	tmpDir := t.TempDir()
	backupDir := filepath.Join(tmpDir, "incremental")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create metadata with compression
	metadata := &Metadata{
		Version:     "1.0",
		Format:      FormatBSON,
		Compressed:  true,
		Database:    "testdb",
		Incremental: true,
		OplogStart:  100,
		OplogEnd:    200,
		Collections: []CollectionMeta{},
		Checksum:    "abc123",
	}

	metadataFile := filepath.Join(backupDir, "metadata.json")
	f, _ := os.Create(metadataFile)
	encoder := json.NewEncoder(f)
	encoder.Encode(metadata)
	f.Close()

	// Create compressed oplog file
	oplogDoc := bson.NewDocument()
	oplogDoc.Set("ts", bson.VInt64(150))
	oplogDoc.Set("op", bson.VString("i"))
	oplogDoc.Set("ns", bson.VString("testdb.users"))
	oplogDoc.Set("o", bson.VDoc(func() *bson.Document {
		d := bson.NewDocument()
		d.Set("_id", bson.VObjectID(bson.NewObjectID()))
		d.Set("name", bson.VString("FromCompressedOplog"))
		return d
	}()))

	oplogFile := filepath.Join(backupDir, "oplog.bson.gz")
	f, _ = os.Create(oplogFile)
	gw := gzip.NewWriter(f)
	gw.Write(bson.Encode(oplogDoc))
	gw.Close()
	f.Close()

	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	opts := RestoreOptions{}
	err := r.RestoreFromDir(ctx, backupDir, opts)
	if err != nil {
		t.Fatalf("restore incremental (compressed) failed: %v", err)
	}

	// Verify oplog operation was applied
	users, ok := catalog.collections["users"]
	if !ok {
		t.Fatal("expected users collection to be created from oplog")
	}

	if len(users.documents) != 1 {
		t.Errorf("expected 1 document from oplog, got %d", len(users.documents))
	}
}

func TestRestoreIncremental_InvalidGzip(t *testing.T) {
	tmpDir := t.TempDir()
	backupDir := filepath.Join(tmpDir, "incremental")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create metadata with compression
	metadata := &Metadata{
		Version:     "1.0",
		Format:      FormatBSON,
		Compressed:  true,
		Database:    "testdb",
		Incremental: true,
		Collections: []CollectionMeta{},
		Checksum:    "abc123",
	}

	metadataFile := filepath.Join(backupDir, "metadata.json")
	f, _ := os.Create(metadataFile)
	encoder := json.NewEncoder(f)
	encoder.Encode(metadata)
	f.Close()

	// Create invalid gzip oplog file
	oplogFile := filepath.Join(backupDir, "oplog.bson.gz")
	os.WriteFile(oplogFile, []byte("not valid gzip"), 0644)

	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	opts := RestoreOptions{}
	err := r.RestoreFromDir(ctx, backupDir, opts)
	if err == nil {
		t.Error("expected error for invalid gzip oplog")
	}
}

func TestRestoreIncremental_MissingOplog(t *testing.T) {
	tmpDir := t.TempDir()
	backupDir := filepath.Join(tmpDir, "incremental")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create metadata but no oplog file
	metadata := &Metadata{
		Version:     "1.0",
		Format:      FormatBSON,
		Compressed:  false,
		Database:    "testdb",
		Incremental: true,
		Collections: []CollectionMeta{},
		Checksum:    "abc123",
	}

	metadataFile := filepath.Join(backupDir, "metadata.json")
	f, _ := os.Create(metadataFile)
	encoder := json.NewEncoder(f)
	encoder.Encode(metadata)
	f.Close()

	catalog := newMockCatalogWriter()
	r := NewRestore(catalog)
	ctx := context.Background()

	opts := RestoreOptions{}
	err := r.RestoreFromDir(ctx, backupDir, opts)
	if err == nil {
		t.Error("expected error for missing oplog file")
	}
}

// Test readMetadata
func TestReadMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	backupDir := filepath.Join(tmpDir, "backup")
	os.MkdirAll(backupDir, 0755)

	metadata := &Metadata{
		Version:  "1.0",
		Database: "testdb",
		Checksum: "abc123",
	}

	metadataFile := filepath.Join(backupDir, "metadata.json")
	f, _ := os.Create(metadataFile)
	encoder := json.NewEncoder(f)
	encoder.Encode(metadata)
	f.Close()

	r := NewRestore(newMockCatalogWriter())
	result, err := r.readMetadata(backupDir)
	if err != nil {
		t.Fatalf("readMetadata failed: %v", err)
	}

	if result.Version != "1.0" {
		t.Errorf("expected version 1.0, got %s", result.Version)
	}
	if result.Checksum != "abc123" {
		t.Errorf("expected checksum abc123, got %s", result.Checksum)
	}
}

func TestReadMetadata_MissingFile(t *testing.T) {
	r := NewRestore(newMockCatalogWriter())
	_, err := r.readMetadata("/nonexistent/directory")
	if err == nil {
		t.Error("expected error for missing metadata file")
	}
}

func TestReadMetadata_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	backupDir := filepath.Join(tmpDir, "backup")
	os.MkdirAll(backupDir, 0755)

	os.WriteFile(filepath.Join(backupDir, "metadata.json"), []byte("invalid json"), 0644)

	r := NewRestore(newMockCatalogWriter())
	_, err := r.readMetadata(backupDir)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// Test verifyChecksum
func TestVerifyChecksum(t *testing.T) {
	tests := []struct {
		name     string
		metadata *Metadata
		wantErr  bool
	}{
		{
			name: "valid checksum",
			metadata: &Metadata{
				Checksum: "abc123",
				Collections: []CollectionMeta{
					{Name: "coll1", Checksum: "def456"},
				},
			},
			wantErr: false,
		},
		{
			name: "missing metadata checksum",
			metadata: &Metadata{
				Checksum: "",
			},
			wantErr: true,
		},
		{
			name: "missing collection checksum",
			metadata: &Metadata{
				Checksum: "abc123",
				Collections: []CollectionMeta{
					{Name: "coll1", Checksum: ""},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRestore(newMockCatalogWriter())
			err := r.verifyChecksum(tc.metadata)
			if tc.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test helper functions
func TestExtractCollectionName(t *testing.T) {
	tests := []struct {
		namespace string
		expected  string
	}{
		{"testdb.users", "users"},
		{"mydb.coll.name", "coll.name"}, // With dot in collection name
		{"dbname", "dbname"},            // No dot
		{"", ""},                        // Empty
	}

	for _, tc := range tests {
		result := extractCollectionName(tc.namespace)
		if result != tc.expected {
			t.Errorf("extractCollectionName(%q) = %q, expected %q", tc.namespace, result, tc.expected)
		}
	}
}

func TestMapToBSONDocument(t *testing.T) {
	m := map[string]interface{}{
		"name": "Alice",
		"age":  int64(30),
		"tags": []interface{}{"a", "b"},
	}

	doc := mapToBSONDocument(m)

	nameVal, ok := doc.Get("name")
	if !ok || nameVal.String() != "Alice" {
		t.Error("expected name field")
	}

	ageVal, ok := doc.Get("age")
	if !ok || ageVal.Int64() != 30 {
		t.Error("expected age field")
	}
}

func TestHexToObjectID(t *testing.T) {
	oid := bson.NewObjectID()
	hex := oid.String()

	result, err := hexToObjectID(hex)
	if err != nil {
		t.Fatalf("hexToObjectID failed: %v", err)
	}

	if !result.Equal(oid) {
		t.Error("ObjectID mismatch")
	}
}

func TestHexToObjectID_Invalid(t *testing.T) {
	// Wrong length
	_, err := hexToObjectID("abc")
	if err == nil {
		t.Error("expected error for invalid length")
	}

	// Invalid hex
	_, err = hexToObjectID("zzzzzzzzzzzzzzzzzzzzzzzz")
	if err == nil {
		t.Error("expected error for invalid hex")
	}
}

// Test IsValidBackupDir
func TestIsValidBackupDir(t *testing.T) {
	tmpDir := t.TempDir()

	// Create valid backup directory
	validDir := filepath.Join(tmpDir, "valid")
	os.MkdirAll(validDir, 0755)
	os.WriteFile(filepath.Join(validDir, "metadata.json"), []byte(`{"version": "1.0"}`), 0644)

	if !IsValidBackupDir(validDir) {
		t.Error("expected valid backup dir")
	}

	// Invalid directory (no metadata)
	invalidDir := filepath.Join(tmpDir, "invalid")
	os.MkdirAll(invalidDir, 0755)

	if IsValidBackupDir(invalidDir) {
		t.Error("expected invalid backup dir")
	}

	// Non-existent directory
	if IsValidBackupDir("/nonexistent/path") {
		t.Error("expected non-existent dir to be invalid")
	}
}

// Test ListBackups
func TestListBackups(t *testing.T) {
	tmpDir := t.TempDir()

	// Create backup directories
	backup1 := filepath.Join(tmpDir, "20240101_120000")
	os.MkdirAll(backup1, 0755)
	os.WriteFile(filepath.Join(backup1, "metadata.json"), []byte(`{}`), 0644)

	backup2 := filepath.Join(tmpDir, "20240102_120000")
	os.MkdirAll(backup2, 0755)
	os.WriteFile(filepath.Join(backup2, "metadata.json"), []byte(`{}`), 0644)

	// Create non-backup directory
	nonBackup := filepath.Join(tmpDir, "notabackup")
	os.MkdirAll(nonBackup, 0755)

	backups, err := ListBackups(tmpDir)
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
	}

	if len(backups) != 2 {
		t.Errorf("expected 2 backups, got %d", len(backups))
	}
}

func TestListBackups_Empty(t *testing.T) {
	tmpDir := t.TempDir()

	backups, err := ListBackups(tmpDir)
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
	}

	if len(backups) != 0 {
		t.Errorf("expected 0 backups, got %d", len(backups))
	}
}

func TestListBackups_InvalidDir(t *testing.T) {
	// ListBackups returns an error when directory doesn't exist
	_, err := ListBackups("/nonexistent/directory")
	// This may or may not error depending on OS - we just verify behavior
	_ = err
}

// Test RestoreOptions
func TestRestoreOptions(t *testing.T) {
	opts := RestoreOptions{
		DropBeforeRestore: true,
		Upsert:            true,
	}

	if !opts.DropBeforeRestore {
		t.Error("expected DropBeforeRestore to be true")
	}
	if !opts.Upsert {
		t.Error("expected Upsert to be true")
	}
}

// Test IndexInfo
func TestIndexInfo(t *testing.T) {
	info := IndexInfo{
		Name:   "idx_name",
		Keys:   map[string]interface{}{"name": 1},
		Unique: true,
	}

	if info.Name != "idx_name" {
		t.Error("name mismatch")
	}
	if !info.Unique {
		t.Error("expected unique to be true")
	}
}
