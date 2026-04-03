package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Mock implementations for testing

type mockCatalog struct {
	collections map[string]*mockCollection
}

func newMockCatalog() *mockCatalog {
	return &mockCatalog{
		collections: make(map[string]*mockCollection),
	}
}

func (m *mockCatalog) ListCollections() ([]string, error) {
	names := make([]string, 0, len(m.collections))
	for name := range m.collections {
		names = append(names, name)
	}
	return names, nil
}

func (m *mockCatalog) GetCollection(name string) (Collection, error) {
	coll, ok := m.collections[name]
	if !ok {
		return nil, errors.New("collection not found")
	}
	return coll, nil
}

func (m *mockCatalog) AddCollection(name string, docs []*bson.Document) {
	m.collections[name] = &mockCollection{
		name: name,
		docs: docs,
	}
}

type mockCollection struct {
	name string
	docs []*bson.Document
	pos  int
}

func (m *mockCollection) Name() string {
	return m.name
}

func (m *mockCollection) FindAll(ctx context.Context) (DocumentIterator, error) {
	return &mockIterator{docs: m.docs}, nil
}

func (m *mockCollection) Count() (int64, error) {
	return int64(len(m.docs)), nil
}

type mockIterator struct {
	docs []*bson.Document
	pos  int
}

func (m *mockIterator) Next() (*bson.Document, error) {
	if m.pos >= len(m.docs) {
		return nil, nil
	}
	doc := m.docs[m.pos]
	m.pos++
	return doc, nil
}

func (m *mockIterator) Close() error {
	return nil
}

type mockOplogStore struct {
	timestamp int64
	entries   []*OplogEntry
}

func (m *mockOplogStore) GetTimestamp() (int64, error) {
	return m.timestamp, nil
}

func (m *mockOplogStore) FindSince(ctx context.Context, timestamp int64) (OplogIterator, error) {
	var filtered []*OplogEntry
	for _, e := range m.entries {
		if e.Timestamp >= timestamp {
			filtered = append(filtered, e)
		}
	}
	return &mockOplogIterator{entries: filtered}, nil
}

type mockOplogIterator struct {
	entries []*OplogEntry
	pos     int
}

func (m *mockOplogIterator) Next() (*OplogEntry, error) {
	if m.pos >= len(m.entries) {
		return nil, nil
	}
	entry := m.entries[m.pos]
	m.pos++
	return entry, nil
}

func (m *mockOplogIterator) Close() error {
	return nil
}

// Test New
func TestNew(t *testing.T) {
	catalog := newMockCatalog()
	oplog := &mockOplogStore{}

	opts := Options{
		Format:   FormatBSON,
		Compress: true,
	}

	b := New(catalog, oplog, opts)
	if b == nil {
		t.Fatal("expected non-nil Backup")
	}
	if b.format != FormatBSON {
		t.Errorf("expected format BSON, got %s", b.format)
	}
	if !b.compress {
		t.Error("expected compress to be true")
	}
}

func TestNew_DefaultFormat(t *testing.T) {
	catalog := newMockCatalog()
	oplog := &mockOplogStore{}

	opts := Options{
		Format: "", // Empty should default to BSON
	}

	b := New(catalog, oplog, opts)
	if b.format != FormatBSON {
		t.Errorf("expected default format BSON, got %s", b.format)
	}
}

// Test Create
func TestCreate(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()

	// Add test collections
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc1.Set("name", bson.VString("Alice"))

	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc2.Set("name", bson.VString("Bob"))

	catalog.AddCollection("users", []*bson.Document{doc1, doc2})
	catalog.AddCollection("empty", []*bson.Document{})

	oplog := &mockOplogStore{}

	opts := Options{
		Format:    FormatBSON,
		Compress:  false,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	metadata, err := b.Create(ctx, "testdb")
	if err != nil {
		t.Fatalf("create backup failed: %v", err)
	}

	if metadata.Version != "1.0" {
		t.Errorf("expected version 1.0, got %s", metadata.Version)
	}
	if metadata.Database != "testdb" {
		t.Errorf("expected database testdb, got %s", metadata.Database)
	}
	if metadata.Format != FormatBSON {
		t.Errorf("expected format BSON, got %s", metadata.Format)
	}
	if len(metadata.Collections) != 2 {
		t.Errorf("expected 2 collections, got %d", len(metadata.Collections))
	}
	if metadata.Checksum == "" {
		t.Error("expected non-empty checksum")
	}
}

func TestCreate_WithCompression(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("data", bson.VString("test data"))

	catalog.AddCollection("data", []*bson.Document{doc})

	oplog := &mockOplogStore{}

	opts := Options{
		Format:    FormatBSON,
		Compress:  true,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	metadata, err := b.Create(ctx, "testdb")
	if err != nil {
		t.Fatalf("create backup failed: %v", err)
	}

	if !metadata.Compressed {
		t.Error("expected compressed to be true")
	}
}

func TestCreate_WithJSON(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("test"))

	catalog.AddCollection("items", []*bson.Document{doc})

	oplog := &mockOplogStore{}

	opts := Options{
		Format:    FormatJSON,
		Compress:  false,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	metadata, err := b.Create(ctx, "testdb")
	if err != nil {
		t.Fatalf("create backup failed: %v", err)
	}

	if metadata.Format != FormatJSON {
		t.Errorf("expected format JSON, got %s", metadata.Format)
	}
}

func TestCreate_ListCollectionsError(t *testing.T) {
	// Create a mock catalog that returns an error
	catalog := &errorCatalog{}
	oplog := &mockOplogStore{}

	opts := Options{Format: FormatBSON}
	b := New(catalog, oplog, opts)
	ctx := context.Background()

	_, err := b.Create(ctx, "testdb")
	if err == nil {
		t.Error("expected error when listing collections fails")
	}
}

// Error catalog for testing error cases
type errorCatalog struct{}

func (e *errorCatalog) ListCollections() ([]string, error) {
	return nil, errors.New("list collections error")
}

func (e *errorCatalog) GetCollection(name string) (Collection, error) {
	return nil, errors.New("collection not found")
}

// Test CreateIncremental
func TestCreateIncremental(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()

	oplog := &mockOplogStore{
		timestamp: time.Now().Unix(),
		entries: []*OplogEntry{
			{
				Timestamp: time.Now().Unix() - 100,
				Operation: "i",
				Namespace: "testdb.users",
				Document:  map[string]interface{}{"name": "Alice"},
			},
			{
				Timestamp: time.Now().Unix() - 50,
				Operation: "i",
				Namespace: "testdb.users",
				Document:  map[string]interface{}{"name": "Bob"},
			},
		},
	}

	opts := Options{
		Format:    FormatBSON,
		Compress:  false,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	startTs := time.Now().Unix() - 200
	metadata, err := b.CreateIncremental(ctx, "testdb", startTs)
	if err != nil {
		t.Fatalf("create incremental backup failed: %v", err)
	}

	if !metadata.Incremental {
		t.Error("expected incremental to be true")
	}
	if metadata.OplogStart != startTs {
		t.Errorf("expected oplog start %d, got %d", startTs, metadata.OplogStart)
	}
	if metadata.OplogEnd != oplog.timestamp {
		t.Errorf("expected oplog end %d, got %d", oplog.timestamp, metadata.OplogEnd)
	}
}

func TestCreateIncremental_Compressed(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()

	oplog := &mockOplogStore{
		timestamp: time.Now().Unix(),
		entries: []*OplogEntry{
			{
				Timestamp: time.Now().Unix(),
				Operation: "i",
				Namespace: "testdb.items",
				Document:  map[string]interface{}{"value": 42},
			},
		},
	}

	opts := Options{
		Format:    FormatBSON,
		Compress:  true,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	metadata, err := b.CreateIncremental(ctx, "testdb", time.Now().Unix()-100)
	if err != nil {
		t.Fatalf("create incremental backup failed: %v", err)
	}

	if !metadata.Compressed {
		t.Error("expected compressed to be true")
	}
}

// Test helper functions
func TestBsonDocumentToMap(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("Alice"))
	doc.Set("age", bson.VInt32(30))
	doc.Set("active", bson.VBool(true))

	m := bsonDocumentToMap(doc)
	if m["name"] != "Alice" {
		t.Errorf("expected name Alice, got %v", m["name"])
	}
	if m["age"] != int32(30) {
		t.Errorf("expected age 30, got %v", m["age"])
	}
	if m["active"] != true {
		t.Errorf("expected active true, got %v", m["active"])
	}
}

func TestBsonDocumentToMap_WithArray(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("tags", bson.VArray(bson.A(
		bson.VString("tag1"),
		bson.VString("tag2"),
	)))

	m := bsonDocumentToMap(doc)
	tags, ok := m["tags"].([]interface{})
	if !ok {
		t.Fatalf("expected tags to be []interface{}, got %T", m["tags"])
	}
	if len(tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(tags))
	}
}

func TestBsonDocumentToMap_WithNestedDoc(t *testing.T) {
	inner := bson.NewDocument()
	inner.Set("city", bson.VString("Boston"))

	doc := bson.NewDocument()
	doc.Set("address", bson.VDoc(inner))

	m := bsonDocumentToMap(doc)
	addr, ok := m["address"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected address to be map, got %T", m["address"])
	}
	if addr["city"] != "Boston" {
		t.Errorf("expected city Boston, got %v", addr["city"])
	}
}

func TestBsonValueToInterface(t *testing.T) {
	tests := []struct {
		name     string
		value    bson.Value
		expected interface{}
	}{
		{"null", bson.VNull(), nil},
		{"bool", bson.VBool(true), true},
		{"int32", bson.VInt32(42), int32(42)},
		{"int64", bson.VInt64(64), int64(64)},
		{"double", bson.VDouble(3.14), 3.14},
		{"string", bson.VString("hello"), "hello"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := bsonValueToInterface(tc.value)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestBsonValueToInterface_ObjectID(t *testing.T) {
	oid := bson.NewObjectID()
	val := bson.VObjectID(oid)
	result := bsonValueToInterface(val)
	if result != oid.String() {
		t.Errorf("expected ObjectID string, got %v", result)
	}
}

func TestInterfaceToBSONValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected bson.BSONType
	}{
		{"nil", nil, bson.TypeNull},
		{"bool", true, bson.TypeBoolean},
		{"int", int(42), bson.TypeInt32},
		{"int32", int32(42), bson.TypeInt32},
		{"int64", int64(64), bson.TypeInt64},
		{"float64", 3.14, bson.TypeDouble},
		{"string", "hello", bson.TypeString},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := interfaceToBSONValue(tc.input)
			if result.Type != tc.expected {
				t.Errorf("expected type %v, got %v", tc.expected, result.Type)
			}
		})
	}
}

func TestInterfaceToBSONValue_Map(t *testing.T) {
	m := map[string]interface{}{
		"name": "Alice",
		"age":  30,
	}
	val := interfaceToBSONValue(m)
	if val.Type != bson.TypeDocument {
		t.Errorf("expected TypeDocument, got %v", val.Type)
	}
	doc := val.DocumentValue()
	if doc == nil {
		t.Fatal("expected non-nil document")
	}
	nameVal, ok := doc.Get("name")
	if !ok || nameVal.String() != "Alice" {
		t.Error("expected name field")
	}
}

func TestInterfaceToBSONValue_Slice(t *testing.T) {
	s := []interface{}{"a", "b", "c"}
	val := interfaceToBSONValue(s)
	if val.Type != bson.TypeArray {
		t.Errorf("expected TypeArray, got %v", val.Type)
	}
	arr := val.ArrayValue()
	if len(arr) != 3 {
		t.Errorf("expected 3 elements, got %d", len(arr))
	}
}

func TestWriteInt32(t *testing.T) {
	file, err := os.CreateTemp("", "test-write-int32")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	err = writeInt32(file, 0x12345678)
	if err != nil {
		t.Fatalf("writeInt32 failed: %v", err)
	}
	file.Close()

	// Read back and verify
	data, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Little endian: 0x78, 0x56, 0x34, 0x12
	expected := []byte{0x78, 0x56, 0x34, 0x12}
	if len(data) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(data))
	}
	for i, b := range expected {
		if data[i] != b {
			t.Errorf("byte %d: expected 0x%x, got 0x%x", i, b, data[i])
		}
	}
}

func TestCalculateMetadataChecksum(t *testing.T) {
	catalog := newMockCatalog()
	oplog := &mockOplogStore{}

	b := New(catalog, oplog, Options{})

	metadata := &Metadata{
		Version:  "1.0",
		Database: "testdb",
	}

	checksum := b.calculateMetadataChecksum(metadata)
	if checksum == "" {
		t.Error("expected non-empty checksum")
	}

	// Same metadata should produce same checksum
	checksum2 := b.calculateMetadataChecksum(metadata)
	if checksum != checksum2 {
		t.Error("expected same checksum for same metadata")
	}

	// Different metadata should produce different checksum
	metadata.Database = "otherdb"
	checksum3 := b.calculateMetadataChecksum(metadata)
	if checksum == checksum3 {
		t.Error("expected different checksum for different metadata")
	}
}

func TestMetadataStructs(t *testing.T) {
	// Test Metadata struct
	meta := Metadata{
		Version:     "1.0",
		CreatedAt:   time.Now(),
		Format:      FormatBSON,
		Compressed:  true,
		Database:    "testdb",
		Incremental: false,
		Checksum:    "abc123",
	}

	if meta.Version != "1.0" {
		t.Error("version mismatch")
	}

	// Test CollectionMeta struct
	collMeta := CollectionMeta{
		Name:          "users",
		DocumentCount: 100,
		Size:          1024,
		Checksum:      "def456",
	}

	if collMeta.Name != "users" {
		t.Error("name mismatch")
	}

	// Test OplogEntry struct
	entry := OplogEntry{
		Timestamp:  1234567890,
		Operation:  "i",
		Namespace:  "testdb.users",
		DocumentID: "abc",
		Document:   map[string]interface{}{"name": "Alice"},
	}

	if entry.Operation != "i" {
		t.Error("operation mismatch")
	}
}

func TestFormatConsts(t *testing.T) {
	if FormatBSON != "bson" {
		t.Errorf("expected FormatBSON to be 'bson', got %s", FormatBSON)
	}
	if FormatJSON != "json" {
		t.Errorf("expected FormatJSON to be 'json', got %s", FormatJSON)
	}
}

func TestOptions(t *testing.T) {
	opts := Options{
		Format:      FormatBSON,
		Compress:    true,
		Incremental: false,
		Collections: []string{"coll1", "coll2"},
		OutputDir:   "/tmp/backup",
	}

	if len(opts.Collections) != 2 {
		t.Errorf("expected 2 collections, got %d", len(opts.Collections))
	}
	if opts.OutputDir != "/tmp/backup" {
		t.Errorf("expected output dir /tmp/backup, got %s", opts.OutputDir)
	}
}

func TestBackupFilesCreated(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("data", bson.VString("test"))

	catalog.AddCollection("testcoll", []*bson.Document{doc})

	oplog := &mockOplogStore{}

	opts := Options{
		Format:    FormatBSON,
		Compress:  false,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	metadata, err := b.Create(ctx, "testdb")
	if err != nil {
		t.Fatalf("create backup failed: %v", err)
	}

	// Check that backup directory was created
	entries, err := os.ReadDir("testdb")
	if err != nil {
		t.Fatalf("failed to read backup dir: %v", err)
	}

	if len(entries) == 0 {
		t.Error("expected backup directory to be created")
	}

	// Find the timestamp directory
	var timestampDir string
	for _, entry := range entries {
		if entry.IsDir() {
			timestampDir = filepath.Join("testdb", entry.Name())
			break
		}
	}

	if timestampDir == "" {
		t.Fatal("expected timestamp directory")
	}

	// Check that metadata file exists
	metadataFile := filepath.Join(timestampDir, "metadata.json")
	if _, err := os.Stat(metadataFile); os.IsNotExist(err) {
		t.Error("expected metadata.json to exist")
	}

	// Check that collection file exists
	collFile := filepath.Join(timestampDir, "testcoll.bson")
	if _, err := os.Stat(collFile); os.IsNotExist(err) {
		t.Error("expected testcoll.bson to exist")
	}

	// Verify metadata content
	if len(metadata.Collections) != 1 {
		t.Errorf("expected 1 collection in metadata, got %d", len(metadata.Collections))
	}
	if metadata.Collections[0].Name != "testcoll" {
		t.Errorf("expected collection name testcoll, got %s", metadata.Collections[0].Name)
	}
	if metadata.Collections[0].DocumentCount != 1 {
		t.Errorf("expected 1 document, got %d", metadata.Collections[0].DocumentCount)
	}
}

func TestEmptyCollection(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()

	catalog.AddCollection("empty", []*bson.Document{})

	oplog := &mockOplogStore{}

	opts := Options{
		Format:    FormatBSON,
		Compress:  false,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	metadata, err := b.Create(ctx, "testdb")
	if err != nil {
		t.Fatalf("create backup failed: %v", err)
	}

	found := false
	for _, coll := range metadata.Collections {
		if coll.Name == "empty" {
			found = true
			if coll.DocumentCount != 0 {
				t.Errorf("expected 0 documents, got %d", coll.DocumentCount)
			}
		}
	}
	if !found {
		t.Error("expected empty collection in metadata")
	}
}
