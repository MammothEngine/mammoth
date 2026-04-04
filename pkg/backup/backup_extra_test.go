package backup

import (
	"context"
	"errors"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Mock error types
type errorCollection struct {
	name string
	err  error
}

func (e *errorCollection) Name() string {
	return e.name
}

func (e *errorCollection) FindAll(ctx context.Context) (DocumentIterator, error) {
	return nil, e.err
}

func (e *errorCollection) Count() (int64, error) {
	return 0, e.err
}

type errorIterator struct {
	err error
}

func (e *errorIterator) Next() (*bson.Document, error) {
	return nil, e.err
}

func (e *errorIterator) Close() error {
	return nil
}

type errorOplogStore struct {
	timestamp int64
	err       error
}

func (e *errorOplogStore) GetTimestamp() (int64, error) {
	return 0, e.err
}

func (e *errorOplogStore) FindSince(ctx context.Context, timestamp int64) (OplogIterator, error) {
	return nil, e.err
}

// TestCreateIncremental_GetTimestampError tests CreateIncremental when GetTimestamp fails
func TestCreateIncremental_GetTimestampError(t *testing.T) {
	catalog := newMockCatalog()
	oplog := &errorOplogStore{err: errors.New("timestamp error")}

	opts := Options{Format: FormatBSON}
	b := New(catalog, oplog, opts)
	ctx := context.Background()

	_, err := b.CreateIncremental(ctx, "testdb", 12345)
	if err == nil {
		t.Error("expected error when GetTimestamp fails")
	}
}

// TestCreateIncremental_BackupOplogError tests CreateIncremental when backupOplog fails
func TestCreateIncremental_BackupOplogError(t *testing.T) {
	catalog := newMockCatalog()
	oplog := &errorOplogStore{timestamp: 12345, err: errors.New("oplog error")}

	opts := Options{Format: FormatBSON}
	b := New(catalog, oplog, opts)
	ctx := context.Background()

	_, err := b.CreateIncremental(ctx, "testdb", 10000)
	if err == nil {
		t.Error("expected error when backupOplog fails")
	}
}

// TestBackupCollection_CountError tests backupCollection when Count fails
func TestBackupCollection_CountError(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()

	// Create collection that returns count error
	coll := &errorCollection{name: "errorcoll", err: errors.New("count error")}

	oplog := &mockOplogStore{}
	opts := Options{
		Format:    FormatBSON,
		Compress:  false,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	_, err := b.backupCollection(ctx, coll, tmpDir)
	if err == nil {
		t.Error("expected error when Count fails")
	}
}

// TestBackupCollection_FindAllError tests backupCollection when FindAll fails
func TestBackupCollection_FindAllError(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()

	// Create collection that returns FindAll error
	coll := &mockCollectionWithFindError{name: "errorcoll", err: errors.New("find error")}

	oplog := &mockOplogStore{}
	opts := Options{
		Format:    FormatBSON,
		Compress:  false,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	_, err := b.backupCollection(ctx, coll, tmpDir)
	if err == nil {
		t.Error("expected error when FindAll fails")
	}
}

type mockCollectionWithFindError struct {
	name string
	err  error
}

func (m *mockCollectionWithFindError) Name() string {
	return m.name
}

func (m *mockCollectionWithFindError) FindAll(ctx context.Context) (DocumentIterator, error) {
	return nil, m.err
}

func (m *mockCollectionWithFindError) Count() (int64, error) {
	return 0, nil
}

// TestExportDocuments_UnsupportedFormat tests exportDocuments with unsupported format
func TestExportDocuments_UnsupportedFormat(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))

	catalog.AddCollection("test", []*bson.Document{doc})

	oplog := &mockOplogStore{}
	opts := Options{
		Format:    Format("unsupported"),
		Compress:  false,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	coll, _ := catalog.GetCollection("test")
	_, err := b.exportDocuments(ctx, coll, &discardWriter{})
	if err == nil {
		t.Error("expected error for unsupported format")
	}
}

type discardWriter struct{}

func (d *discardWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

// TestExportBSON_IteratorError tests exportBSON when iterator returns error
func TestExportBSON_IteratorError(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()
	oplog := &mockOplogStore{}

	opts := Options{
		Format:    FormatBSON,
		Compress:  false,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)

	iterator := &errorIterator{err: errors.New("iterator error")}
	_, err := b.exportBSON(iterator, &discardWriter{})
	if err == nil {
		t.Error("expected error when iterator fails")
	}
}

// TestExportJSON_IteratorError tests exportJSON when iterator returns error
func TestExportJSON_IteratorError(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()
	oplog := &mockOplogStore{}

	opts := Options{
		Format:    FormatJSON,
		Compress:  false,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)

	iterator := &errorIterator{err: errors.New("iterator error")}
	_, err := b.exportJSON(iterator, &discardWriter{})
	if err == nil {
		t.Error("expected error when iterator fails")
	}
}

type errorWriter struct{}

func (e *errorWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("write error")
}

// TestBackupOplog_FindSinceError tests backupOplog when FindSince fails
func TestBackupOplog_FindSinceError(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()
	oplog := &errorOplogStore{err: errors.New("find since error")}

	opts := Options{
		Format:    FormatBSON,
		Compress:  false,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	err := b.backupOplog(ctx, 10000, 20000, tmpDir)
	if err == nil {
		t.Error("expected error when FindSince fails")
	}
}

// TestBackupOplog_IteratorError tests backupOplog when iterator returns error
func TestBackupOplog_IteratorError(t *testing.T) {
	tmpDir := t.TempDir()
	catalog := newMockCatalog()
	oplog := &mockOplogStoreWithIteratorError{err: errors.New("iterator error")}

	opts := Options{
		Format:    FormatBSON,
		Compress:  false,
		OutputDir: tmpDir,
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	err := b.backupOplog(ctx, 10000, 20000, tmpDir)
	if err == nil {
		t.Error("expected error when iterator fails")
	}
}

type mockOplogStoreWithIteratorError struct {
	err error
}

func (m *mockOplogStoreWithIteratorError) GetTimestamp() (int64, error) {
	return 20000, nil
}

func (m *mockOplogStoreWithIteratorError) FindSince(ctx context.Context, timestamp int64) (OplogIterator, error) {
	return &errorOplogIterator{err: m.err}, nil
}

type errorOplogIterator struct {
	err error
}

func (e *errorOplogIterator) Next() (*OplogEntry, error) {
	return nil, e.err
}

func (e *errorOplogIterator) Close() error {
	return nil
}

// TestExportJSON_ArrayEndWriteError tests exportJSON array end write error
func TestExportJSON_ArrayEndWriteError(t *testing.T) {
	catalog := newMockCatalog()
	oplog := &mockOplogStore{}

	// Empty collection - will try to write array end
	catalog.AddCollection("test", []*bson.Document{})

	opts := Options{
		Format:    FormatJSON,
		Compress:  false,
		OutputDir: t.TempDir(),
	}

	b := New(catalog, oplog, opts)
	ctx := context.Background()

	coll, _ := catalog.GetCollection("test")

	// First write (array start) should succeed, array end should fail
	writer := &arrayEndErrorWriter{}
	_, err := b.exportDocuments(ctx, coll, writer)
	if err == nil {
		t.Error("expected error when array end write fails")
	}
}

type arrayEndErrorWriter struct {
	writeCount int
}

func (a *arrayEndErrorWriter) Write(p []byte) (n int, err error) {
	a.writeCount++
	// First write ([\n) succeeds, subsequent writes fail
	if a.writeCount > 1 {
		return 0, errors.New("write error")
	}
	return len(p), nil
}

// TestBsonValueToInterface_Default tests the default case in bsonValueToInterface
func TestBsonValueToInterface_Default(t *testing.T) {
	// Create a value with an unsupported type - use binary type
	val := bson.VBinary(bson.BinaryGeneric, []byte{0x01, 0x02, 0x03})
	result := bsonValueToInterface(val)
	if result == nil {
		t.Error("expected non-nil result for binary type")
	}
}

// TestInterfaceToBSONValue_Default tests the default case in interfaceToBSONValue
func TestInterfaceToBSONValue_Default(t *testing.T) {
	// Pass a type that doesn't match any case
	type customStruct struct {
		Name string
	}
	val := interfaceToBSONValue(customStruct{Name: "test"})
	if val.Type != bson.TypeNull {
		t.Errorf("expected TypeNull for unknown type, got %v", val.Type)
	}
}

// TestInterfaceToBSONValue_NilDoc tests nil document case
func TestInterfaceToBSONValue_NilDoc(t *testing.T) {
	// Create a BSON value with nil document
	val := bson.VDoc(nil)
	result := bsonValueToInterface(val)
	if result != nil {
		t.Error("expected nil result for nil document")
	}
}
