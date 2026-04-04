package mammoth

import (
	"fmt"
	"testing"
)

// TestGridFSUploadStream_AbortWithChunks tests aborting after writing multiple chunks
func TestGridFSUploadStream_AbortWithChunks(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, err := db.OpenBucket(&BucketOptions{ChunkSizeBytes: 1024}) // Small chunks (minimum allowed)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	stream, _ := bucket.OpenUploadStream("abort_chunks.txt")

	// Write multiple chunks worth of data
	data := make([]byte, 100)
	for i := range data {
		data[i] = byte('a' + i%26)
	}
	stream.Write(data)

	// Abort should delete all chunks
	if err := stream.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
}

// TestGridFSUploadStream_AbortClosed tests aborting a closed stream
func TestGridFSUploadStream_AbortClosed(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, err := db.OpenBucket(nil)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	stream, _ := bucket.OpenUploadStream("abort_closed.txt")
	stream.Write([]byte("test"))

	// Close the stream first
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Abort should fail after close
	if err := stream.Abort(); err == nil {
		t.Error("expected error when aborting closed stream")
	}
}

// TestGridFSUploadStream_DoubleAbort tests aborting twice
func TestGridFSUploadStream_DoubleAbort(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, err := db.OpenBucket(nil)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	stream, _ := bucket.OpenUploadStream("double_abort.txt")
	stream.Write([]byte("test"))

	// First abort should succeed
	if err := stream.Abort(); err != nil {
		t.Fatalf("First Abort: %v", err)
	}

	// Second abort should return nil (already aborted)
	if err := stream.Abort(); err != nil {
		t.Errorf("Second Abort should return nil, got: %v", err)
	}
}

// TestGridFSBucket_OpenUploadStreamWithID tests uploading with specific ID
func TestGridFSBucket_OpenUploadStreamWithID(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, err := db.OpenBucket(nil)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	// Upload with specific ID
	fileID := "custom-file-id-123"
	stream, err := bucket.OpenUploadStreamWithID(fileID, "upload_with_id.txt")
	if err != nil {
		t.Fatalf("OpenUploadStreamWithID: %v", err)
	}
	stream.Write([]byte("test data"))
	stream.Close()

	// Verify file ID
	if stream.FileID() != fileID {
		t.Errorf("FileID = %v, want %v", stream.FileID(), fileID)
	}

	// Should be able to find by that ID
	file, err := bucket.FindOne(fileID)
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	if file.Filename != "upload_with_id.txt" {
		t.Errorf("Filename = %q, want upload_with_id.txt", file.Filename)
	}
}

// TestGridFSBucket_DropNotFound tests dropping a bucket when files don't exist
func TestGridFSBucket_DropNotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, err := db.OpenBucket(nil)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}

	// Drop on empty bucket should not fail
	if err := bucket.Drop(); err != nil {
		t.Errorf("Drop on empty bucket: %v", err)
	}
}

// TestGridFSBucket_UploadFromStreamError tests UploadFromStream with error reader
func TestGridFSBucket_UploadFromStreamError(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, err := db.OpenBucket(nil)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	// Use a reader that returns an error
	errorReader := &errorReader{err: fmt.Errorf("read error")}

	_, err = bucket.UploadFromStream("error_file.txt", errorReader)
	if err == nil {
		t.Error("expected error from UploadFromStream with failing reader")
	}
}

// errorReader is a test helper that always returns an error
type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (int, error) {
	return 0, r.err
}

// TestGetString tests the getString helper function
func TestGetString(t *testing.T) {
	m := map[string]interface{}{
		"key": "value",
		"int": 123,
	}

	if got := getString(m, "key"); got != "value" {
		t.Errorf("getString(m, 'key') = %q, want 'value'", got)
	}

	if got := getString(m, "int"); got != "123" {
		t.Errorf("getString(m, 'int') = %q, want '123'", got)
	}

	if got := getString(m, "missing"); got != "" {
		t.Errorf("getString(m, 'missing') = %q, want empty string", got)
	}
}

// TestGetInt64 tests the getInt64 helper function
func TestGetInt64(t *testing.T) {
	m := map[string]interface{}{
		"int":    int(42),
		"int32":  int32(100),
		"int64":  int64(999),
		"float":  float64(123.45),
		"string": "not a number",
	}

	if got := getInt64(m, "int"); got != 42 {
		t.Errorf("getInt64(m, 'int') = %d, want 42", got)
	}

	if got := getInt64(m, "int32"); got != 100 {
		t.Errorf("getInt64(m, 'int32') = %d, want 100", got)
	}

	if got := getInt64(m, "int64"); got != 999 {
		t.Errorf("getInt64(m, 'int64') = %d, want 999", got)
	}

	if got := getInt64(m, "float"); got != 123 {
		t.Errorf("getInt64(m, 'float') = %d, want 123", got)
	}

	// String value returns 0 (falls through default case)
	if got := getInt64(m, "string"); got != 0 {
		t.Errorf("getInt64(m, 'string') = %d, want 0", got)
	}

	if got := getInt64(m, "missing"); got != 0 {
		t.Errorf("getInt64(m, 'missing') = %d, want 0", got)
	}
}

// TestGridFSDownloadStream_LoadChunkNotFound tests loading a missing chunk
func TestGridFSDownloadStream_LoadChunkNotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, err := db.OpenBucket(nil)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	// Upload a file
	data := []byte("test data for chunk test")
	stream, _ := bucket.OpenUploadStream("chunk_test.txt")
	stream.Write(data)
	stream.Close()

	// Open download stream
	download, err := bucket.OpenDownloadStreamByName("chunk_test.txt")
	if err != nil {
		t.Fatalf("OpenDownloadStreamByName: %v", err)
	}
	defer download.Close()

	// Manually try to load a non-existent chunk (chunk index 999)
	// This tests the error path in loadChunk
	if err := download.loadChunk(999); err == nil {
		t.Error("expected error when loading non-existent chunk")
	}
}

// TestGridFSBucket_DeleteNotFound tests deleting a non-existent file
func TestGridFSBucket_DeleteNotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, err := db.OpenBucket(nil)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	// Delete non-existent file should not fail (no chunks to delete)
	err = bucket.Delete("non-existent-id")
	if err != nil {
		t.Errorf("Delete non-existent file: %v", err)
	}
}

// TestGridFSBucket_DeleteByNameEmpty tests deleting by name when no files match
func TestGridFSBucket_DeleteByNameEmpty(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, err := db.OpenBucket(nil)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	// Delete by name with no matches should not fail
	err = bucket.DeleteByName("non-existent-file.txt")
	if err != nil {
		t.Errorf("DeleteByName non-existent file: %v", err)
	}
}

// TestGridFSBucket_OpenDownloadStreamByNameNotFound tests opening non-existent file
func TestGridFSBucket_OpenDownloadStreamByNameNotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, err := db.OpenBucket(nil)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	_, err = bucket.OpenDownloadStreamByName("non-existent-file.txt")
	if err == nil {
		t.Error("expected error when opening non-existent file")
	}
}

// TestGridFSBucket_FindWithFilter tests the Find function with various filters
func TestGridFSBucket_FindWithFilter(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, err := db.OpenBucket(nil)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	// Upload multiple files
	for i := 0; i < 3; i++ {
		stream, _ := bucket.OpenUploadStream(fmt.Sprintf("find_test_%d.txt", i))
		stream.Write([]byte(fmt.Sprintf("content %d", i)))
		stream.Close()
	}

	// Find all files with prefix
	files, err := bucket.Find(map[string]interface{}{"filename": map[string]interface{}{"$regex": "^find_test_"}})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	if len(files) != 3 {
		t.Errorf("Find returned %d files, want 3", len(files))
	}

	// Find with empty filter should return all files
	allFiles, err := bucket.Find(map[string]interface{}{})
	if err != nil {
		t.Fatalf("Find all: %v", err)
	}

	if len(allFiles) < 3 {
		t.Errorf("Find all returned %d files, want at least 3", len(allFiles))
	}
}
