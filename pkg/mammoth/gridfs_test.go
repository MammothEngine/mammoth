package mammoth

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestGridFSBucket_OpenBucket(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Open default bucket
	bucket, err := db.OpenBucket(nil)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	if bucket == nil {
		t.Fatal("expected non-nil bucket")
	}
	if bucket.name != "fs" {
		t.Errorf("expected default name 'fs', got '%s'", bucket.name)
	}
	if bucket.chunkSize != DefaultChunkSize {
		t.Errorf("expected default chunk size %d, got %d", DefaultChunkSize, bucket.chunkSize)
	}
}

func TestGridFSBucket_OpenBucket_CustomOptions(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	opts := &BucketOptions{
		Name:           "custom",
		ChunkSizeBytes: 1024 * 1024, // 1MB
	}

	bucket, err := db.OpenBucket(opts)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	if bucket.name != "custom" {
		t.Errorf("expected name 'custom', got '%s'", bucket.name)
	}
	if bucket.chunkSize != 1024*1024 {
		t.Errorf("expected chunk size 1MB, got %d", bucket.chunkSize)
	}
}

func TestGridFSBucket_InvalidChunkSize(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Too small
	opts := &BucketOptions{ChunkSizeBytes: 500}
	_, err := db.OpenBucket(opts)
	if err == nil {
		t.Error("expected error for chunk size too small")
	}

	// Too large
	opts = &BucketOptions{ChunkSizeBytes: 20 * 1024 * 1024}
	_, err = db.OpenBucket(opts)
	if err == nil {
		t.Error("expected error for chunk size too large")
	}
}

func TestGridFSUploadStream_Write(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	stream, err := bucket.OpenUploadStream("test.txt")
	if err != nil {
		t.Fatalf("OpenUploadStream: %v", err)
	}

	data := []byte("Hello, GridFS World!")
	n, err := stream.Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected to write %d bytes, wrote %d", len(data), n)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify file was created
	fileID := stream.FileID()
	if fileID == nil {
		t.Error("expected non-nil file ID")
	}
}

func TestGridFSUploadStream_MultipleChunks(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Use small chunk size to force multiple chunks (but above minimum)
	opts := &BucketOptions{ChunkSizeBytes: 1024}
	bucket, err := db.OpenBucket(opts)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	stream, _ := bucket.OpenUploadStream("large.txt")

	// Write data larger than chunk size
	data := make([]byte, 250)
	for i := range data {
		data[i] = byte(i % 256)
	}

	n, err := stream.Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected to write %d bytes, wrote %d", len(data), n)
	}

	stream.Close()

	// Verify file length
	file, err := bucket.FindOne(stream.FileID())
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	if file.Length != int64(len(data)) {
		t.Errorf("expected length %d, got %d", len(data), file.Length)
	}
}

func TestGridFSDownloadStream_Read(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload a file
	originalData := []byte("Hello, GridFS World!")
	stream, _ := bucket.OpenUploadStream("test.txt")
	stream.Write(originalData)
	stream.Close()
	fileID := stream.FileID()

	// Download the file
	downloadStream, err := bucket.OpenDownloadStream(fileID)
	if err != nil {
		t.Fatalf("OpenDownloadStream: %v", err)
	}
	defer downloadStream.Close()

	// Read all data
	downloadedData, err := io.ReadAll(downloadStream)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if !bytes.Equal(originalData, downloadedData) {
		t.Errorf("downloaded data doesn't match original")
	}
}

func TestGridFSDownloadStream_ReadChunks(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	opts := &BucketOptions{ChunkSizeBytes: 1024}
	bucket, _ := db.OpenBucket(opts)
	defer bucket.Drop()

	// Upload multi-chunk file
	originalData := make([]byte, 250)
	for i := range originalData {
		originalData[i] = byte(i % 256)
	}

	stream, _ := bucket.OpenUploadStream("large.bin")
	stream.Write(originalData)
	stream.Close()
	fileID := stream.FileID()

	// Download and verify
	downloadStream, _ := bucket.OpenDownloadStream(fileID)
	defer downloadStream.Close()

	downloadedData, _ := io.ReadAll(downloadStream)

	if !bytes.Equal(originalData, downloadedData) {
		t.Error("downloaded data doesn't match original")
	}
}

func TestGridFSDownloadStream_Seek(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload file
	originalData := []byte("0123456789ABCDEF")
	stream, _ := bucket.OpenUploadStream("seekable.txt")
	stream.Write(originalData)
	stream.Close()
	fileID := stream.FileID()

	// Open download stream
	ds, _ := bucket.OpenDownloadStream(fileID)
	defer ds.Close()

	// Seek to middle
	pos, err := ds.Seek(8, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if pos != 8 {
		t.Errorf("expected position 8, got %d", pos)
	}

	// Read from middle
	buf := make([]byte, 4)
	n, _ := ds.Read(buf)
	if string(buf[:n]) != "89AB" {
		t.Errorf("expected '89AB', got '%s'", string(buf[:n]))
	}

	// Seek from current
	pos, _ = ds.Seek(2, io.SeekCurrent)
	if pos != 14 {
		t.Errorf("expected position 14, got %d", pos)
	}

	// Seek from end
	pos, _ = ds.Seek(-4, io.SeekEnd)
	if pos != 12 {
		t.Errorf("expected position 12, got %d", pos)
	}
}

func TestGridFSBucket_UploadFromStream(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload from reader
	data := strings.NewReader("Hello from reader!")
	fileID, err := bucket.UploadFromStream("from_reader.txt", data)
	if err != nil {
		t.Fatalf("UploadFromStream: %v", err)
	}

	if fileID == nil {
		t.Error("expected non-nil file ID")
	}

	// Verify upload
	file, err := bucket.FindOne(fileID)
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	if file.Length != 18 {
		t.Errorf("expected length 18, got %d", file.Length)
	}
}

func TestGridFSBucket_DownloadToStream(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload
	stream, _ := bucket.OpenUploadStream("download_test.txt")
	stream.Write([]byte("Download test content"))
	stream.Close()
	fileID := stream.FileID()

	// Download to writer
	var buf bytes.Buffer
	n, err := bucket.DownloadToStream(fileID, &buf)
	if err != nil {
		t.Fatalf("DownloadToStream: %v", err)
	}

	if n != 21 {
		t.Errorf("expected 21 bytes, got %d", n)
	}
	if buf.String() != "Download test content" {
		t.Errorf("unexpected content: %s", buf.String())
	}
}

func TestGridFSBucket_FindOne(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload with metadata
	opts := UploadOptions{
		Metadata:    map[string]interface{}{"author": "test", "version": 1},
		ContentType: "text/plain",
		Aliases:     []string{"alias1", "alias2"},
	}
	stream, _ := bucket.OpenUploadStream("metadata_test.txt", opts)
	stream.Write([]byte("test"))
	stream.Close()
	fileID := stream.FileID()

	// Find the file
	file, err := bucket.FindOne(fileID)
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}

	if file.Filename != "metadata_test.txt" {
		t.Errorf("expected filename 'metadata_test.txt', got '%s'", file.Filename)
	}
	if file.ContentType != "text/plain" {
		t.Errorf("expected content type 'text/plain', got '%s'", file.ContentType)
	}
	if len(file.Aliases) != 2 {
		t.Errorf("expected 2 aliases, got %d", len(file.Aliases))
	}
	if file.Metadata == nil {
		t.Error("expected metadata")
	} else if file.Metadata["author"] != "test" {
		t.Errorf("expected author='test', got '%v'", file.Metadata["author"])
	}
}

func TestGridFSBucket_FindOneByName(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload multiple files with same name
	stream1, _ := bucket.OpenUploadStream("duplicate.txt")
	stream1.Write([]byte("version 1"))
	stream1.Close()

	stream2, _ := bucket.OpenUploadStream("duplicate.txt")
	stream2.Write([]byte("version 2"))
	stream2.Close()

	// Find by name should return most recent
	file, err := bucket.FindOneByName("duplicate.txt")
	if err != nil {
		t.Fatalf("FindOneByName: %v", err)
	}

	if file.Length != 9 {
		t.Errorf("expected length 9 (version 2), got %d", file.Length)
	}
}

func TestGridFSBucket_Find(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload multiple files
	for i := 0; i < 3; i++ {
		stream, _ := bucket.OpenUploadStream(fmt.Sprintf("file%d.txt", i))
		stream.Write([]byte(fmt.Sprintf("content %d", i)))
		stream.Close()
	}

	// Find all
	files, err := bucket.Find(nil)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	if len(files) != 3 {
		t.Errorf("expected 3 files, got %d", len(files))
	}
}

func TestGridFSBucket_Delete(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload
	stream, _ := bucket.OpenUploadStream("to_delete.txt")
	stream.Write([]byte("delete me"))
	stream.Close()
	fileID := stream.FileID()

	// Verify exists
	_, err := bucket.FindOne(fileID)
	if err != nil {
		t.Fatalf("FindOne before delete: %v", err)
	}

	// Delete
	if err := bucket.Delete(fileID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify deleted
	_, err = bucket.FindOne(fileID)
	if err != ErrFileNotFound {
		t.Errorf("expected ErrFileNotFound after delete, got: %v", err)
	}
}

func TestGridFSBucket_Rename(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload
	stream, _ := bucket.OpenUploadStream("old_name.txt")
	stream.Write([]byte("content"))
	stream.Close()
	fileID := stream.FileID()

	// Rename
	if err := bucket.Rename(fileID, "new_name.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Verify
	file, _ := bucket.FindOne(fileID)
	if file.Filename != "new_name.txt" {
		t.Errorf("expected new filename 'new_name.txt', got '%s'", file.Filename)
	}
}

func TestGridFSUploadStream_Abort(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Use valid small chunk size
	opts := &BucketOptions{ChunkSizeBytes: 1024}
	bucket, err := db.OpenBucket(opts)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Drop()

	// Start upload
	stream, _ := bucket.OpenUploadStream("aborted.txt")
	stream.Write([]byte("This is some data that will be aborted"))

	// Abort
	if err := stream.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	// Close should fail after abort
	if err := stream.Close(); err == nil {
		t.Error("expected error when closing aborted stream")
	}

	// File should not exist
	_, err = bucket.FindOne(stream.FileID())
	if err != ErrFileNotFound {
		t.Error("aborted file should not exist")
	}
}

func TestGridFSUploadStream_DoubleClose(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	stream, _ := bucket.OpenUploadStream("double_close.txt")
	stream.Write([]byte("test"))

	// First close should succeed
	if err := stream.Close(); err != nil {
		t.Fatalf("First Close: %v", err)
	}

	// Second close should be safe
	if err := stream.Close(); err != nil {
		t.Errorf("Second Close should be safe: %v", err)
	}
}

func TestGridFSDownloadStream_FileNotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Try to open non-existent file
	fakeID := bson.NewObjectID()
	_, err := bucket.OpenDownloadStream(fakeID)
	if err != ErrFileNotFound {
		t.Errorf("expected ErrFileNotFound, got: %v", err)
	}
}

func TestGridFSBucket_Drop(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(&BucketOptions{Name: "droppable"})

	// Upload a file
	stream, _ := bucket.OpenUploadStream("test.txt")
	stream.Write([]byte("test"))
	stream.Close()

	// Drop bucket
	if err := bucket.Drop(); err != nil {
		t.Fatalf("Drop: %v", err)
	}

	// Verify collections are gone
	collections, _ := db.ListCollections()
	for _, name := range collections {
		if name == "droppable.files" || name == "droppable.chunks" {
			t.Error("bucket collections should be dropped")
		}
	}
}

func TestGridFSBucket_UploadWithCustomID(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload with custom ID
	customID := "my-custom-id-123"
	stream, err := bucket.OpenUploadStreamWithID(customID, "custom_id.txt")
	if err != nil {
		t.Fatalf("OpenUploadStreamWithID: %v", err)
	}
	stream.Write([]byte("content"))
	stream.Close()

	// Verify we can find by custom ID
	file, err := bucket.FindOne(customID)
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	if file.ID != customID {
		t.Errorf("expected ID '%s', got '%v'", customID, file.ID)
	}
}

func TestGridFS_MD5Verification(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload
	content := []byte("content to hash")
	stream, _ := bucket.OpenUploadStream("hash_test.txt")
	stream.Write(content)
	stream.Close()
	fileID := stream.FileID()

	// Verify MD5 is set
	file, _ := bucket.FindOne(fileID)
	if file.MD5 == "" {
		t.Error("expected MD5 to be set")
	}
}

func TestGridFS_EmptyFile(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload empty file
	stream, _ := bucket.OpenUploadStream("empty.txt")
	stream.Close()
	fileID := stream.FileID()

	// Download
	ds, _ := bucket.OpenDownloadStream(fileID)
	defer ds.Close()

	data, _ := io.ReadAll(ds)
	if len(data) != 0 {
		t.Errorf("expected empty file, got %d bytes", len(data))
	}

	// Verify file metadata
	file, _ := bucket.FindOne(fileID)
	if file.Length != 0 {
		t.Errorf("expected length 0, got %d", file.Length)
	}
}

func TestGridFS_LargeFile(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Create 1MB file
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	stream, _ := bucket.OpenUploadStream("large.bin")
	n, err := stream.Write(largeData)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(largeData) {
		t.Errorf("expected to write %d bytes, wrote %d", len(largeData), n)
	}
	stream.Close()

	// Verify
	file, _ := bucket.FindOne(stream.FileID())
	if file.Length != int64(len(largeData)) {
		t.Errorf("expected length %d, got %d", len(largeData), file.Length)
	}
}

func TestGridFS_SeekNegative(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload
	stream, _ := bucket.OpenUploadStream("seek_test.txt")
	stream.Write([]byte("0123456789"))
	stream.Close()
	fileID := stream.FileID()

	// Try to seek to negative position
	ds, _ := bucket.OpenDownloadStream(fileID)
	defer ds.Close()

	_, err := ds.Seek(-100, io.SeekStart)
	if err == nil {
		t.Error("expected error for negative seek")
	}
}

// Tests for uncovered functions

func TestGridFSBucket_DeleteByName(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload a file
	stream, _ := bucket.OpenUploadStream("to_delete.txt")
	stream.Write([]byte("delete me"))
	stream.Close()

	// Verify exists
	_, err := bucket.FindOneByName("to_delete.txt")
	if err != nil {
		t.Fatalf("FindOneByName before delete: %v", err)
	}

	// Delete by name
	if err := bucket.DeleteByName("to_delete.txt"); err != nil {
		t.Fatalf("DeleteByName: %v", err)
	}

	// Verify deleted
	_, err = bucket.FindOneByName("to_delete.txt")
	if err != ErrFileNotFound {
		t.Errorf("expected ErrFileNotFound after delete, got: %v", err)
	}
}

func TestGridFSBucket_OpenDownloadStreamByName(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload a file
	originalData := []byte("Hello from OpenDownloadStreamByName!")
	stream, _ := bucket.OpenUploadStream("by_name.txt")
	stream.Write(originalData)
	stream.Close()

	// Open download stream by name
	ds, err := bucket.OpenDownloadStreamByName("by_name.txt")
	if err != nil {
		t.Fatalf("OpenDownloadStreamByName: %v", err)
	}
	defer ds.Close()

	// Read and verify
	data, _ := io.ReadAll(ds)
	if !bytes.Equal(originalData, data) {
		t.Error("downloaded data doesn't match original")
	}
}

func TestGridFSUploadStream_SetTransaction(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Start a transaction
	tx, err := db.StartTransaction()
	if err != nil {
		t.Fatalf("StartTransaction: %v", err)
	}
	defer tx.Rollback()

	// Open upload stream
	stream, err := bucket.OpenUploadStream("tx_test.txt")
	if err != nil {
		t.Fatalf("OpenUploadStream: %v", err)
	}

	// Set transaction on upload stream
	stream.SetTransaction(tx)

	// Write and close should use the transaction
	_, err = stream.Write([]byte("transaction test data"))
	if err != nil {
		t.Errorf("Write with transaction: %v", err)
	}

	// Close without committing
	stream.Close()
}

func TestGridFSDownloadStream_GetFile(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	bucket, _ := db.OpenBucket(nil)
	defer bucket.Drop()

	// Upload a file
	stream, _ := bucket.OpenUploadStream("getfile_test.txt")
	stream.Write([]byte("test data for GetFile"))
	stream.Close()

	// Find the file
	file, err := bucket.FindOneByName("getfile_test.txt")
	if err != nil {
		t.Fatalf("FindOneByName: %v", err)
	}

	// Open download stream
	ds, err := bucket.OpenDownloadStream(file.ID)
	if err != nil {
		t.Fatalf("OpenDownloadStream: %v", err)
	}
	defer ds.Close()

	// GetFile should return the file info
	gotFile := ds.GetFile()
	if gotFile == nil {
		t.Fatal("GetFile returned nil")
	}
	if gotFile.ID != file.ID {
		t.Errorf("GetFile().ID = %v, want %v", gotFile.ID, file.ID)
	}
	if gotFile.Filename != "getfile_test.txt" {
		t.Errorf("GetFile().Filename = %q, want %q", gotFile.Filename, "getfile_test.txt")
	}
}
