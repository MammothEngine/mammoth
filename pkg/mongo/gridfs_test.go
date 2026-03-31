package mongo

import (
	"bytes"
	"testing"

	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupGridFS(t *testing.T) (*engine.Engine, *Catalog, *GridFS) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { eng.Close() })

	cat := NewCatalog(eng)
	cat.EnsureCollection("testdb", "fs.files")
	cat.EnsureCollection("testdb", "fs.chunks")

	gfs := NewGridFS("testdb", eng, cat)
	return eng, cat, gfs
}

func TestGridFSUploadDownload(t *testing.T) {
	_, _, gfs := setupGridFS(t)

	content := []byte("Hello, GridFS! This is a test file with some content.")
	id, err := gfs.UploadFile("hello.txt", content)
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if id.IsZero() {
		t.Error("expected non-zero ObjectID")
	}

	downloaded, err := gfs.DownloadFile(id)
	if err != nil {
		t.Fatalf("DownloadFile: %v", err)
	}
	if !bytes.Equal(downloaded, content) {
		t.Errorf("downloaded content mismatch: got %d bytes, want %d bytes", len(downloaded), len(content))
	}
}

func TestGridFSDeleteFile(t *testing.T) {
	_, _, gfs := setupGridFS(t)

	content := []byte("file to be deleted")
	id, err := gfs.UploadFile("deleteme.txt", content)
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}

	if err := gfs.DeleteFile(id); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}

	files, err := gfs.ListFiles()
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	for _, f := range files {
		if f.ID == id {
			t.Error("deleted file should not appear in listing")
		}
	}

	// Downloading a deleted file should fail
	if _, err := gfs.DownloadFile(id); err == nil {
		t.Error("expected error downloading deleted file")
	}
}

func TestGridFSMultipleChunks(t *testing.T) {
	_, _, gfs := setupGridFS(t)

	// Create data larger than the default chunk size (255 KB)
	size := 600 * 1024 // 600 KB
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 251)
	}

	id, err := gfs.UploadFile("bigfile.bin", content)
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}

	downloaded, err := gfs.DownloadFile(id)
	if err != nil {
		t.Fatalf("DownloadFile: %v", err)
	}
	if !bytes.Equal(downloaded, content) {
		t.Errorf("downloaded content mismatch: got %d bytes, want %d bytes", len(downloaded), len(content))
	}

	// Verify metadata
	files, err := gfs.ListFiles()
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	var found bool
	for _, f := range files {
		if f.ID == id {
			found = true
			if f.Filename != "bigfile.bin" {
				t.Errorf("filename = %q, want %q", f.Filename, "bigfile.bin")
			}
			if f.Length != int64(size) {
				t.Errorf("length = %d, want %d", f.Length, size)
			}
			if f.ChunkSize != gfs.chunkSize {
				t.Errorf("chunkSize = %d, want %d", f.ChunkSize, gfs.chunkSize)
			}
			if f.MD5 == "" {
				t.Error("expected non-empty MD5")
			}
		}
	}
	if !found {
		t.Error("uploaded file not found in listing")
	}
}

func TestGridFSListFiles(t *testing.T) {
	_, _, gfs := setupGridFS(t)

	files := []struct {
		name    string
		content []byte
	}{
		{"file1.txt", []byte("content one")},
		{"file2.txt", []byte("content two")},
		{"file3.txt", []byte("content three")},
	}

	ids := make(map[string]bool)
	for _, f := range files {
		id, err := gfs.UploadFile(f.name, f.content)
		if err != nil {
			t.Fatalf("UploadFile(%q): %v", f.name, err)
		}
		ids[id.String()] = true
	}

	listing, err := gfs.ListFiles()
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(listing) != len(files) {
		t.Errorf("ListFiles returned %d entries, want %d", len(listing), len(files))
	}

	names := make(map[string]bool)
	for _, fi := range listing {
		names[fi.Filename] = true
		if !ids[fi.ID.String()] {
			t.Errorf("unexpected file ID %s in listing", fi.ID)
		}
	}
	for _, f := range files {
		if !names[f.name] {
			t.Errorf("file %q not found in listing", f.name)
		}
	}
}
