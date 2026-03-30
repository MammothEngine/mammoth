package manifest

import (
	"testing"
)

func TestVersionAddRemoveFiles(t *testing.T) {
	v := NewVersion()

	v.AddFile(0, FileMetadata{FileNum: 1, Size: 100, SmallestKey: []byte("a"), LargestKey: []byte("b")})
	v.AddFile(0, FileMetadata{FileNum: 2, Size: 200, SmallestKey: []byte("c"), LargestKey: []byte("d")})

	if v.NumFiles(0) != 2 {
		t.Fatalf("expected 2 files, got %d", v.NumFiles(0))
	}
	if v.TotalSize() != 300 {
		t.Fatalf("expected total size 300, got %d", v.TotalSize())
	}

	v.RemoveFile(0, 1)
	if v.NumFiles(0) != 1 {
		t.Fatalf("expected 1 file, got %d", v.NumFiles(0))
	}
}

func TestVersionClone(t *testing.T) {
	v := NewVersion()
	v.AddFile(0, FileMetadata{FileNum: 1, Size: 100})

	clone := v.Clone()
	clone.RemoveFile(0, 1)

	if v.NumFiles(0) != 1 {
		t.Fatal("original should not be affected by clone modification")
	}
}

func TestVersionInvalidLevel(t *testing.T) {
	v := NewVersion()
	v.AddFile(-1, FileMetadata{FileNum: 1})
	v.AddFile(7, FileMetadata{FileNum: 1})

	if v.NumFiles(-1) != 0 || v.NumFiles(7) != 0 {
		t.Fatal("invalid levels should return 0 files")
	}
}

func TestManifestLogEdit(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	err = m.LogEdit(ManifestEdit{
		Type:        EditAddFile,
		Level:       0,
		FileNum:     1,
		FileSize:    1024,
		SmallestKey: []byte("a"),
		LargestKey:  []byte("z"),
	})
	if err != nil {
		t.Fatalf("log edit: %v", err)
	}

	v := m.CurrentVersion()
	if v.NumFiles(0) != 1 {
		t.Fatalf("expected 1 file, got %d", v.NumFiles(0))
	}

	m.Close()
}

func TestManifestRecovery(t *testing.T) {
	dir := t.TempDir()

	// Write some edits
	m, _ := Open(dir)
	m.LogEdit(ManifestEdit{Type: EditAddFile, Level: 0, FileNum: 1, FileSize: 100, SmallestKey: []byte("a"), LargestKey: []byte("b")})
	m.LogEdit(ManifestEdit{Type: EditAddFile, Level: 0, FileNum: 2, FileSize: 200, SmallestKey: []byte("c"), LargestKey: []byte("d")})
	m.LogEdit(ManifestEdit{Type: EditAddFile, Level: 1, FileNum: 3, FileSize: 300, SmallestKey: []byte("e"), LargestKey: []byte("f")})
	m.Close()

	// Reopen and verify
	m2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	v := m2.CurrentVersion()

	if v.NumFiles(0) != 2 {
		t.Fatalf("expected 2 files at L0, got %d", v.NumFiles(0))
	}
	if v.NumFiles(1) != 1 {
		t.Fatalf("expected 1 file at L1, got %d", v.NumFiles(1))
	}
	m2.Close()
}

func TestManifestBatchEdits(t *testing.T) {
	dir := t.TempDir()
	m, _ := Open(dir)

	edits := []ManifestEdit{
		{Type: EditAddFile, Level: 0, FileNum: 1, SmallestKey: []byte("a"), LargestKey: []byte("b")},
		{Type: EditAddFile, Level: 0, FileNum: 2, SmallestKey: []byte("c"), LargestKey: []byte("d")},
		{Type: EditRemoveFile, Level: 0, FileNum: 1},
	}
	if err := m.LogBatch(edits); err != nil {
		t.Fatalf("batch: %v", err)
	}

	v := m.CurrentVersion()
	if v.NumFiles(0) != 1 {
		t.Fatalf("expected 1 file, got %d", v.NumFiles(0))
	}
	m.Close()
}

func TestManifestRemoveFile(t *testing.T) {
	dir := t.TempDir()
	m, _ := Open(dir)

	m.LogEdit(ManifestEdit{Type: EditAddFile, Level: 0, FileNum: 1, SmallestKey: []byte("a"), LargestKey: []byte("b")})
	m.LogEdit(ManifestEdit{Type: EditRemoveFile, Level: 0, FileNum: 1})

	v := m.CurrentVersion()
	if v.NumFiles(0) != 0 {
		t.Fatalf("expected 0 files, got %d", v.NumFiles(0))
	}
	m.Close()
}
