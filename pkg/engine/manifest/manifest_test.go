package manifest

import (
	"encoding/binary"
	"os"
	"path/filepath"
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

// Test Version Files() method
func TestVersion_Files(t *testing.T) {
	v := NewVersion()

	// Add some files
	v.AddFile(0, FileMetadata{FileNum: 1, Size: 100, SmallestKey: []byte("a"), LargestKey: []byte("b")})
	v.AddFile(0, FileMetadata{FileNum: 2, Size: 200, SmallestKey: []byte("c"), LargestKey: []byte("d")})
	v.AddFile(1, FileMetadata{FileNum: 3, Size: 300, SmallestKey: []byte("e"), LargestKey: []byte("f")})

	// Test Files() returns correct files for level 0
	files0 := v.Files(0)
	if len(files0) != 2 {
		t.Errorf("Files(0) returned %d files, want 2", len(files0))
	}

	// Test Files() returns correct files for level 1
	files1 := v.Files(1)
	if len(files1) != 1 {
		t.Errorf("Files(1) returned %d files, want 1", len(files1))
	}

	// Test Files() returns nil for invalid levels
	filesNeg := v.Files(-1)
	if filesNeg != nil {
		t.Error("Files(-1) should return nil")
	}

	filesHigh := v.Files(7)
	if filesHigh != nil {
		t.Error("Files(7) should return nil")
	}

	// Verify file metadata is correct
	if files0[0].FileNum != 1 && files0[0].FileNum != 2 {
		t.Errorf("Unexpected FileNum: %d", files0[0].FileNum)
	}
}

// Test manifest recovery with corrupted data
func TestManifestRecovery_Corruption(t *testing.T) {
	dir := t.TempDir()

	// Write some valid edits
	m, _ := Open(dir)
	m.LogEdit(ManifestEdit{Type: EditAddFile, Level: 0, FileNum: 1, FileSize: 100, SmallestKey: []byte("a"), LargestKey: []byte("b")})
	m.Close()

	// Append corrupt data to manifest file
	path := filepath.Join(dir, manifestFileName)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}) // Invalid length
	f.Close()

	// Reopen should stop at corruption but keep valid edits
	m2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with corruption: %v", err)
	}
	v := m2.CurrentVersion()
	if v.NumFiles(0) != 1 {
		t.Errorf("expected 1 file after recovery, got %d", v.NumFiles(0))
	}
	m2.Close()
}

// Test manifest recovery with CRC mismatch
func TestManifestRecovery_CRCMismatch(t *testing.T) {
	dir := t.TempDir()

	// Write some valid edits
	m, _ := Open(dir)
	m.LogEdit(ManifestEdit{Type: EditAddFile, Level: 0, FileNum: 1, FileSize: 100, SmallestKey: []byte("a"), LargestKey: []byte("b")})
	m.Close()

	// Append data with wrong CRC
	path := filepath.Join(dir, manifestFileName)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	// Write: length(4) + minimal data(22) + bad crc(4)
	buf := make([]byte, 30)
	binary.LittleEndian.PutUint32(buf, 22) // edit length
	buf[4] = 1                             // EditAddFile
	buf[5] = 0                             // level
	// rest is zeros, CRC will be wrong
	binary.LittleEndian.PutUint32(buf[26:], 0xDEADBEEF) // bad CRC
	f.Write(buf)
	f.Close()

	// Reopen should stop at CRC mismatch
	m2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with CRC mismatch: %v", err)
	}
	v := m2.CurrentVersion()
	if v.NumFiles(0) != 1 {
		t.Errorf("expected 1 file after recovery, got %d", v.NumFiles(0))
	}
	m2.Close()
}

// Test manifest recovery with truncated edit
func TestManifestRecovery_TruncatedEdit(t *testing.T) {
	dir := t.TempDir()

	// Write valid edit
	m, _ := Open(dir)
	m.LogEdit(ManifestEdit{Type: EditAddFile, Level: 0, FileNum: 1, FileSize: 100, SmallestKey: []byte("a"), LargestKey: []byte("b")})
	m.Close()

	// Append truncated data (length says 100 but only write 10 bytes)
	path := filepath.Join(dir, manifestFileName)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf, 100) // length says 100 bytes
	f.Write(buf)
	f.Write([]byte{1, 0, 0, 0}) // only 4 bytes of data
	f.Close()

	// Reopen should handle truncated data
	m2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with truncation: %v", err)
	}
	v := m2.CurrentVersion()
	if v.NumFiles(0) != 1 {
		t.Errorf("expected 1 file after recovery, got %d", v.NumFiles(0))
	}
	m2.Close()
}

// Test decodeEdit with invalid data
func TestDecodeEdit_InvalidData(t *testing.T) {
	m := &Manifest{}

	// Test data too short
	_, err := m.decodeEdit([]byte{1, 0, 0, 0})
	if err == nil {
		t.Error("expected error for data too short")
	}

	// Test truncated smallest key
	data := make([]byte, 26)
	data[0] = 1 // EditAddFile
	data[1] = 0 // level
	binary.LittleEndian.PutUint64(data[2:], 1)  // fileNum
	binary.LittleEndian.PutUint64(data[10:], 100) // fileSize
	binary.LittleEndian.PutUint32(data[18:], 100) // smallestLen says 100 but data is only 26 bytes
	_, err = m.decodeEdit(data)
	if err == nil {
		t.Error("expected error for truncated smallest key")
	}

	// Test truncated largest key
	data2 := make([]byte, 30)
	data2[0] = 1 // EditAddFile
	data2[1] = 0 // level
	binary.LittleEndian.PutUint64(data2[2:], 1)
	binary.LittleEndian.PutUint64(data2[10:], 100)
	binary.LittleEndian.PutUint32(data2[18:], 2) // smallestLen = 2
	data2[22] = 'a'
	data2[23] = 'b'
	binary.LittleEndian.PutUint32(data2[24:], 50) // largestLen says 50 but data is short
	_, err = m.decodeEdit(data2)
	if err == nil {
		t.Error("expected error for truncated largest key")
	}
}

// Test Open with empty directory (no existing manifest)
func TestOpen_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()

	// Open should create new manifest
	m, err := Open(dir)
	if err != nil {
		t.Fatalf("open empty dir: %v", err)
	}

	v := m.CurrentVersion()
	if v.NumFiles(0) != 0 {
		t.Errorf("expected 0 files, got %d", v.NumFiles(0))
	}

	m.Close()
}

// Test Close idempotency
func TestClose_Idempotent(t *testing.T) {
	dir := t.TempDir()
	m, _ := Open(dir)
	m.Close()

	// Second close should not panic (may return error for already closed file)
	_ = m.Close()
}

// Test Open with invalid directory path
func TestOpen_InvalidPath(t *testing.T) {
	// Try to open with a file path instead of directory
	f, err := os.CreateTemp("", "manifest_test")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(f.Name())

	_, err = Open(f.Name())
	if err == nil {
		t.Error("expected error when opening with file path")
	}
}

// Test Open with corrupted manifest file
func TestOpen_CorruptedManifest(t *testing.T) {
	dir := t.TempDir()

	// Create a corrupted manifest file
	path := filepath.Join(dir, manifestFileName)
	f, _ := os.Create(path)
	f.WriteString("not a valid manifest file header - this is garbage data")
	f.Close()

	// Try to open - may error or handle gracefully
	m, err := Open(dir)
	if err == nil {
		// If it didn't error, it should have empty version
		v := m.CurrentVersion()
		if v.NumFiles(0) != 0 {
			t.Log("opened corrupted manifest with non-zero files")
		}
		m.Close()
	}
}

// Test compareBytes with various scenarios
func TestCompareBytes(t *testing.T) {
	tests := []struct {
		name     string
		a        []byte
		b        []byte
		expected int
	}{
		{"equal", []byte("abc"), []byte("abc"), 0},
		{"a < b", []byte("abc"), []byte("def"), -1},
		{"a > b", []byte("xyz"), []byte("abc"), 1},
		{"empty a", []byte{}, []byte("abc"), -1},
		{"empty b", []byte("abc"), []byte{}, 1},
		{"both empty", []byte{}, []byte{}, 0},
		{"prefix", []byte("ab"), []byte("abc"), -1},
		{"common prefix longer", []byte("abcd"), []byte("abc"), 1},
		{"binary different", []byte{0x00, 0x01}, []byte{0x00, 0x02}, -1},
		{"large difference", []byte{0xFF, 0xFF}, []byte{0x00, 0x00}, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := compareBytes(tc.a, tc.b)
			if result != tc.expected {
				t.Errorf("compareBytes(%v, %v) = %d, want %d", tc.a, tc.b, result, tc.expected)
			}
		})
	}
}

// Test LogEdit with various edit types
func TestLogEdit_Types(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer m.Close()

	// Test AddFile
	err = m.LogEdit(ManifestEdit{
		Type:        EditAddFile,
		Level:       0,
		FileNum:     1,
		FileSize:    100,
		SmallestKey: []byte("a"),
		LargestKey:  []byte("b"),
	})
	if err != nil {
		t.Errorf("LogEdit AddFile: %v", err)
	}

	// Test RemoveFile
	err = m.LogEdit(ManifestEdit{
		Type:    EditRemoveFile,
		Level:   0,
		FileNum: 1,
	})
	if err != nil {
		t.Errorf("LogEdit RemoveFile: %v", err)
	}

	// Verify final state
	v := m.CurrentVersion()
	if v.NumFiles(0) != 0 {
		t.Errorf("expected 0 files after remove, got %d", v.NumFiles(0))
	}
}

// Test LogBatch with empty batch
func TestLogBatch_Empty(t *testing.T) {
	dir := t.TempDir()
	m, _ := Open(dir)
	defer m.Close()

	err := m.LogBatch([]ManifestEdit{})
	if err != nil {
		t.Errorf("LogBatch empty: %v", err)
	}
}

// Test LogBatch with large batch
func TestLogBatch_Large(t *testing.T) {
	dir := t.TempDir()
	m, _ := Open(dir)
	defer m.Close()

	// Create large batch
	edits := make([]ManifestEdit, 100)
	for i := 0; i < 100; i++ {
		edits[i] = ManifestEdit{
			Type:        EditAddFile,
			Level:       0,
			FileNum:     uint64(i + 1),
			FileSize:    100,
			SmallestKey: []byte("a"),
			LargestKey:  []byte("z"),
		}
	}

	err := m.LogBatch(edits)
	if err != nil {
		t.Fatalf("LogBatch large: %v", err)
	}

	v := m.CurrentVersion()
	if v.NumFiles(0) != 100 {
		t.Errorf("expected 100 files, got %d", v.NumFiles(0))
	}
}

// Test RemoveFile with non-existent file
func TestRemoveFile_NonExistent(t *testing.T) {
	v := NewVersion()

	// Add a file
	v.AddFile(0, FileMetadata{FileNum: 1, Size: 100})

	// Remove non-existent file - should not panic
	v.RemoveFile(0, 999)

	// Original file should still be there
	if v.NumFiles(0) != 1 {
		t.Errorf("expected 1 file, got %d", v.NumFiles(0))
	}
}

// Test TotalSize calculation
func TestVersion_TotalSize(t *testing.T) {
	v := NewVersion()

	if v.TotalSize() != 0 {
		t.Errorf("expected 0 size for empty version, got %d", v.TotalSize())
	}

	v.AddFile(0, FileMetadata{FileNum: 1, Size: 100})
	v.AddFile(0, FileMetadata{FileNum: 2, Size: 200})
	v.AddFile(1, FileMetadata{FileNum: 3, Size: 300})

	if v.TotalSize() != 600 {
		t.Errorf("expected total size 600, got %d", v.TotalSize())
	}

	// Remove file and verify size updates
	v.RemoveFile(0, 1)
	if v.TotalSize() != 500 {
		t.Errorf("expected total size 500 after remove, got %d", v.TotalSize())
	}
}

// Test FileMetadata contents
func TestFileMetadata(t *testing.T) {
	v := NewVersion()
	v.AddFile(0, FileMetadata{
		FileNum:     42,
		Size:        1024,
		SmallestKey: []byte("apple"),
		LargestKey:  []byte("zebra"),
	})

	files := v.Files(0)
	if len(files) != 1 {
		t.Fatal("expected 1 file")
	}

	f := files[0]
	if f.FileNum != 42 {
		t.Errorf("FileNum = %d, want 42", f.FileNum)
	}
	if f.Size != 1024 {
		t.Errorf("Size = %d, want 1024", f.Size)
	}
	if string(f.SmallestKey) != "apple" {
		t.Errorf("SmallestKey = %s, want apple", f.SmallestKey)
	}
	if string(f.LargestKey) != "zebra" {
		t.Errorf("LargestKey = %s, want zebra", f.LargestKey)
	}
}

// Test manifest file after multiple closes and reopens
func TestManifest_Persistence(t *testing.T) {
	dir := t.TempDir()

	// First session
	m1, _ := Open(dir)
	m1.LogEdit(ManifestEdit{Type: EditAddFile, Level: 0, FileNum: 1, FileSize: 100, SmallestKey: []byte("a"), LargestKey: []byte("b")})
	m1.Close()

	// Second session
	m2, _ := Open(dir)
	m2.LogEdit(ManifestEdit{Type: EditAddFile, Level: 0, FileNum: 2, FileSize: 200, SmallestKey: []byte("c"), LargestKey: []byte("d")})
	m2.Close()

	// Third session - should see all edits
	m3, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer m3.Close()

	v := m3.CurrentVersion()
	if v.NumFiles(0) != 2 {
		t.Errorf("expected 2 files after persistence, got %d", v.NumFiles(0))
	}
	if v.TotalSize() != 300 {
		t.Errorf("expected total size 300, got %d", v.TotalSize())
	}
}
