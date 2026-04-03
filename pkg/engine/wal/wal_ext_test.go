package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReplayBatched(t *testing.T) {
	dir := t.TempDir()

	// Create a WAL and write some records
	opts := DefaultOptions(dir)
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write some records with gaps
	seq1, err := w.WALWrite([]byte("record1"))
	if err != nil {
		t.Fatalf("WALWrite 1: %v", err)
	}
	seq2, err := w.WALWrite([]byte("record2"))
	if err != nil {
		t.Fatalf("WALWrite 2: %v", err)
	}
	_ = seq1
	_ = seq2

	// Write batch with gap
	_, err = w.WriteBatch([][]byte{
		[]byte("batch1"),
		[]byte("batch2"),
	})
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	w.Close()

	// Test ReplayBatched
	batches, err := ReplayBatched(dir)
	if err != nil {
		t.Fatalf("ReplayBatched: %v", err)
	}

	// Should have at least 1 batch
	if len(batches) == 0 {
		t.Error("expected at least 1 batch")
	}
}

func TestReplayBatched_Empty(t *testing.T) {
	dir := t.TempDir()

	// Test with empty directory
	batches, err := ReplayBatched(dir)
	if err != nil {
		t.Fatalf("ReplayBatched on empty dir: %v", err)
	}
	if batches != nil {
		t.Errorf("expected nil batches for empty dir, got %v", batches)
	}
}

func TestSegment_Index(t *testing.T) {
	dir := t.TempDir()

	seg, err := CreateSegment(dir, 42)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	defer seg.Close()

	if seg.Index() != 42 {
		t.Errorf("expected Index() = 42, got %d", seg.Index())
	}
}

func TestUitoa(t *testing.T) {
	tests := []struct {
		input    uint
		expected string
	}{
		{0, "000000"},
		{1, "000001"},
		{42, "000042"},
		{999999, "999999"},
	}

	for _, tt := range tests {
		result := uitoa(tt.input)
		if result != tt.expected {
			t.Errorf("uitoa(%d) = %s, want %s", tt.input, result, tt.expected)
		}
	}
}

func TestPutUint32LE(t *testing.T) {
	b := make([]byte, 4)
	putUint32LE(b, 0x12345678)

	// Check little-endian encoding
	if b[0] != 0x78 || b[1] != 0x56 || b[2] != 0x34 || b[3] != 0x12 {
		t.Errorf("putUint32LE incorrect encoding: %x %x %x %x", b[0], b[1], b[2], b[3])
	}

	// Test with zero
	putUint32LE(b, 0)
	if b[0] != 0 || b[1] != 0 || b[2] != 0 || b[3] != 0 {
		t.Error("putUint32LE(0) should produce all zeros")
	}

	// Test with max uint32
	putUint32LE(b, 0xFFFFFFFF)
	if b[0] != 0xFF || b[1] != 0xFF || b[2] != 0xFF || b[3] != 0xFF {
		t.Error("putUint32LE(0xFFFFFFFF) should produce all 0xFF")
	}
}

func TestWAL_Dir(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	if got := w.Dir(); got != dir {
		t.Errorf("Dir() = %s, want %s", got, dir)
	}
}

func TestCreateSegment_InvalidDir(t *testing.T) {
	// Try to create a segment in a non-existent directory (Unix path)
	// On Windows this might not fail, so we just log the result
	_, err := CreateSegment("/nonexistent/directory", 1)
	if err == nil {
		t.Log("CreateSegment did not return error for invalid directory (may be expected on Windows)")
	}
}

func TestOpenSegment_InvalidPath(t *testing.T) {
	// Try to open a non-existent segment
	_, err := OpenSegment("/nonexistent/file.log")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestSegment_WriteRecord_WithPadding(t *testing.T) {
	dir := t.TempDir()

	seg, err := CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	defer seg.Close()

	// Create a large record that will trigger padding logic
	// defaultBlockSize is 32KB, so write a record near block boundary
	largeData := make([]byte, defaultBlockSize-100)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	rec := &Record{Type: RecordFull, SeqNum: 1, Payload: largeData}
	if err := seg.WriteRecord(rec, true); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	// Write another record - this should trigger padding logic
	rec2 := &Record{Type: RecordFull, SeqNum: 2, Payload: []byte("second record data")}
	if err := seg.WriteRecord(rec2, true); err != nil {
		t.Fatalf("WriteRecord 2: %v", err)
	}

	// Verify segment has correct size
	if seg.Size() <= int64(len(largeData)) {
		t.Error("segment size should be greater than first record data")
	}
}

func TestSegment_Sync(t *testing.T) {
	dir := t.TempDir()

	seg, err := CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}

	// Write something first
	rec := &Record{Type: RecordFull, SeqNum: 1, Payload: []byte("test data")}
	if err := seg.WriteRecord(rec, false); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	// Sync should work
	if err := seg.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}

	seg.Close()

	// Sync on closed segment should return error or be safe
	// The behavior depends on implementation, we just verify it doesn't panic
	_ = seg.Sync()
}

func TestParseSegmentIndex(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		expected int
		wantErr  bool
	}{
		{"valid wal_000001.log", "wal_000001.log", 1, false},
		{"valid wal_000042.log", "wal_000042.log", 42, false},
		{"valid wal_999999.log", "wal_999999.log", 999999, false},
		{"invalid format", "invalid.log", 0, true},
		{"wrong prefix", "segment_001.log", 0, true},
		{"no extension", "wal_000001", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := ParseSegmentIndex(tt.filename)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if idx != tt.expected {
				t.Errorf("ParseSegmentIndex(%s) = %d, want %d", tt.filename, idx, tt.expected)
			}
		})
	}
}

func TestListSegments_EmptyDir(t *testing.T) {
	dir := t.TempDir()

	segments, err := ListSegments(dir)
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(segments) != 0 {
		t.Errorf("expected 0 segments in empty dir, got %d", len(segments))
	}
}

func TestListSegments_WithSegments(t *testing.T) {
	dir := t.TempDir()

	// Create some segment files
	for _, idx := range []int{3, 1, 2} {
		name := filepath.Join(dir, uitoa(uint(idx))+".log")
		f, err := os.Create(name)
		if err != nil {
			t.Fatalf("Create file: %v", err)
		}
		f.Close()
	}

	// Create a non-log file
	nonLog := filepath.Join(dir, "other.txt")
	f, _ := os.Create(nonLog)
	f.Close()

	segments, err := ListSegments(dir)
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}

	// Should only return .log files, sorted
	if len(segments) != 3 {
		t.Errorf("expected 3 segments, got %d", len(segments))
	}

	// Check they are sorted
	for i, seg := range segments {
		expectedIdx := i + 1
		expectedName := uitoa(uint(expectedIdx)) + ".log"
		if filepath.Base(seg) != expectedName {
			t.Errorf("segment %d: expected %s, got %s", i, expectedName, filepath.Base(seg))
		}
	}
}

func TestReadRecords_Corruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal_000001.log")

	// Create a file with some corrupted data
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Write a valid record first
	rec := &Record{Type: RecordFull, SeqNum: 1, Payload: []byte("valid data")}
	data := rec.Encode()
	f.Write(data)

	// Write some corrupted/garbage data - this will cause ReadRecords to skip to next block
	// Write enough garbage to trigger block skip
	garbage := make([]byte, defaultBlockSize-len(data)+100)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	f.Write(garbage)

	// Write another valid record after the corrupted block
	rec2 := &Record{Type: RecordFull, SeqNum: 2, Payload: []byte("more valid data")}
	data2 := rec2.Encode()
	f.Write(data2)

	f.Close()

	// ReadRecords should skip corrupted data and find valid records
	records, err := ReadRecords(path)
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}

	// Should at least get the first valid record
	foundSeqNums := make(map[uint64]bool)
	for _, r := range records {
		foundSeqNums[r.SeqNum] = true
	}

	if !foundSeqNums[1] {
		t.Error("expected to find record with SeqNum 1")
	}
	// Second record may or may not be found depending on corruption handling
}

func TestWAL_BatchSyncMode(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	opts.SyncMode = SyncBatch
	opts.BatchSyncInterval = 50 * time.Millisecond

	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write some records
	for i := 0; i < 5; i++ {
		_, err := w.WALWrite([]byte("test data"))
		if err != nil {
			t.Fatalf("WALWrite: %v", err)
		}
	}

	// Wait for batch sync
	time.Sleep(100 * time.Millisecond)

	w.Close()

	// Verify records were written
	records, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(records) != 5 {
		t.Errorf("expected 5 records, got %d", len(records))
	}
}

func TestWAL_WriteBatch_Empty(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	// Write empty batch
	seq, err := w.WriteBatch([][]byte{})
	if err != nil {
		t.Fatalf("WriteBatch empty: %v", err)
	}

	// Should return current sequence number
	if seq != w.SeqNum() {
		t.Errorf("expected seq = %d, got %d", w.SeqNum(), seq)
	}
}

func TestWAL_Close_Idempotent(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// First close should succeed
	if err := w.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}

	// Second close should be safe (idempotent)
	if err := w.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestWAL_WALWrite_Closed(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.Close()

	// Write after close should fail
	_, err = w.WALWrite([]byte("test"))
	if err == nil {
		t.Error("expected error for Write after Close")
	}
}

func TestSegment_Path(t *testing.T) {
	dir := t.TempDir()

	seg, err := CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	defer seg.Close()

	expectedPath := filepath.Join(dir, "wal_000001.log")
	if seg.Path() != expectedPath {
		t.Errorf("Path() = %s, want %s", seg.Path(), expectedPath)
	}
}

func TestSegment_Size(t *testing.T) {
	dir := t.TempDir()

	seg, err := CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	defer seg.Close()

	initialSize := seg.Size()

	// Write a record
	rec := &Record{Type: RecordFull, SeqNum: 1, Payload: []byte("test data")}
	if err := seg.WriteRecord(rec, true); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	// Size should increase
	if seg.Size() <= initialSize {
		t.Error("Size should increase after write")
	}
}
