package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecordRoundTrip(t *testing.T) {
	rec := Record{
		Type:    RecordFull,
		SeqNum:  42,
		Payload: []byte("hello world"),
	}

	data := rec.Encode()
	decoded, consumed, err := DecodeRecord(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if consumed != len(data) {
		t.Fatalf("consumed mismatch: %d vs %d", consumed, len(data))
	}
	if decoded.Type != rec.Type || decoded.SeqNum != rec.SeqNum {
		t.Fatalf("type/seq mismatch")
	}
	if string(decoded.Payload) != string(rec.Payload) {
		t.Fatalf("payload mismatch: %s vs %s", decoded.Payload, rec.Payload)
	}
}

func TestRecordCRCValidation(t *testing.T) {
	rec := Record{Type: RecordFull, SeqNum: 1, Payload: []byte("test")}
	data := rec.Encode()

	// Corrupt payload
	data[13] ^= 0xFF
	_, _, err := DecodeRecord(data)
	if err == nil {
		t.Fatal("expected CRC error")
	}
}

func TestSegmentWriteRead(t *testing.T) {
	dir := t.TempDir()
	seg, err := CreateSegment(dir, 1)
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}

	recs := []Record{
		{Type: RecordFull, SeqNum: 1, Payload: []byte("first")},
		{Type: RecordFull, SeqNum: 2, Payload: []byte("second")},
		{Type: RecordFull, SeqNum: 3, Payload: []byte("third")},
	}

	for _, r := range recs {
		if err := seg.WriteRecord(&r, false); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	seg.Close()

	// Read back
	readRecs, err := ReadRecords(seg.Path())
	if err != nil {
		t.Fatalf("read records: %v", err)
	}
	if len(readRecs) != 3 {
		t.Fatalf("expected 3 records, got %d", len(readRecs))
	}
	for i, r := range readRecs {
		if r.SeqNum != recs[i].SeqNum {
			t.Fatalf("record %d: seq mismatch %d vs %d", i, r.SeqNum, recs[i].SeqNum)
		}
		if string(r.Payload) != string(recs[i].Payload) {
			t.Fatalf("record %d: payload mismatch", i)
		}
	}
}

func TestWALWriteRead(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	for i := 0; i < 100; i++ {
		_, err := w.WALWrite([]byte{byte(i)})
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	recs, err := Replay(dir)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(recs) != 100 {
		t.Fatalf("expected 100 records, got %d", len(recs))
	}
}

func TestWAL10KRecords(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 10000; i++ {
		payload := make([]byte, 50)
		payload[0] = byte(i >> 8)
		payload[1] = byte(i)
		_, err := w.WALWrite(payload)
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	w.Close()

	recs, err := Replay(dir)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(recs) != 10000 {
		t.Fatalf("expected 10000, got %d", len(recs))
	}
}

func TestWALCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Write records
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 50; i++ {
		w.WALWrite([]byte("data"))
	}
	w.Close()

	// Reopen and verify
	w2, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if w2.SeqNum() < 50 {
		t.Fatalf("expected seq >= 50, got %d", w2.SeqNum())
	}
	w2.Close()

	recs, _ := Replay(dir)
	if len(recs) < 50 {
		t.Fatalf("expected >= 50 records, got %d", len(recs))
	}
}

func TestWALBatchWrite(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	batch := [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
	}
	baseSeq, err := w.WriteBatch(batch)
	if err != nil {
		t.Fatalf("batch write: %v", err)
	}

	w.Close()

	recs, _ := Replay(dir)
	if len(recs) != 3 {
		t.Fatalf("expected 3, got %d", len(recs))
	}
	if recs[0].SeqNum != baseSeq {
		t.Fatalf("seq mismatch: %d vs %d", recs[0].SeqNum, baseSeq)
	}
}

func TestWALSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = 1024 // Small for testing

	w, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 200; i++ {
		w.WALWrite(make([]byte, 50))
	}
	w.Close()

	// Should have multiple segments
	paths, _ := ListSegments(dir)
	if len(paths) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(paths))
	}

	// All records should be recoverable
	recs, _ := Replay(dir)
	if len(recs) != 200 {
		t.Fatalf("expected 200 records, got %d", len(recs))
	}
}

func TestCorruptionHandling(t *testing.T) {
	dir := t.TempDir()
	seg, _ := CreateSegment(dir, 1)

	rec1 := Record{Type: RecordFull, SeqNum: 1, Payload: []byte("good")}
	rec2 := Record{Type: RecordFull, SeqNum: 2, Payload: []byte("will_be_corrupted")}
	rec3 := Record{Type: RecordFull, SeqNum: 3, Payload: []byte("also_lost")}

	seg.WriteRecord(&rec1, false)
	seg.WriteRecord(&rec2, false)
	seg.WriteRecord(&rec3, false)
	seg.Close()

	// Corrupt the second record
	data, _ := os.ReadFile(seg.Path())
	tmpRec := Record{Type: RecordFull, SeqNum: 1, Payload: []byte("good")}
	offset := len(tmpRec.Encode())
	if offset+13 < len(data) {
		data[offset+13] ^= 0xFF // Flip a byte in the payload
	}
	os.WriteFile(seg.Path(), data, 0644)

	recs, _ := ReadRecords(seg.Path())
	// First record should be fine, second and possibly third should be lost
	if len(recs) < 1 {
		t.Fatal("expected at least 1 valid record")
	}
	if recs[0].SeqNum != 1 {
		t.Fatalf("first record seq mismatch: %d", recs[0].SeqNum)
	}
}

func TestListSegments(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"wal_000001.log", "wal_000003.log", "wal_000002.log"} {
		f, _ := os.Create(filepath.Join(dir, name))
		f.Close()
	}

	paths, err := ListSegments(dir)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(paths) != 3 {
		t.Fatalf("expected 3, got %d", len(paths))
	}
	// Should be sorted
	if filepath.Base(paths[0]) != "wal_000001.log" {
		t.Fatalf("wrong order: %v", paths)
	}
}
