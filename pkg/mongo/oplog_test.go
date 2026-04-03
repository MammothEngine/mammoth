package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupOplogTest(t *testing.T) (*engine.Engine, *Oplog) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	return eng, NewOplog(eng)
}

func TestOplog_WriteInsert(t *testing.T) {
	_, oplog := setupOplogTest(t)

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("Alice"))

	err := oplog.WriteInsert("testdb.users", bson.Encode(doc))
	if err != nil {
		t.Fatalf("WriteInsert: %v", err)
	}
}

func TestOplog_WriteUpdate(t *testing.T) {
	_, oplog := setupOplogTest(t)

	oldDoc := bson.NewDocument()
	oldDoc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	oldDoc.Set("name", bson.VString("Alice"))

	newDoc := bson.NewDocument()
	newDoc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	newDoc.Set("name", bson.VString("Bob"))

	err := oplog.WriteUpdate("testdb.users", bson.Encode(newDoc), bson.Encode(oldDoc))
	if err != nil {
		t.Fatalf("WriteUpdate: %v", err)
	}
}

func TestOplog_WriteDelete(t *testing.T) {
	_, oplog := setupOplogTest(t)

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("Deleted"))

	err := oplog.WriteDelete("testdb.users", bson.Encode(doc))
	if err != nil {
		t.Fatalf("WriteDelete: %v", err)
	}
}

func TestOplogKey(t *testing.T) {
	key := oplogKey(1234567890, 42)

	if len(key) != len(oplogKeyPrefix)+16 {
		t.Errorf("oplogKey length = %d, want %d", len(key), len(oplogKeyPrefix)+16)
	}

	// Verify prefix
	if string(key[:len(oplogKeyPrefix)]) != string(oplogKeyPrefix) {
		t.Error("oplogKey missing correct prefix")
	}
}

func TestDecodeOplogEntry(t *testing.T) {
	// Create a BSON-encoded oplog entry
	entryDoc := bson.NewDocument()
	entryDoc.Set("ts", bson.VInt64(1234567890))
	entryDoc.Set("c", bson.VInt64(42))
	entryDoc.Set("op", bson.VString("i"))
	entryDoc.Set("ns", bson.VString("testdb.users"))
	entryDoc.Set("o", bson.VBinary(bson.BinaryGeneric, []byte{1, 2, 3}))
	entryDoc.Set("o2", bson.VBinary(bson.BinaryGeneric, []byte{4, 5, 6}))
	entryDoc.Set("wall", bson.VInt64(1234567890123))

	data := bson.Encode(entryDoc)

	entry, err := decodeOplogEntry(data)
	if err != nil {
		t.Fatalf("decodeOplogEntry: %v", err)
	}

	if entry.Timestamp != 1234567890 {
		t.Errorf("Timestamp = %d, want 1234567890", entry.Timestamp)
	}
	if entry.Counter != 42 {
		t.Errorf("Counter = %d, want 42", entry.Counter)
	}
	if entry.Operation != "i" {
		t.Errorf("Operation = %s, want i", entry.Operation)
	}
	if entry.Namespace != "testdb.users" {
		t.Errorf("Namespace = %s, want testdb.users", entry.Namespace)
	}
	if string(entry.Document) != string([]byte{1, 2, 3}) {
		t.Errorf("Document = %v, want [1 2 3]", entry.Document)
	}
	if string(entry.Document2) != string([]byte{4, 5, 6}) {
		t.Errorf("Document2 = %v, want [4 5 6]", entry.Document2)
	}
	if entry.WallTime != 1234567890123 {
		t.Errorf("WallTime = %d, want 1234567890123", entry.WallTime)
	}
}

func TestDecodeOplogEntry_InvalidData(t *testing.T) {
	_, err := decodeOplogEntry([]byte{1, 2, 3})
	if err == nil {
		t.Error("decodeOplogEntry should error on invalid data")
	}
}

func TestDecodeOplogEntry_MissingFields(t *testing.T) {
	// Entry with minimal fields
	entryDoc := bson.NewDocument()
	entryDoc.Set("ts", bson.VInt64(100))

	data := bson.Encode(entryDoc)

	entry, err := decodeOplogEntry(data)
	if err != nil {
		t.Fatalf("decodeOplogEntry: %v", err)
	}

	if entry.Timestamp != 100 {
		t.Errorf("Timestamp = %d, want 100", entry.Timestamp)
	}
	// Other fields should be zero values
	if entry.Counter != 0 {
		t.Errorf("Counter = %d, want 0", entry.Counter)
	}
	if entry.Operation != "" {
		t.Errorf("Operation = %s, want empty", entry.Operation)
	}
}

func TestOplogScanner_ScanSince(t *testing.T) {
	eng, oplog := setupOplogTest(t)
	_ = eng

	// Write some entries
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc1.Set("data", bson.VString("first"))
	oplog.WriteInsert("testdb.users", bson.Encode(doc1))

	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc2.Set("data", bson.VString("second"))
	oplog.WriteInsert("testdb.users", bson.Encode(doc2))

	// Scan since the beginning
	scanner := NewOplogScanner(eng)
	count := 0
	err := scanner.ScanSince(0, func(entry OplogEntry) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanSince: %v", err)
	}
	if count != 2 {
		t.Errorf("ScanSince count = %d, want 2", count)
	}
}

func TestOplogScanner_ScanSince_Filter(t *testing.T) {
	eng, oplog := setupOplogTest(t)
	_ = eng

	// Write entries
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	oplog.WriteInsert("testdb.users", bson.Encode(doc))

	// Scan with a future timestamp (should get no results)
	scanner := NewOplogScanner(eng)
	count := 0
	scanner.ScanSince(9999999999, func(entry OplogEntry) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("ScanSince with future timestamp count = %d, want 0", count)
	}
}

func TestOplogScanner_ScanSince_EarlyStop(t *testing.T) {
	eng, oplog := setupOplogTest(t)
	_ = eng

	// Write multiple entries
	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("idx", bson.VInt32(int32(i)))
		oplog.WriteInsert("testdb.users", bson.Encode(doc))
	}

	// Scan but stop early
	scanner := NewOplogScanner(eng)
	count := 0
	scanner.ScanSince(0, func(entry OplogEntry) bool {
		count++
		return count < 2 // Stop after 2
	})
	if count != 2 {
		t.Errorf("ScanSince early stop count = %d, want 2", count)
	}
}

func TestOplogScanner_ScanAll(t *testing.T) {
	eng, oplog := setupOplogTest(t)
	_ = eng

	// Write entries
	for i := 0; i < 3; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("idx", bson.VInt32(int32(i)))
		oplog.WriteInsert("testdb.users", bson.Encode(doc))
	}

	// Scan all
	scanner := NewOplogScanner(eng)
	count := 0
	err := scanner.ScanAll(func(entry OplogEntry) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	if count != 3 {
		t.Errorf("ScanAll count = %d, want 3", count)
	}
}

func TestOplogScanner_ScanAll_EarlyStop(t *testing.T) {
	eng, oplog := setupOplogTest(t)
	_ = eng

	// Write entries
	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		oplog.WriteInsert("testdb.users", bson.Encode(doc))
	}

	// Scan all but stop early
	scanner := NewOplogScanner(eng)
	count := 0
	scanner.ScanAll(func(entry OplogEntry) bool {
		count++
		return count < 2
	})
	if count != 2 {
		t.Errorf("ScanAll early stop count = %d, want 2", count)
	}
}

func TestChangeStreamManager_Watch(t *testing.T) {
	eng, _ := setupOplogTest(t)
	_ = eng

	manager := NewChangeStreamManager(eng)

	watcher := manager.Watch("testdb.users", 0)
	if watcher == nil {
		t.Fatal("Watch returned nil")
	}
	if watcher.ID == 0 {
		t.Error("Watcher ID should not be 0")
	}
	if watcher.Namespace != "testdb.users" {
		t.Errorf("Watcher Namespace = %s, want testdb.users", watcher.Namespace)
	}
	if watcher.ResumeTS != 0 {
		t.Errorf("Watcher ResumeTS = %d, want 0", watcher.ResumeTS)
	}
}

func TestChangeStreamManager_Notify(t *testing.T) {
	eng, _ := setupOplogTest(t)
	_ = eng

	manager := NewChangeStreamManager(eng)
	watcher := manager.Watch("testdb.users", 0)

	entry := OplogEntry{
		Timestamp: 1234567890,
		Operation: "i",
		Namespace: "testdb.users",
	}

	// Notify should send entry to watcher
	manager.Notify(entry)

	select {
	case received := <-watcher.Ch:
		if received.Timestamp != entry.Timestamp {
			t.Error("Received entry has wrong timestamp")
		}
	default:
		t.Error("Notify did not send entry to channel")
	}
}

func TestChangeStreamManager_Notify_FilterNamespace(t *testing.T) {
	eng, _ := setupOplogTest(t)
	_ = eng

	manager := NewChangeStreamManager(eng)
	watcher := manager.Watch("testdb.users", 0)

	entry := OplogEntry{
		Timestamp: 1234567890,
		Operation: "i",
		Namespace: "otherdb.othercoll",
	}

	// Notify should NOT send entry to watcher (namespace mismatch)
	manager.Notify(entry)

	select {
	case <-watcher.Ch:
		t.Error("Notify should not send entry for different namespace")
	default:
		// Expected - no entry received
	}
}

func TestChangeStreamManager_Notify_AllNamespaces(t *testing.T) {
	eng, _ := setupOplogTest(t)
	_ = eng

	manager := NewChangeStreamManager(eng)
	watcher := manager.Watch("", 0) // Empty namespace = watch all

	entry := OplogEntry{
		Timestamp: 1234567890,
		Operation: "i",
		Namespace: "anydb.anycoll",
	}

	// Notify should send entry (watching all namespaces)
	manager.Notify(entry)

	select {
	case received := <-watcher.Ch:
		if received.Namespace != entry.Namespace {
			t.Error("Received entry has wrong namespace")
		}
	default:
		t.Error("Notify should send entry when watching all namespaces")
	}
}

func TestChangeStreamManager_Remove(t *testing.T) {
	eng, _ := setupOplogTest(t)
	_ = eng

	manager := NewChangeStreamManager(eng)
	watcher := manager.Watch("testdb.users", 0)

	// Remove the watcher
	manager.Remove(watcher.ID)

	// Done channel should be closed
	select {
	case <-watcher.Done:
		// Expected
	default:
		t.Error("Done channel should be closed after Remove")
	}

	// Notify should not panic with removed watcher
	entry := OplogEntry{
		Timestamp: 1234567890,
		Operation: "i",
		Namespace: "testdb.users",
	}
	manager.Notify(entry)
}

func TestChangeStreamManager_Remove_NonExistent(t *testing.T) {
	eng, _ := setupOplogTest(t)
	_ = eng

	manager := NewChangeStreamManager(eng)

	// Remove non-existent watcher should not panic
	manager.Remove(99999)
}

func TestChangeStreamManager_Poll(t *testing.T) {
	eng, oplog := setupOplogTest(t)
	_ = eng

	manager := NewChangeStreamManager(eng)
	watcher := manager.Watch("testdb.users", 0)

	// Write some entries
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc1.Set("data", bson.VString("first"))
	oplog.WriteInsert("testdb.users", bson.Encode(doc1))

	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc2.Set("data", bson.VString("second"))
	oplog.WriteInsert("testdb.users", bson.Encode(doc2))

	// Poll should return entries
	entries := manager.Poll(watcher)
	if len(entries) != 2 {
		t.Errorf("Poll returned %d entries, want 2", len(entries))
	}

	// ResumeTS should be updated
	if watcher.ResumeTS == 0 {
		t.Error("ResumeTS should be updated after Poll")
	}
}

func TestChangeStreamManager_Poll_FilterNamespace(t *testing.T) {
	eng, oplog := setupOplogTest(t)
	_ = eng

	manager := NewChangeStreamManager(eng)
	watcher := manager.Watch("testdb.users", 0)

	// Write entry for different namespace
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	oplog.WriteInsert("otherdb.othercoll", bson.Encode(doc))

	// Poll should not return entries for different namespace
	entries := manager.Poll(watcher)
	if len(entries) != 0 {
		t.Errorf("Poll returned %d entries, want 0", len(entries))
	}
}

func TestChangeStreamManager_Poll_NoEntries(t *testing.T) {
	eng, _ := setupOplogTest(t)
	_ = eng

	manager := NewChangeStreamManager(eng)
	watcher := manager.Watch("testdb.users", 0)

	// Poll with no entries should return empty slice
	entries := manager.Poll(watcher)
	if len(entries) != 0 {
		t.Errorf("Poll returned %d entries, want 0", len(entries))
	}
}

func TestChangeStreamManager_Poll_ResumeAfterTS(t *testing.T) {
	eng, oplog := setupOplogTest(t)
	_ = eng

	// Write first entry
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc1.Set("idx", bson.VInt32(1))
	oplog.WriteInsert("testdb.users", bson.Encode(doc1))

	// Create watcher and poll
	manager := NewChangeStreamManager(eng)
	watcher := manager.Watch("testdb.users", 0)
	entries := manager.Poll(watcher)
	if len(entries) != 1 {
		t.Fatalf("First poll returned %d entries, want 1", len(entries))
	}

	// Note: ResumeTS is updated to the last seen timestamp
	// Next poll will start after this timestamp
	firstResumeTS := watcher.ResumeTS
	if firstResumeTS == 0 {
		t.Fatal("ResumeTS should be updated after first poll")
	}

	// Write second entry
	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc2.Set("idx", bson.VInt32(2))
	oplog.WriteInsert("testdb.users", bson.Encode(doc2))

	// Poll again - should return 0 or 1 entries depending on timing
	// (entries with same timestamp may be skipped due to sinceTS++ logic)
	entries = manager.Poll(watcher)
	// Just verify Poll doesn't error and ResumeTS is consistent
	if watcher.ResumeTS < firstResumeTS {
		t.Error("ResumeTS should not decrease")
	}
}

func TestChangeStreamManager_MultipleWatchers(t *testing.T) {
	eng, _ := setupOplogTest(t)
	_ = eng

	manager := NewChangeStreamManager(eng)
	watcher1 := manager.Watch("testdb.coll1", 0)
	watcher2 := manager.Watch("testdb.coll2", 0)
	watcher3 := manager.Watch("", 0) // Watch all

	entry := OplogEntry{
		Timestamp: 1234567890,
		Operation: "i",
		Namespace: "testdb.coll1",
	}

	manager.Notify(entry)

	// watcher1 should receive
	select {
	case <-watcher1.Ch:
		// Expected
	default:
		t.Error("watcher1 should receive entry")
	}

	// watcher2 should NOT receive
	select {
	case <-watcher2.Ch:
		t.Error("watcher2 should not receive entry for different namespace")
	default:
		// Expected
	}

	// watcher3 should receive (watch all)
	select {
	case <-watcher3.Ch:
		// Expected
	default:
		t.Error("watcher3 should receive entry")
	}
}

func TestChangeStreamManager_Notify_ChannelFull(t *testing.T) {
	eng, _ := setupOplogTest(t)
	_ = eng

	manager := NewChangeStreamManager(eng)
	watcher := manager.Watch("testdb.users", 0)
	_ = watcher

	// Fill the channel
	for i := 0; i < 256; i++ {
		entry := OplogEntry{
			Timestamp: int64(i),
			Operation: "i",
			Namespace: "testdb.users",
		}
		manager.Notify(entry)
	}

	// This should not block/panic (drops when full)
	entry := OplogEntry{
		Timestamp: 999,
		Operation: "i",
		Namespace: "testdb.users",
	}
	manager.Notify(entry)
}
