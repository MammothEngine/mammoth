package repl

import (
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func TestOplog_Append(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)

	// Create a test document
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("test"))
	doc.Set("value", bson.VInt32(42))

	// Append an insert operation
	entry, err := oplog.Append(OpInsert, "test.collection", doc, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Verify entry fields
	if entry.Operation != OpInsert {
		t.Errorf("expected OpInsert, got %s", entry.Operation)
	}
	if entry.Namespace != "test.collection" {
		t.Errorf("expected namespace=test.collection, got %s", entry.Namespace)
	}
	if entry.Object == nil {
		t.Error("expected non-nil Object")
	}
	if entry.Hash == 0 {
		t.Error("expected non-zero hash")
	}
	if entry.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}
}

func TestOplog_GetSince(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)

	// Append multiple entries
	since := time.Now().UTC()
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("i", bson.VInt32(int32(i)))
		_, err := oplog.Append(OpInsert, "test.items", doc, nil)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Get entries since
	entries, err := oplog.GetSince(since, 10)
	if err != nil {
		t.Fatalf("GetSince: %v", err)
	}

	if len(entries) != 5 {
		t.Errorf("expected 5 entries, got %d", len(entries))
	}
}

func TestOplog_GetSince_Limit(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)

	// Append 10 entries
	since := time.Now().UTC()
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		_, err := oplog.Append(OpInsert, "test.items", doc, nil)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Get with limit
	entries, err := oplog.GetSince(since, 5)
	if err != nil {
		t.Fatalf("GetSince: %v", err)
	}

	if len(entries) != 5 {
		t.Errorf("expected 5 entries (limited), got %d", len(entries))
	}
}

func TestOplog_Truncate(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)

	// Append entries
	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		_, err := oplog.Append(OpInsert, "test.items", doc, nil)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		time.Sleep(time.Millisecond)
	}

	cutoff := time.Now().UTC()
	time.Sleep(10 * time.Millisecond)

	// Append more entries after cutoff
	for i := 0; i < 3; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		_, err := oplog.Append(OpInsert, "test.items", doc, nil)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Truncate old entries
	if err := oplog.Truncate(cutoff); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// Verify only newer entries remain
	entries, _ := oplog.GetSince(time.Time{}, 100)
	if len(entries) != 3 {
		t.Errorf("expected 3 entries after truncate, got %d", len(entries))
	}
}

func TestOplogApplier_ApplyInsert(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	applier := NewOplogApplier(eng)

	// Create insert entry
	id := bson.NewObjectID()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(id))
	doc.Set("name", bson.VString("alice"))

	entry := &OplogEntry{
		Operation: OpInsert,
		Namespace: "test.users",
		Object:    doc,
	}

	// Apply insert
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply insert: %v", err)
	}

	// Verify document exists
	key := makeDocumentKey("test.users", bson.VObjectID(id))
	data, err := eng.Get(key)
	if err != nil {
		t.Fatalf("Get after insert: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected data after insert")
	}
}

func TestOplogApplier_ApplyUpdate(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	applier := NewOplogApplier(eng)

	// Insert initial document
	id := bson.NewObjectID()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(id))
	doc.Set("name", bson.VString("alice"))
	doc.Set("age", bson.VInt32(30))

	insertEntry := &OplogEntry{
		Operation: OpInsert,
		Namespace: "test.users",
		Object:    doc,
	}
	if err := applier.Apply(insertEntry); err != nil {
		t.Fatalf("Apply insert: %v", err)
	}

	// Create update entry with $set
	query := bson.NewDocument()
	query.Set("_id", bson.VObjectID(id))

	update := bson.NewDocument()
	setDoc := bson.NewDocument()
	setDoc.Set("age", bson.VInt32(31))
	update.Set("$set", bson.VDoc(setDoc))

	updateEntry := &OplogEntry{
		Operation: OpUpdate,
		Namespace: "test.users",
		Object:    update,
		Object2:   query,
	}

	// Apply update
	if err := applier.Apply(updateEntry); err != nil {
		t.Fatalf("Apply update: %v", err)
	}

	// Verify update
	key := makeDocumentKey("test.users", bson.VObjectID(id))
	data, _ := eng.Get(key)
	updatedDoc, _ := bson.Decode(data)

	age, ok := updatedDoc.Get("age")
	if !ok {
		t.Fatal("age field not found after update")
	}
	if age.Int32() != 31 {
		t.Errorf("expected age=31 after update, got %d", age.Int32())
	}
}

func TestOplogApplier_ApplyDelete(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	applier := NewOplogApplier(eng)

	// Insert document
	id := bson.NewObjectID()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(id))
	doc.Set("name", bson.VString("alice"))

	insertEntry := &OplogEntry{
		Operation: OpInsert,
		Namespace: "test.users",
		Object:    doc,
	}
	if err := applier.Apply(insertEntry); err != nil {
		t.Fatalf("Apply insert: %v", err)
	}

	// Delete document
	query := bson.NewDocument()
	query.Set("_id", bson.VObjectID(id))

	deleteEntry := &OplogEntry{
		Operation: OpDelete,
		Namespace: "test.users",
		Object:    query,
	}

	if err := applier.Apply(deleteEntry); err != nil {
		t.Fatalf("Apply delete: %v", err)
	}

	// Verify deletion
	key := makeDocumentKey("test.users", bson.VObjectID(id))
	_, err = eng.Get(key)
	if err == nil {
		t.Error("expected document to be deleted")
	}
}

func TestOplogApplier_ApplyNoop(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	applier := NewOplogApplier(eng)

	entry := &OplogEntry{
		Operation: OpNoop,
		Namespace: "test.items",
	}

	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply noop: %v", err)
	}
}

func TestOplogApplier_MissingID(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	applier := NewOplogApplier(eng)

	// Insert without _id
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("alice")) // no _id

	entry := &OplogEntry{
		Operation: OpInsert,
		Namespace: "test.users",
		Object:    doc,
	}

	err = applier.Apply(entry)
	if err == nil {
		t.Error("expected error for missing _id")
	}
}

func TestOplog_MonotonicTimestamps(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)

	// Append entries rapidly
	var timestamps []time.Time
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		entry, err := oplog.Append(OpInsert, "test.items", doc, nil)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		timestamps = append(timestamps, entry.Timestamp)
	}

	// Verify monotonicity
	for i := 1; i < len(timestamps); i++ {
		if !timestamps[i].After(timestamps[i-1]) {
			t.Errorf("timestamp %d not after timestamp %d: %v vs %v",
				i, i-1, timestamps[i], timestamps[i-1])
		}
	}
}

func TestOplog_LatestTimestamp(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)

	// Initial timestamp should be zero
	if !oplog.GetLatestTimestamp().IsZero() {
		t.Error("expected zero timestamp initially")
	}

	// Append entry
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	entry, _ := oplog.Append(OpInsert, "test.items", doc, nil)

	// Latest timestamp should match
	latest := oplog.GetLatestTimestamp()
	if !latest.Equal(entry.Timestamp) {
		t.Errorf("expected latest timestamp %v, got %v", entry.Timestamp, latest)
	}
}

func TestTimeConversion(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Nanosecond)
	
	// Test timeToBytes and bytesToTime roundtrip
	b := timeToBytes(now)
	if len(b) != 8 {
		t.Errorf("timeToBytes returned %d bytes, want 8", len(b))
	}
	
	result := bytesToTime(b)
	if !result.Equal(now) {
		t.Errorf("bytesToTime(timeToBytes(t)) = %v, want %v", result, now)
	}
}

func TestBytesToTime_ShortBuffer(t *testing.T) {
	// Test with buffer shorter than 8 bytes
	b := []byte{0, 0, 0} // Only 3 bytes
	result := bytesToTime(b)
	
	// Should return zero time
	if !result.IsZero() {
		t.Errorf("bytesToTime(short buffer) = %v, want zero time", result)
	}
}

