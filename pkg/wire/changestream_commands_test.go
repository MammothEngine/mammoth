package wire

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestOplogWriteAndScan(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "mammoth_oplog_test")
	os.RemoveAll(dir)
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	defer os.RemoveAll(dir)

	oplog := mongo.NewOplog(eng)
	scanner := mongo.NewOplogScanner(eng)

	// Write entries
	doc := bson.D("_id", bson.VInt32(1), "name", bson.VString("test"))
	docData := bson.Encode(doc)
	oplog.WriteInsert("test.users", docData)
	oplog.WriteUpdate("test.users", docData, nil)
	oplog.WriteDelete("test.users", docData)

	// Scan all
	var entries []mongo.OplogEntry
	scanner.ScanAll(func(entry mongo.OplogEntry) bool {
		entries = append(entries, entry)
		return true
	})

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	if entries[0].Operation != "i" {
		t.Errorf("expected first op to be 'i', got %s", entries[0].Operation)
	}
	if entries[1].Operation != "u" {
		t.Errorf("expected second op to be 'u', got %s", entries[1].Operation)
	}
	if entries[2].Operation != "d" {
		t.Errorf("expected third op to be 'd', got %s", entries[2].Operation)
	}

	// Scan since epoch (should find all)
	var sinceEntries []mongo.OplogEntry
	scanner.ScanSince(1, func(entry mongo.OplogEntry) bool {
		sinceEntries = append(sinceEntries, entry)
		return true
	})
	if len(sinceEntries) != 3 {
		t.Errorf("expected 3 entries since epoch, got %d", len(sinceEntries))
	}
}

func TestChangeStreamManager(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "mammoth_cs_test")
	os.RemoveAll(dir)
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	defer os.RemoveAll(dir)

	oplog := mongo.NewOplog(eng)
	mgr := mongo.NewChangeStreamManager(eng)

	// Write an oplog entry first
	docData := bson.Encode(bson.D("_id", bson.VInt32(1)))
	oplog.WriteInsert("test.users", docData)

	// Verify the oplog entry can be scanned directly
	scanner := mongo.NewOplogScanner(eng)
	var directEntries []mongo.OplogEntry
	scanner.ScanAll(func(entry mongo.OplogEntry) bool {
		directEntries = append(directEntries, entry)
		return true
	})
	if len(directEntries) != 1 {
		t.Fatalf("direct scan: expected 1 entry, got %d", len(directEntries))
	}

	// Watch and poll
	w := mgr.Watch("test.users", 0)
	defer mgr.Remove(w.ID)

	entries := mgr.Poll(w)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry from poll, got %d", len(entries))
	}
	if entries[0].Operation != "i" {
		t.Errorf("expected operation 'i', got %s", entries[0].Operation)
	}
}

func TestChangeEventFromEntry(t *testing.T) {
	entry := mongo.OplogEntry{
		Timestamp: 1711807200,
		Operation: "i",
		Namespace: "test.users",
		Document:  bson.Encode(bson.D("_id", bson.VInt32(1), "name", bson.VString("Alice"))),
		WallTime:  1711807200000,
	}

	event := changeEventFromEntry(entry)

	// Check operationType
	if ot, ok := event.Get("operationType"); !ok || ot.String() != "insert" {
		t.Error("expected operationType=insert")
	}

	// Check namespace
	if ns, ok := event.Get("ns"); ok && ns.Type == bson.TypeDocument {
		if db, ok2 := ns.DocumentValue().Get("db"); !ok2 || db.String() != "test" {
			t.Error("expected ns.db=test")
		}
		if coll, ok2 := ns.DocumentValue().Get("coll"); !ok2 || coll.String() != "users" {
			t.Error("expected ns.coll=users")
		}
	}

	// Check fullDocument
	if fd, ok := event.Get("fullDocument"); ok && fd.Type == bson.TypeDocument {
		if name, ok2 := fd.DocumentValue().Get("name"); !ok2 || name.String() != "Alice" {
			t.Error("expected fullDocument.name=Alice")
		}
	}
}

func TestOpToOperationType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"i", "insert"},
		{"u", "update"},
		{"d", "delete"},
		{"x", "x"}, // unknown op
	}

	for _, tt := range tests {
		result := opToOperationType(tt.input)
		if result != tt.expected {
			t.Errorf("opToOperationType(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestSplitNamespace(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"test.users", []string{"test", "users"}},
		{"test.users.extra", []string{"test", "users.extra"}},
		{"admin", []string{"admin"}},
		{"", []string{""}},
	}

	for _, tt := range tests {
		result := splitNamespace(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("splitNamespace(%q) len = %d, want %d", tt.input, len(result), len(tt.expected))
			continue
		}
		for i, v := range result {
			if v != tt.expected[i] {
				t.Errorf("splitNamespace(%q)[%d] = %q, want %q", tt.input, i, v, tt.expected[i])
			}
		}
	}
}

func TestChangeEventFromEntry_Update(t *testing.T) {
	entry := mongo.OplogEntry{
		Timestamp: 1711807200,
		Operation: "u",
		Namespace: "test.users",
		Document:  bson.Encode(bson.D("_id", bson.VInt32(1), "name", bson.VString("Bob"))),
		WallTime:  1711807200000,
	}

	event := changeEventFromEntry(entry)

	// Check operationType
	if ot, ok := event.Get("operationType"); !ok || ot.String() != "update" {
		t.Errorf("expected operationType=update, got %v", ot)
	}
}

func TestChangeEventFromEntry_Delete(t *testing.T) {
	entry := mongo.OplogEntry{
		Timestamp: 1711807200,
		Operation: "d",
		Namespace: "test.users",
		Document:  bson.Encode(bson.D("_id", bson.VInt32(1))),
		WallTime:  1711807200000,
	}

	event := changeEventFromEntry(entry)

	// Check operationType
	if ot, ok := event.Get("operationType"); !ok || ot.String() != "delete" {
		t.Errorf("expected operationType=delete, got %v", ot)
	}

	// For delete, documentKey should be set
	if dk, ok := event.Get("documentKey"); !ok {
		t.Error("expected documentKey for delete operation")
	} else if dk.Type != bson.TypeDocument {
		t.Errorf("expected documentKey to be document, got %v", dk.Type)
	}
}

func TestChangeEventFromEntry_InvalidDocument(t *testing.T) {
	entry := mongo.OplogEntry{
		Timestamp: 1711807200,
		Operation: "i",
		Namespace: "test.users",
		Document:  []byte{0xff, 0xfe}, // Invalid BSON
		WallTime:  1711807200000,
	}

	// Should not panic
	event := changeEventFromEntry(entry)

	// Should still have basic fields
	if ot, ok := event.Get("operationType"); !ok || ot.String() != "insert" {
		t.Error("expected operationType=insert even with invalid document")
	}
}

func TestChangeEventFromEntry_NoNamespaceDot(t *testing.T) {
	entry := mongo.OplogEntry{
		Timestamp: 1711807200,
		Operation: "i",
		Namespace: "admin", // No dot
		Document:  bson.Encode(bson.D("_id", bson.VInt32(1))),
		WallTime:  1711807200000,
	}

	event := changeEventFromEntry(entry)

	// Check namespace
	if ns, ok := event.Get("ns"); ok && ns.Type == bson.TypeDocument {
		if db, ok2 := ns.DocumentValue().Get("db"); !ok2 || db.String() != "admin" {
			t.Errorf("expected ns.db=admin, got %v", db)
		}
		// coll should not be set
		if _, hasColl := ns.DocumentValue().Get("coll"); hasColl {
			t.Error("expected no coll in namespace when no dot")
		}
	}
}

func TestHandleChangeStream_WithResumeAfter(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	h := NewHandler(eng, cat, nil)
	defer h.Close()

	// Create stage value with resumeAfter
	resumeDoc := bson.D("_data", bson.VInt64(1000))
	stageDoc := bson.D("resumeAfter", bson.VDoc(resumeDoc))
	stageVal := bson.VDoc(stageDoc)

	// Call handleChangeStream (should not panic)
	docs := h.handleChangeStream("test", "users", stageVal)
	// Result may be empty but function should complete
	_ = docs
}

func TestHandleChangeStream_ResumeWithInt32(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	h := NewHandler(eng, cat, nil)
	defer h.Close()

	// Create stage value with resumeAfter using int32
	resumeDoc := bson.D("_data", bson.VInt32(500))
	stageDoc := bson.D("resumeAfter", bson.VDoc(resumeDoc))
	stageVal := bson.VDoc(stageDoc)

	// Call handleChangeStream (should not panic)
	docs := h.handleChangeStream("test", "users", stageVal)
	_ = docs
}

func TestHandleChangeStream_InvalidStageType(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	h := NewHandler(eng, cat, nil)
	defer h.Close()

	// Call with non-document value (should not panic)
	docs := h.handleChangeStream("test", "users", bson.VString("invalid"))
	_ = docs
}

func TestOplogWriteInsert(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	h := NewHandler(eng, cat, nil)
	defer h.Close()

	doc := bson.D("_id", bson.VInt32(1), "name", bson.VString("test"))

	// Should not panic even without oplog
	h.oplogWriteInsert("test", "users", doc)
}

func TestOplogWriteUpdate(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	h := NewHandler(eng, cat, nil)
	defer h.Close()

	doc := bson.D("_id", bson.VInt32(1), "name", bson.VString("updated"))

	// Should not panic even without oplog
	h.oplogWriteUpdate("test", "users", doc)
}

func TestOplogWriteDelete(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	h := NewHandler(eng, cat, nil)
	defer h.Close()

	doc := bson.D("_id", bson.VInt32(1))

	// Should not panic even without oplog
	h.oplogWriteDelete("test", "users", doc)
}
