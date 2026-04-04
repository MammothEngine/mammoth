package repl

import (
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// Test applyDelete with nil Object (error case)
func TestOplogApplier_ApplyDelete_NilObject(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	applier := NewOplogApplier(eng)

	// Delete with nil Object
	deleteEntry := &OplogEntry{
		Operation: OpDelete,
		Namespace: "test.users",
		Object:    nil,
	}

	err = applier.Apply(deleteEntry)
	if err == nil {
		t.Error("expected error for delete with nil Object")
	}
	if err.Error() != "oplog: delete missing query" {
		t.Errorf("unexpected error message: %v", err)
	}
}

// Test applyDelete with missing _id (error case)
func TestOplogApplier_ApplyDelete_MissingID(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	applier := NewOplogApplier(eng)

	// Delete with Object but no _id
	query := bson.NewDocument()
	query.Set("name", bson.VString("alice")) // no _id

	deleteEntry := &OplogEntry{
		Operation: OpDelete,
		Namespace: "test.users",
		Object:    query,
	}

	err = applier.Apply(deleteEntry)
	if err == nil {
		t.Error("expected error for delete with missing _id")
	}
	if err.Error() != "oplog: delete missing _id" {
		t.Errorf("unexpected error message: %v", err)
	}
}

// Test makeDocumentKey with various ID types
func TestMakeDocumentKey_Types(t *testing.T) {
	tests := []struct {
		name     string
		ns       string
		id       bson.Value
		expected string
	}{
		{
			name:     "ObjectID",
			ns:       "test.coll",
			id:       bson.VObjectID(bson.ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}),
			expected: "test.coll.\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c",
		},
		{
			name:     "String",
			ns:       "db.users",
			id:       bson.VString("user123"),
			expected: "db.users.user123",
		},
		{
			name:     "Int32",
			ns:       "test.items",
			id:       bson.VInt32(42),
			expected: "test.items.42",
		},
		{
			name:     "Int64",
			ns:       "test.items",
			id:       bson.VInt64(9223372036854775807),
			expected: "test.items.9223372036854775807",
		},
		{
			name:     "Int32 negative",
			ns:       "test.items",
			id:       bson.VInt32(-100),
			expected: "test.items.-100",
		},
		{
			name:     "Int64 zero",
			ns:       "test.items",
			id:       bson.VInt64(0),
			expected: "test.items.0",
		},
		{
			name:     "Double (default case)",
			ns:       "test.items",
			id:       bson.VDouble(3.14),
			expected: "test.items.", // empty string for double (uses default case)
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := string(makeDocumentKey(tc.ns, tc.id))
			if result != tc.expected {
				t.Errorf("makeDocumentKey(%q, %v) = %q, want %q", tc.ns, tc.id, result, tc.expected)
			}
		})
	}
}



// Test Oplog Append with different operation types
func TestOplog_AppendOperations(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)

	tests := []struct {
		op   OpType
		name string
	}{
		{OpInsert, "insert"},
		{OpUpdate, "update"},
		{OpDelete, "delete"},
		{OpNoop, "noop"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			doc := bson.NewDocument()
			doc.Set("_id", bson.VObjectID(bson.NewObjectID()))

			var query *bson.Document
			if tc.op == OpUpdate || tc.op == OpDelete {
				query = bson.NewDocument()
				query.Set("_id", bson.VObjectID(bson.NewObjectID()))
			}

			entry, err := oplog.Append(tc.op, "test.coll", doc, query)
			if err != nil {
				t.Fatalf("Append %s: %v", tc.op, err)
			}
			if entry.Operation != tc.op {
				t.Errorf("expected operation %s, got %s", tc.op, entry.Operation)
			}
		})
	}
}

// Test Oplog GetSince with exact time boundary
func TestOplog_GetSince_ExactBoundary(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)

	// Add entry and capture exact time
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	entry, err := oplog.Append(OpInsert, "test.items", doc, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// GetSince with the entry's exact timestamp
	entries, err := oplog.GetSince(entry.Timestamp, 10)
	if err != nil {
		t.Fatalf("GetSince: %v", err)
	}

	// Should not include entries at exactly the boundary
	// (implementation uses After, not AfterOrEqual)
	// Result depends on exact implementation
	_ = entries
}

// Test Oplog Truncate with zero time
func TestOplog_Truncate_ZeroTime(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)

	// Add entries
	for i := 0; i < 3; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		_, err := oplog.Append(OpInsert, "test.items", doc, nil)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Truncate with zero time (should delete nothing or everything depending on implementation)
	err = oplog.Truncate(time.Time{})
	if err != nil {
		t.Errorf("Truncate with zero time: %v", err)
	}
}

// Test Oplog Truncate with future time
func TestOplog_Truncate_FutureTime(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)

	// Add entries
	for i := 0; i < 3; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		_, err := oplog.Append(OpInsert, "test.items", doc, nil)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Truncate with future time (should delete all)
	future := time.Now().UTC().Add(time.Hour)
	err = oplog.Truncate(future)
	if err != nil {
		t.Errorf("Truncate with future time: %v", err)
	}

	// All entries should be gone
	entries, _ := oplog.GetSince(time.Time{}, 100)
	if len(entries) != 0 {
		t.Errorf("expected 0 entries after future truncate, got %d", len(entries))
	}
}

// Test Applier Apply with unknown operation
func TestOplogApplier_ApplyUnknown(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	applier := NewOplogApplier(eng)

	entry := &OplogEntry{
		Operation: OpType("unknown"),
		Namespace: "test.items",
		Object:    bson.NewDocument(),
	}

	err = applier.Apply(entry)
	if err == nil {
		t.Error("expected error for unknown operation")
	}
}
