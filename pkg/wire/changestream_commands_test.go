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
