package main

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestCompactCmd(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	// Insert some data to create memtables
	for i := 0; i < 100; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey("testdb", "testcoll", id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Get initial stats
	statsBefore := eng.Stats()
	t.Logf("Before: Memtables=%d, SSTables=%d", statsBefore.MemtableCount, statsBefore.SSTableCount)

	eng.Close()

	// Note: We can't easily test the full compactCmd because it calls os.Exit
	// But we can test that the engine was created and closed successfully
}

func TestCompactCmd_EngineOpen(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	stats := eng.Stats()
	t.Logf("Engine stats: %+v", stats)

	eng.Close()
}
