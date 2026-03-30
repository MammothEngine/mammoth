package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestCursor_GetBatch(t *testing.T) {
	docs := make([]*bson.Document, 10)
	for i := range docs {
		docs[i] = bson.NewDocument()
		docs[i].Set("i", bson.VInt32(int32(i)))
	}

	cm := NewCursorManager()
	defer cm.Close()

	cursor := cm.Register("test.coll", docs, 3)

	// First batch
	batch := cursor.GetBatch(3)
	if len(batch) != 3 {
		t.Fatalf("first batch length = %d, want 3", len(batch))
	}

	// Second batch
	batch = cursor.GetBatch(3)
	if len(batch) != 3 {
		t.Fatalf("second batch length = %d, want 3", len(batch))
	}

	// Third batch
	batch = cursor.GetBatch(3)
	if len(batch) != 3 {
		t.Fatalf("third batch length = %d, want 3", len(batch))
	}

	// Fourth batch (remaining 1)
	batch = cursor.GetBatch(3)
	if len(batch) != 1 {
		t.Fatalf("fourth batch length = %d, want 1", len(batch))
	}

	// No more batches
	batch = cursor.GetBatch(3)
	if batch != nil {
		t.Fatalf("fifth batch should be nil, got %d", len(batch))
	}
}

func TestCursor_Kill(t *testing.T) {
	docs := []*bson.Document{bson.NewDocument()}
	cm := NewCursorManager()
	defer cm.Close()

	cursor := cm.Register("test.coll", docs, 1)
	id := cursor.ID()

	cm.Kill([]uint64{id})
	_, ok := cm.Get(id)
	if ok {
		t.Error("cursor should be removed after Kill")
	}
}

func TestCursor_TimeoutCleanup(t *testing.T) {
	cm := NewCursorManager()
	defer cm.Close()

	docs := []*bson.Document{bson.NewDocument()}
	cursor := cm.Register("test.coll", docs, 1)

	// Cursor should be accessible
	_, ok := cm.Get(cursor.ID())
	if !ok {
		t.Error("cursor should be accessible immediately after registration")
	}
}

func TestCursor_CloseNoPanic(t *testing.T) {
	cm := NewCursorManager()
	docs := []*bson.Document{bson.NewDocument()}
	cm.Register("test.coll", docs, 1)

	// Close should not panic
	cm.Close()

	// Double close should not panic either
	cm.Close()
}
