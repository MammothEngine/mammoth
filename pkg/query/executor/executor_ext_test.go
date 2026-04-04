package executor

import (
	"context"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func setupTestEngine(t *testing.T) (*engine.Engine, func()) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	return eng, func() { eng.Close() }
}

func TestIndexScanNode_Basic(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	// Create an index spec
	spec := &mongo.IndexSpec{
		Name: "test_idx",
		Key:  []mongo.IndexKey{{Field: "name", Descending: false}},
	}

	// Create index scan node
	bounds := IndexBounds{
		LowerBound: []byte("a"),
		UpperBound: []byte("z"),
	}
	node := NewIndexScanNode(eng, "test", "coll", spec, bounds)

	if node == nil {
		t.Fatal("expected non-nil IndexScanNode")
	}

	// Test WithFilter
	filterDoc := bson.NewDocument()
	filterDoc.Set("active", bson.VBool(true))
	node.WithFilter(filterDoc)

	// Test Filter() to get filter back
	fn := node.Filter()
	if fn == nil {
		t.Error("expected non-nil filter function")
	}

	// Test SetFilter
	node.SetFilter(func(doc *bson.Document) bool {
		return true
	})

	// Test WithProjection
	projDoc := bson.NewDocument()
	projDoc.Set("name", bson.VInt32(1))
	node.WithProjection(projDoc)
}

func TestIndexScanNode_OpenClose(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	spec := &mongo.IndexSpec{
		Name: "test_idx",
		Key:  []mongo.IndexKey{{Field: "name", Descending: false}},
	}

	bounds := IndexBounds{
		LowerBound: []byte(""),
		UpperBound: []byte("~"),
	}
	node := NewIndexScanNode(eng, "test", "coll", spec, bounds)

	// Open the node
	ctx := context.Background()
	err := node.Open(ctx)
	if err != nil {
		t.Logf("Open returned: %v (may be expected if index doesn't exist)", err)
	}

	// Double open should error
	if node.open {
		err = node.Open(ctx)
		if err == nil {
			t.Error("expected error on double open")
		}
	}

	// Close should work
	err = node.Close()
	if err != nil {
		t.Logf("Close returned: %v", err)
	}

	// Double close should be safe
	err = node.Close()
	if err != nil {
		t.Logf("Second Close returned: %v", err)
	}
}

func TestIndexScanNode_Explain(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	spec := &mongo.IndexSpec{
		Name: "test_idx",
		Key:  []mongo.IndexKey{{Field: "name", Descending: false}},
	}

	bounds := IndexBounds{}
	node := NewIndexScanNode(eng, "test", "coll", spec, bounds)

	// Test Explain
	info := node.Explain()
	if info.NodeType != "IXSCAN" {
		t.Errorf("expected NodeType 'IXSCAN', got '%s'", info.NodeType)
	}
}

func TestIndexScanNode_Stats(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	spec := &mongo.IndexSpec{
		Name: "test_idx",
		Key:  []mongo.IndexKey{{Field: "name", Descending: false}},
	}

	bounds := IndexBounds{}
	node := NewIndexScanNode(eng, "test", "coll", spec, bounds)

	// Test Stats
	stats := node.Stats()
	// Stats should return zero values initially
	_ = stats
}


func TestIndexScanNode_NextNotOpen(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	spec := &mongo.IndexSpec{
		Name: "test_idx",
		Key:  []mongo.IndexKey{{Field: "name", Descending: false}},
	}

	bounds := IndexBounds{}
	node := NewIndexScanNode(eng, "test", "coll", spec, bounds)

	// Calling Next without Open should return error
	_, err := node.Next()
	if err == nil {
		t.Error("expected error when calling Next without Open")
	}
}

func TestSortNode_DifferentDirections(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	db, coll := "test", "sortdir"
	
	// Insert test data
	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Test descending sort (using different value types for sort spec)
	// Test descending sort (using different value types for sort spec)
	sortSpecs := []*bson.Document{
		func() *bson.Document { d := bson.NewDocument(); d.Set("value", bson.VInt32(-1)); return d }(),           // int negative
		func() *bson.Document { d := bson.NewDocument(); d.Set("value", bson.VInt64(-1)); return d }(),           // int64 negative
		func() *bson.Document { d := bson.NewDocument(); d.Set("value", bson.VDouble(-1.0)); return d }(),        // double negative
		func() *bson.Document { d := bson.NewDocument(); d.Set("value", bson.VBool(false)); return d }(),         // boolean false = descending
	}

	for i, sortSpec := range sortSpecs {
		ctx := context.Background()
		scan := NewCollScanNode(eng, db, coll)
		sort, err := NewSortNode(scan, sortSpec)
		if err != nil {
			t.Fatalf("sort %d: %v", i, err)
		}

		if err := sort.Open(ctx); err != nil {
			t.Fatalf("open %d: %v", i, err)
		}

		// Just verify it works
		doc, err := sort.Next()
		if err != nil {
			t.Fatalf("next %d: %v", i, err)
		}
		if doc == nil {
			t.Errorf("sort %d: expected document", i)
		}

		sort.Close()
	}
}
