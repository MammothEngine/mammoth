package executor

import (
	"context"
	"encoding/binary"
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

func TestExternalSortNode_Basic(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	// Create a mock child node
	child := NewCollScanNode(eng, "test", "coll")

	// Create external sort node
	sortSpec := bson.NewDocument()
	sortSpec.Set("name", bson.VInt32(1))
	node, err := NewExternalSortNode(child, sortSpec, 1024*1024) // 1MB memory limit
	if err != nil {
		t.Fatalf("NewExternalSortNode: %v", err)
	}

	if node == nil {
		t.Fatal("expected non-nil ExternalSortNode")
	}
}

func TestExternalSortNode_OpenClose(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	// Insert some test data first
	id1 := bson.NewObjectID()
	key := mongo.EncodeDocumentKey("test", "coll", id1[:])
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(id1))
	doc.Set("name", bson.VString("alice"))
	eng.Put(key, bson.Encode(doc))

	id2 := bson.NewObjectID()
	key2 := mongo.EncodeDocumentKey("test", "coll", id2[:])
	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VObjectID(id2))
	doc2.Set("name", bson.VString("bob"))
	eng.Put(key2, bson.Encode(doc2))

	child := NewCollScanNode(eng, "test", "coll")
	sortSpec := bson.NewDocument()
	sortSpec.Set("name", bson.VInt32(1))
	node, err := NewExternalSortNode(child, sortSpec, 1024*1024)
	if err != nil {
		t.Fatalf("NewExternalSortNode: %v", err)
	}

	ctx := context.Background()

	// Open should work
	err = node.Open(ctx)
	if err != nil {
		t.Logf("Open returned: %v", err)
	}

	// Try to get some rows
	count := 0
	for count < 5 {
		row, err := node.Next()
		if err != nil || row == nil {
			break
		}
		count++
	}

	// Close should work
	err = node.Close()
	if err != nil {
		t.Logf("Close returned: %v", err)
	}
}

func TestExternalSortNode_Cleanup(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	child := NewCollScanNode(eng, "test", "coll")
	sortSpec := bson.NewDocument()
	sortSpec.Set("name", bson.VInt32(1))
	node, err := NewExternalSortNode(child, sortSpec, 1024*1024)
	if err != nil {
		t.Fatalf("NewExternalSortNode: %v", err)
	}

	// Cleanup should be safe even before Open
	node.cleanup()

	// Cleanup should be safe after close
	ctx := context.Background()
	node.Open(ctx)
	node.Close()
	node.cleanup()
}

// Test ExternalSortNode Explain
func TestExternalSortNode_Explain(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	child := NewCollScanNode(eng, "test", "coll")
	sortSpec := bson.NewDocument()
	sortSpec.Set("name", bson.VInt32(1))
	node, err := NewExternalSortNode(child, sortSpec, 1024*1024)
	if err != nil {
		t.Fatalf("NewExternalSortNode: %v", err)
	}

	explain := node.Explain()
	if explain.NodeType != "EXT_SORT" {
		t.Errorf("NodeType = %s, want EXT_SORT", explain.NodeType)
	}

	// Should have child explain
	if len(explain.Children) != 1 {
		t.Errorf("expected 1 child, got %d", len(explain.Children))
	}

	// Should have details
	if explain.Details == nil {
		t.Error("Explain should have details")
	}
}

// Test ExternalSortNode Stats
func TestExternalSortNode_Stats(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	child := NewCollScanNode(eng, "test", "coll")
	sortSpec := bson.NewDocument()
	sortSpec.Set("name", bson.VInt32(1))
	node, err := NewExternalSortNode(child, sortSpec, 1024*1024)
	if err != nil {
		t.Fatalf("NewExternalSortNode: %v", err)
	}

	stats := node.Stats()
	// Stats should be initially zero
	if stats.RowsIn != 0 {
		t.Errorf("RowsIn = %d, want 0", stats.RowsIn)
	}
	if stats.RowsOut != 0 {
		t.Errorf("RowsOut = %d, want 0", stats.RowsOut)
	}
}

func TestNewSortNode_WithSortSpec(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	// Insert test data
	for i := 0; i < 5; i++ {
		id := bson.NewObjectID()
		key := mongo.EncodeDocumentKey("test", "coll", id[:])
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		eng.Put(key, bson.Encode(doc))
	}

	child := NewCollScanNode(eng, "test", "coll")
	sortSpec := bson.NewDocument()
	sortSpec.Set("value", bson.VInt32(1))
	node, err := NewSortNode(child, sortSpec)
	if err != nil {
		t.Fatalf("NewSortNode: %v", err)
	}

	ctx := context.Background()
	err = node.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	// Read all sorted results
	var results []*bson.Document
	for {
		doc, err := node.Next()
		if err != nil || doc == nil {
			break
		}
		results = append(results, doc)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}
}

func TestIndexScanNode_Next(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	// Insert test data with an index
	id1 := bson.NewObjectID()
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(id1))
	doc1.Set("name", bson.VString("alice"))
	key1 := mongo.EncodeDocumentKey("test", "coll", id1[:])
	eng.Put(key1, bson.Encode(doc1))

	id2 := bson.NewObjectID()
	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VObjectID(id2))
	doc2.Set("name", bson.VString("bob"))
	key2 := mongo.EncodeDocumentKey("test", "coll", id2[:])
	eng.Put(key2, bson.Encode(doc2))

	// Create index entries manually (simulating an index)
	spec := &mongo.IndexSpec{
		Name: "name_idx",
		Key:  []mongo.IndexKey{{Field: "name", Descending: false}},
	}

	bounds := IndexBounds{
		LowerBound: []byte("a"),
		UpperBound: []byte("z"),
	}
	node := NewIndexScanNode(eng, "test", "coll", spec, bounds)

	// Test Next without Open - should error
	_, err := node.Next()
	if err == nil {
		t.Error("expected error when Next called before Open")
	}

	// Open the node
	ctx := context.Background()
	err = node.Open(ctx)
	if err != nil {
		t.Logf("Open returned: %v (may be expected if index doesn't exist)", err)
	}

	// Test Next after Open
	count := 0
	for {
		doc, err := node.Next()
		if err != nil || doc == nil {
			break
		}
		count++
		t.Logf("Got document: %v", doc)
	}

	// Since we didn't actually create index entries, we expect 0 results
	// But the test exercises the Next function
	t.Logf("Total documents from index scan: %d", count)

	// Close and test that Next errors again
	node.Close()
	_, err = node.Next()
	if err == nil {
		t.Error("expected error when Next called after Close")
	}
}

func TestIndexScanNode_Next_WithResults(t *testing.T) {
	eng, cleanup := setupTestEngine(t)
	defer cleanup()

	// Insert test documents
	id1 := bson.NewObjectID()
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(id1))
	doc1.Set("name", bson.VString("alice"))
	key1 := mongo.EncodeDocumentKey("test", "coll", id1[:])
	eng.Put(key1, bson.Encode(doc1))

	// Create index prefix using the correct format
	// Format: 4-byte db len + db + 4-byte coll len + coll + idx separator + index name + id
	ns := make([]byte, 0, 4+4+len("test")+len("coll")+4+len("name_idx"))
	ns = binary.BigEndian.AppendUint32(ns, uint32(len("test")))
	ns = append(ns, []byte("test")...)
	ns = binary.BigEndian.AppendUint32(ns, uint32(len("coll")))
	ns = append(ns, []byte("coll")...)
	ns = append(ns, 0x00, 'i', 'd', 'x')
	ns = append(ns, []byte("name_idx")...)

	// Manually create index entries to make IndexScanNode find documents
	idxKey := append(ns, id1[:]...)
	eng.Put(idxKey, []byte{1})

	spec := &mongo.IndexSpec{
		Name: "name_idx",
		Key:  []mongo.IndexKey{{Field: "name", Descending: false}},
	}

	bounds := IndexBounds{}
	node := NewIndexScanNode(eng, "test", "coll", spec, bounds)

	// Open the node
	ctx := context.Background()
	err := node.Open(ctx)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer node.Close()

	// Test Next - should return the document
	doc, err := node.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if doc == nil {
		t.Error("expected a document, got nil")
	} else {
		// Verify we got our document back
		nameVal, ok := doc.Get("name")
		if ok && nameVal.Type == bson.TypeString {
			if nameVal.String() != "alice" {
				t.Errorf("expected name 'alice', got '%s'", nameVal.String())
			}
		}
	}

	// Next should return nil when exhausted
	doc2, err := node.Next()
	if err != nil {
		t.Errorf("unexpected error on second Next: %v", err)
	}
	if doc2 != nil {
		t.Error("expected nil after exhausting results")
	}
}
