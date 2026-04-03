package executor

import (
	"context"
	"os"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestCollScanNode(t *testing.T) {
	// Create temp engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert test data using proper key encoding
	db, coll := "test", "scan"
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VInt32(int32(i)))
		doc.Set("value", bson.VInt32(int32(i * 10)))
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Test collection scan
	ctx := context.Background()
	scan := NewCollScanNode(eng, db, coll)

	if err := scan.Open(ctx); err != nil {
		t.Fatalf("open: %v", err)
	}

	count := 0
	for {
		doc, err := scan.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if doc == nil {
			break
		}
		count++
	}

	if count != 10 {
		t.Errorf("expected 10 docs, got %d", count)
	}

	if err := scan.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestFilterNode(t *testing.T) {
	// Create temp engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert test data
	db, coll := "test", "filter"
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Test filter: value >= 5
	ctx := context.Background()
	scan := NewCollScanNode(eng, db, coll)
	filter := bson.NewDocument()
	gteDoc := bson.NewDocument()
	gteDoc.Set("$gte", bson.VInt32(5))
	filter.Set("value", bson.VDoc(gteDoc))

	filterNode, err := NewFilterNode(scan, filter)
	if err != nil {
		t.Fatalf("create filter: %v", err)
	}

	results, err := Exec(ctx, filterNode)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 docs (value >= 5), got %d", len(results))
	}

	// Verify all results have value >= 5
	for _, doc := range results {
		v, _ := doc.Get("value")
		if v.Int32() < 5 {
			t.Errorf("unexpected doc with value %d", v.Int32())
		}
	}
}

func TestLimitNode(t *testing.T) {
	// Create temp engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert test data
	db, coll := "test", "limit"
	for i := 0; i < 100; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Test limit
	ctx := context.Background()
	scan := NewCollScanNode(eng, db, coll)
	limitNode, err := NewLimitNode(scan, 10)
	if err != nil {
		t.Fatalf("create limit: %v", err)
	}

	results, err := Exec(ctx, limitNode)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	if len(results) != 10 {
		t.Errorf("expected 10 docs, got %d", len(results))
	}
}

func TestSortNode(t *testing.T) {
	// Create temp engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert test data in reverse order
	db, coll := "test", "sort"
	ids := make([]bson.ObjectID, 10)
	for i := range ids {
		ids[i] = bson.NewObjectID()
	}
	for i := 9; i >= 0; i-- {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(ids[i]))
		doc.Set("value", bson.VInt32(int32(i + 1)))
		key := mongo.EncodeDocumentKey(db, coll, ids[i][:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Test sort by value ascending
	ctx := context.Background()
	scan := NewCollScanNode(eng, db, coll)
	sortSpec := bson.NewDocument()
	sortSpec.Set("value", bson.VInt32(1)) // 1 = ascending

	sortNode, err := NewSortNode(scan, sortSpec)
	if err != nil {
		t.Fatalf("create sort: %v", err)
	}

	results, err := Exec(ctx, sortNode)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	if len(results) != 10 {
		t.Errorf("expected 10 docs, got %d", len(results))
	}

	// Verify sorted order
	for i, doc := range results {
		v, _ := doc.Get("value")
		expected := int32(i + 1)
		if v.Int32() != expected {
			t.Errorf("position %d: expected value %d, got %d", i, expected, v.Int32())
		}
	}
}

func TestExec(t *testing.T) {
	// Create temp engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert test data
	db, coll := "test", "exec"
	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	ctx := context.Background()
	scan := NewCollScanNode(eng, db, coll)

	results, err := Exec(ctx, scan)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 docs, got %d", len(results))
	}
}

func TestEmptyScanNode(t *testing.T) {
	ctx := context.Background()
	empty := NewEmptyScanNode()

	if err := empty.Open(ctx); err != nil {
		t.Fatalf("open: %v", err)
	}

	doc, err := empty.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if doc != nil {
		t.Error("expected nil from empty scan")
	}

	if err := empty.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestExplainNode(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	scan := NewCollScanNode(eng, "test", "explain")
	explain := scan.Explain()

	if explain.NodeType != "COLLSCAN" {
		t.Errorf("expected node type COLLSCAN, got %s", explain.NodeType)
	}
}

func TestSkipNode(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "skip"
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	ctx := context.Background()
	scan := NewCollScanNode(eng, db, coll)
	skipNode, err := NewSkipNode(scan, 5)
	if err != nil {
		t.Fatalf("create skip: %v", err)
	}

	results, err := Exec(ctx, skipNode)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 docs (10 - 5 skip), got %d", len(results))
	}

	// Verify values are 5-9
	for i, doc := range results {
		v, _ := doc.Get("value")
		expected := int32(i + 5)
		if v.Int32() != expected {
			t.Errorf("position %d: expected value %d, got %d", i, expected, v.Int32())
		}
	}
}

func TestLimitSkipNode(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "limitskip"
	for i := 0; i < 20; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	ctx := context.Background()
	scan := NewCollScanNode(eng, db, coll)
	node, err := NewLimitSkipNode(scan, 5, 10) // skip 10, limit 5
	if err != nil {
		t.Fatalf("create limitskip: %v", err)
	}

	results, err := Exec(ctx, node)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 docs (skip 10, limit 5), got %d", len(results))
	}

	// Verify values are 10-14
	for i, doc := range results {
		v, _ := doc.Get("value")
		expected := int32(i + 10)
		if v.Int32() != expected {
			t.Errorf("position %d: expected value %d, got %d", i, expected, v.Int32())
		}
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

// Test ResultSet
func TestResultSet(t *testing.T) {
	docs := []*bson.Document{
		bson.D("_id", bson.VInt32(1), "name", bson.VString("Alice")),
		bson.D("_id", bson.VInt32(2), "name", bson.VString("Bob")),
		bson.D("_id", bson.VInt32(3), "name", bson.VString("Charlie")),
	}

	rs := NewResultSet(docs)

	// Test HasNext
	if !rs.HasNext() {
		t.Error("HasNext should return true initially")
	}

	// Test Next
	doc, err := rs.Next()
	if err != nil {
		t.Fatalf("Next error: %v", err)
	}
	if doc == nil {
		t.Fatal("Next should return first doc")
	}
	id, _ := doc.Get("_id")
	if id.Int32() != 1 {
		t.Errorf("First doc _id = %d, want 1", id.Int32())
	}

	// Test HasNext after some reads
	if !rs.HasNext() {
		t.Error("HasNext should still return true")
	}

	// Test All remaining
	remaining, err := rs.All()
	if err != nil {
		t.Fatalf("All error: %v", err)
	}
	if len(remaining) != 2 {
		t.Errorf("All returned %d docs, want 2", len(remaining))
	}

	// Test HasNext after All
	if rs.HasNext() {
		t.Error("HasNext should return false after All")
	}

	// Test Next after All
	doc, _ = rs.Next()
	if doc != nil {
		t.Error("Next should return nil after exhaustion")
	}

	// Test Close
	rs2 := NewResultSet(docs)
	rs2.Close()
	if rs2.HasNext() {
		t.Error("HasNext should return false after Close")
	}
	_, err = rs2.All()
	if err == nil {
		t.Error("All should return error after Close")
	}
}

// Test ExecLimit
func TestExecLimit(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert test data
	db, coll := "test", "execlimit"
	for i := 0; i < 20; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	ctx := context.Background()
	scan := NewCollScanNode(eng, db, coll)

	// Exec with limit of 5
	results, err := ExecLimit(ctx, scan, 5)
	if err != nil {
		t.Fatalf("ExecLimit: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("ExecLimit returned %d docs, want 5", len(results))
	}
}

// Test FilterNode Explain and Stats
func TestFilterNode_ExplainAndStats(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "filterstats"
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	scan := NewCollScanNode(eng, db, coll)
	filter := bson.D("value", bson.VDoc(bson.D("$gte", bson.VInt32(5))))
	filterNode, err := NewFilterNode(scan, filter)
	if err != nil {
		t.Fatalf("create filter: %v", err)
	}

	// Execute to populate scan results (needed for Explain to work correctly)
	ctx := context.Background()
	scan.Open(ctx)
	scan.Next() // Trigger one read to populate results
	scan.Close()

	// Test Explain (now scan has results)
	explain := filterNode.Explain()
	if explain.NodeType != "FILTER" {
		t.Errorf("Explain NodeType = %s, want FILTER", explain.NodeType)
	}
	// EstCost is based on child Explain, which depends on scan results
	if len(explain.Children) != 1 {
		t.Errorf("Explain Children = %d, want 1", len(explain.Children))
	}

	// Execute filter to populate stats
	results, err := Exec(ctx, filterNode)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}

	// Test Stats
	stats := filterNode.Stats()
	if stats.RowsIn != 10 {
		t.Errorf("Stats RowsIn = %d, want 10", stats.RowsIn)
	}
	if stats.RowsOut != 5 {
		t.Errorf("Stats RowsOut = %d, want 5", stats.RowsOut)
	}
}

// Test FilterNode error cases
func TestFilterNode_Errors(t *testing.T) {
	// Empty filter
	scan := NewEmptyScanNode()
	_, err := NewFilterNode(scan, nil)
	if err == nil {
		t.Error("NewFilterNode with nil filter should error")
	}

	_, err = NewFilterNode(scan, bson.NewDocument())
	if err == nil {
		t.Error("NewFilterNode with empty filter should error")
	}
}

// Test ProjectNode
func TestProjectNode(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "project"
	doc := bson.NewDocument()
	id := bson.NewObjectID()
	doc.Set("_id", bson.VObjectID(id))
	doc.Set("name", bson.VString("Alice"))
	doc.Set("age", bson.VInt32(30))
	doc.Set("city", bson.VString("NYC"))
	key := mongo.EncodeDocumentKey(db, coll, id[:])
	if err := eng.Put(key, bson.Encode(doc)); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Test inclusion projection
	scan := NewCollScanNode(eng, db, coll)
	projection := bson.D("name", bson.VInt32(1), "age", bson.VInt32(1))
	projectNode, err := NewProjectNode(scan, projection)
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	ctx := context.Background()
	results, err := Exec(ctx, projectNode)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Verify projection - should have _id, name, age but not city
	result := results[0]
	if _, ok := result.Get("name"); !ok {
		t.Error("projected doc should have name")
	}
	if _, ok := result.Get("age"); !ok {
		t.Error("projected doc should have age")
	}
	if _, ok := result.Get("city"); ok {
		t.Error("projected doc should not have city")
	}

	// Test Explain
	explain := projectNode.Explain()
	if explain.NodeType != "PROJECT" {
		t.Errorf("Explain NodeType = %s, want PROJECT", explain.NodeType)
	}
	if include, ok := explain.Details["inclusion"].(bool); !ok || !include {
		t.Error("Explain should show inclusion=true")
	}

	// Test Stats
	stats := projectNode.Stats()
	if stats.RowsIn != 1 || stats.RowsOut != 1 {
		t.Errorf("Stats: RowsIn=%d, RowsOut=%d, want both=1", stats.RowsIn, stats.RowsOut)
	}
}

// Test ProjectNode error cases
func TestProjectNode_Errors(t *testing.T) {
	scan := NewEmptyScanNode()
	_, err := NewProjectNode(scan, nil)
	if err == nil {
		t.Error("NewProjectNode with nil projection should error")
	}

	_, err = NewProjectNode(scan, bson.NewDocument())
	if err == nil {
		t.Error("NewProjectNode with empty projection should error")
	}
}

// Test ProjectNode exclusion mode
func TestProjectNode_Exclusion(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "projectexclude"
	doc := bson.NewDocument()
	id := bson.NewObjectID()
	doc.Set("_id", bson.VObjectID(id))
	doc.Set("name", bson.VString("Alice"))
	doc.Set("secret", bson.VString("password"))
	key := mongo.EncodeDocumentKey(db, coll, id[:])
	if err := eng.Put(key, bson.Encode(doc)); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Test exclusion projection (exclude secret)
	scan := NewCollScanNode(eng, db, coll)
	projection := bson.D("secret", bson.VInt32(0))
	projectNode, err := NewProjectNode(scan, projection)
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	explain := projectNode.Explain()
	if include, ok := explain.Details["inclusion"].(bool); !ok || include {
		t.Error("Explain should show inclusion=false for exclusion mode")
	}
}

// Test LimitNode WithOffset and error cases
func TestLimitNode_WithOffset(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "limitoffset"
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	ctx := context.Background()
	scan := NewCollScanNode(eng, db, coll)

	// Create limit node with offset - use the returned value
	limitNode, err := NewLimitNode(scan, 3)
	if err != nil {
		t.Fatalf("create limit: %v", err)
	}
	// WithOffset returns the modified node, need to capture it
	limitNode = limitNode.WithOffset(5)

	// Test Explain BEFORE execution (offset is consumed during Open)
	explain := limitNode.Explain()
	if explain.NodeType != "LIMIT" {
		t.Errorf("Explain NodeType = %s, want LIMIT", explain.NodeType)
	}
	if offset, ok := explain.Details["offset"].(int64); !ok || offset != 5 {
		t.Errorf("Explain offset = %v, want 5", explain.Details["offset"])
	}

	results, err := Exec(ctx, limitNode)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 docs (limit), got %d", len(results))
	}
}

// Test LimitNode error cases
func TestLimitNode_Errors(t *testing.T) {
	scan := NewEmptyScanNode()
	_, err := NewLimitNode(scan, -1)
	if err == nil {
		t.Error("NewLimitNode with negative limit should error")
	}
}

// Test LimitNode not open error
func TestLimitNode_NotOpen(t *testing.T) {
	scan := NewEmptyScanNode()
	limitNode, _ := NewLimitNode(scan, 10)

	_, err := limitNode.Next()
	if err == nil {
		t.Error("Next should error when not open")
	}
}

// Test SkipNode Explain and Stats
func TestSkipNode_ExplainAndStats(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "skipexplain"
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	scan := NewCollScanNode(eng, db, coll)
	skipNode, err := NewSkipNode(scan, 3)
	if err != nil {
		t.Fatalf("create skip: %v", err)
	}

	// Test Explain
	explain := skipNode.Explain()
	if explain.NodeType != "SKIP" {
		t.Errorf("Explain NodeType = %s, want SKIP", explain.NodeType)
	}
	if skip, ok := explain.Details["skip"].(int64); !ok || skip != 3 {
		t.Errorf("Explain skip = %v, want 3", explain.Details["skip"])
	}

	// Execute and check stats
	ctx := context.Background()
	results, err := Exec(ctx, skipNode)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(results) != 7 {
		t.Errorf("expected 7 results, got %d", len(results))
	}

	stats := skipNode.Stats()
	// RowsIn includes skipped rows (3) + output rows (7) = 10
	if stats.RowsIn != 10 {
		t.Errorf("Stats RowsIn = %d, want 10", stats.RowsIn)
	}
	if stats.RowsOut != 7 { // Output 7 rows
		t.Errorf("Stats RowsOut = %d, want 7", stats.RowsOut)
	}
}

// Test SkipNode error cases
func TestSkipNode_Errors(t *testing.T) {
	scan := NewEmptyScanNode()
	_, err := NewSkipNode(scan, -1)
	if err == nil {
		t.Error("NewSkipNode with negative skip should error")
	}
}

// Test LimitSkipNode Explain and Stats
func TestLimitSkipNode_ExplainAndStats(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "limitskipexplain"
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	scan := NewCollScanNode(eng, db, coll)
	lsNode, err := NewLimitSkipNode(scan, 3, 2)
	if err != nil {
		t.Fatalf("create limitskip: %v", err)
	}

	// Test Explain
	explain := lsNode.Explain()
	if explain.NodeType != "LIMIT_SKIP" {
		t.Errorf("Explain NodeType = %s, want LIMIT_SKIP", explain.NodeType)
	}
	if limit, ok := explain.Details["limit"].(int64); !ok || limit != 3 {
		t.Errorf("Explain limit = %v, want 3", explain.Details["limit"])
	}
	if offset, ok := explain.Details["offset"].(int64); !ok || offset != 2 {
		t.Errorf("Explain offset = %v, want 2", explain.Details["offset"])
	}

	// Execute and check stats
	ctx := context.Background()
	results, err := Exec(ctx, lsNode)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	stats := lsNode.Stats()
	if stats.RowsIn != 5 { // 2 skipped + 3 output
		t.Errorf("Stats RowsIn = %d, want 5", stats.RowsIn)
	}
	if stats.RowsOut != 3 {
		t.Errorf("Stats RowsOut = %d, want 3", stats.RowsOut)
	}
}

// Test LimitSkipNode error cases
func TestLimitSkipNode_Errors(t *testing.T) {
	scan := NewEmptyScanNode()
	_, err := NewLimitSkipNode(scan, -1, 0)
	if err == nil {
		t.Error("NewLimitSkipNode with negative limit should error")
	}

	_, err = NewLimitSkipNode(scan, 10, -1)
	if err == nil {
		t.Error("NewLimitSkipNode with negative offset should error")
	}
}

// Test LimitSkipNode not open error
func TestLimitSkipNode_NotOpen(t *testing.T) {
	scan := NewEmptyScanNode()
	lsNode, _ := NewLimitSkipNode(scan, 10, 0)

	_, err := lsNode.Next()
	if err == nil {
		t.Error("Next should error when not open")
	}
}

// Test helper functions
func TestMinMaxInt64(t *testing.T) {
	tests := []struct {
		a, b     int64
		min, max int64
	}{
		{1, 2, 1, 2},
		{5, 3, 3, 5},
		{0, 0, 0, 0},
		{-1, 1, -1, 1},
		{-5, -10, -10, -5},
	}

	for _, tc := range tests {
		min := minInt64(tc.a, tc.b)
		max := maxInt64(tc.a, tc.b)
		if min != tc.min {
			t.Errorf("minInt64(%d, %d) = %d, want %d", tc.a, tc.b, min, tc.min)
		}
		if max != tc.max {
			t.Errorf("maxInt64(%d, %d) = %d, want %d", tc.a, tc.b, max, tc.max)
		}
	}
}

// Test CollScanNode Explain with Stats interface
func TestCollScanNode_Stats(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "scanstats"
	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	scan := NewCollScanNode(eng, db, coll)

	// Check it implements PlanNodeWithStats
	if _, ok := interface{}(scan).(PlanNodeWithStats); !ok {
		t.Error("CollScanNode should implement PlanNodeWithStats")
	}

	ctx := context.Background()
	Exec(ctx, scan)

	stats := scan.Stats()
	if stats.RowsOut != 5 {
		t.Errorf("Stats RowsOut = %d, want 5", stats.RowsOut)
	}
}

// Test SortNode Stats
func TestSortNode_Stats(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "sortstats"
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

	scan := NewCollScanNode(eng, db, coll)
	sortSpec := bson.D("value", bson.VInt32(1))
	sortNode, err := NewSortNode(scan, sortSpec)
	if err != nil {
		t.Fatalf("create sort: %v", err)
	}

	// Check it implements PlanNodeWithStats
	if _, ok := interface{}(sortNode).(PlanNodeWithStats); !ok {
		t.Error("SortNode should implement PlanNodeWithStats")
	}

	ctx := context.Background()
	Exec(ctx, sortNode)

	stats := sortNode.Stats()
	if stats.RowsOut != 5 {
		t.Errorf("Stats RowsOut = %d, want 5", stats.RowsOut)
	}
}

// Test CollScanNode WithFilter, SetFilter, Filter, WithProjection
func TestCollScanNode_FilterMethods(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "filtertest"

	// Insert test documents
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		doc.Set("category", bson.VString("A"))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Test WithFilter
	scan := NewCollScanNode(eng, db, coll)
	filter := bson.NewDocument()
	filter.Set("value", bson.VInt32(5))
	scan.WithFilter(filter)

	if scan.Filter() == nil {
		t.Error("Filter should be set after WithFilter")
	}

	// Test SetFilter
	scan2 := NewCollScanNode(eng, db, coll)
	customFilter := func(doc *bson.Document) bool {
		return true
	}
	scan2.SetFilter(customFilter)

	if scan2.Filter() == nil {
		t.Error("Filter should be set after SetFilter")
	}

	// Test WithProjection
	scan3 := NewCollScanNode(eng, db, coll)
	proj := bson.NewDocument()
	proj.Set("value", bson.VInt32(1))
	scan3.WithProjection(proj)

	ctx := context.Background()
	err = scan3.Open(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	doc, err := scan3.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if doc == nil {
		t.Fatal("expected document")
	}

	// Should have value field
	if _, ok := doc.Get("value"); !ok {
		t.Error("document should have value field")
	}

	// Should not have category field (projected out)
	if _, ok := doc.Get("category"); ok {
		t.Error("document should not have category field after projection")
	}

	scan3.Close()
}

// Test EmptyScanNode Explain
func TestEmptyScanNode_Explain(t *testing.T) {
	empty := NewEmptyScanNode()

	explain := empty.Explain()
	if explain.NodeType != "EMPTY" {
		t.Errorf("NodeType = %s, want EMPTY", explain.NodeType)
	}
	if explain.EstRows != 0 {
		t.Errorf("EstRows = %d, want 0", explain.EstRows)
	}

	// Empty scan should return nil on Next
	err := empty.Open(context.Background())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	doc, err := empty.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if doc != nil {
		t.Error("Empty scan should return nil document")
	}

	empty.Close()
}

// Test LimitNode Stats
func TestLimitNode_Stats(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	opts.MemtableSize = 1024 * 1024
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "limitstats"

	// Insert 10 documents
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	scan := NewCollScanNode(eng, db, coll)
	limitNode, err := NewLimitNode(scan, 5)
	if err != nil {
		t.Fatalf("create limit: %v", err)
	}

	ctx := context.Background()
	Exec(ctx, limitNode)

	stats := limitNode.Stats()
	if stats.RowsOut != 5 {
		t.Errorf("Stats RowsOut = %d, want 5", stats.RowsOut)
	}
}

// Test SortNode WithLimit (top-K optimization)
func TestSortNode_WithLimit(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "sortlimit"

	// Insert 10 documents
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	scan := NewCollScanNode(eng, db, coll)
	sortSpec := bson.D("value", bson.VInt32(-1)) // Descending
	sortNode, err := NewSortNode(scan, sortSpec)
	if err != nil {
		t.Fatalf("create sort: %v", err)
	}

	// Apply limit - should use topK optimization
	sortNode.WithLimit(3)

	ctx := context.Background()
	err = sortNode.Open(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Should only get top 3 (highest values)
	count := 0
	for {
		doc, err := sortNode.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if doc == nil {
			break
		}
		count++
		val, _ := doc.Get("value")
		// With descending sort and limit 3, we should get values 9, 8, 7
		expected := int32(10 - count)
		if val.Int32() != expected {
			t.Errorf("doc %d: value = %d, want %d", count, val.Int32(), expected)
		}
	}

	if count != 3 {
		t.Errorf("got %d docs, want 3", count)
	}

	sortNode.Close()
}

// Test SortNode topK with fewer docs than limit
func TestSortNode_TopK_FewerDocs(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "sortfew"

	// Insert only 3 documents
	for i := 0; i < 3; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	scan := NewCollScanNode(eng, db, coll)
	sortSpec := bson.D("value", bson.VInt32(1))
	sortNode, err := NewSortNode(scan, sortSpec)
	if err != nil {
		t.Fatalf("create sort: %v", err)
	}

	// Limit larger than doc count
	sortNode.WithLimit(10)

	ctx := context.Background()
	err = sortNode.Open(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	count := 0
	for {
		doc, err := sortNode.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if doc == nil {
			break
		}
		count++
	}

	if count != 3 {
		t.Errorf("got %d docs, want 3", count)
	}

	sortNode.Close()
}

// Test SortNode Explain
func TestSortNode_Explain(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	scan := NewCollScanNode(eng, "test", "coll")
	sortSpec := bson.D("value", bson.VInt32(1))
	sortNode, err := NewSortNode(scan, sortSpec)
	if err != nil {
		t.Fatalf("create sort: %v", err)
	}

	// Test Explain before Open
	explain := sortNode.Explain()
	if explain.NodeType != "SORT" {
		t.Errorf("NodeType = %s, want SORT", explain.NodeType)
	}

	sortNode.Close()
}
