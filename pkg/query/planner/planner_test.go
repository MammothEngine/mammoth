package planner

import (
	"context"
	"os"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// mockIndexCatalog is a mock index catalog for testing.
type mockIndexCatalog struct {
	indexes map[string][]mongo.IndexSpec
}

func newMockIndexCatalog() *mockIndexCatalog {
	return &mockIndexCatalog{
		indexes: make(map[string][]mongo.IndexSpec),
	}
}

func (m *mockIndexCatalog) AddIndex(db, coll string, spec mongo.IndexSpec) {
	key := db + "." + coll
	m.indexes[key] = append(m.indexes[key], spec)
}

func (m *mockIndexCatalog) ListIndexes(db, coll string) ([]mongo.IndexSpec, error) {
	key := db + "." + coll
	return m.indexes[key], nil
}

func (m *mockIndexCatalog) GetIndex(db, coll, name string) (*mongo.IndexSpec, error) {
	key := db + "." + coll
	for i := range m.indexes[key] {
		if m.indexes[key][i].Name == name {
			return &m.indexes[key][i], nil
		}
	}
	return nil, mongo.ErrNotFound
}

// mockStatsManager is a mock stats manager for testing.
type mockStatsManager struct {
	stats map[string]*CollectionStats
}

func newMockStatsManager() *mockStatsManager {
	return &mockStatsManager{
		stats: make(map[string]*CollectionStats),
	}
}

func (m *mockStatsManager) GetStats(db, coll, component string) *CollectionStats {
	key := db + "." + coll + "." + component
	return m.stats[key]
}

func (m *mockStatsManager) Selectivity(db, coll, indexName string) float64 {
	return 0.1 // 10% selectivity
}

func TestPlannerPlanCollScan(t *testing.T) {
	// Create temp engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert test data
	db, coll := "test", "planner"
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

	// Create planner with mock catalogs
	indexCat := newMockIndexCatalog()
	statsMgr := newMockStatsManager()
	planner := NewPlanner(eng, indexCat, statsMgr)

	// Plan a simple query with filter
	filter := bson.NewDocument()
	gteDoc := bson.NewDocument()
	gteDoc.Set("$gte", bson.VInt32(5))
	filter.Set("value", bson.VDoc(gteDoc))

	planOpts := PlanOptions{
		Filter: filter,
	}

	plan, err := planner.Plan(context.Background(), db, coll, planOpts)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	// Execute the plan
	results, err := ExecPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 docs (value >= 5), got %d", len(results))
	}
}

func TestPlannerPlanWithIndex(t *testing.T) {
	// Create temp engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert test data
	db, coll := "test", "indexed"
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

	// Create planner with index catalog that has an index on "value"
	indexCat := newMockIndexCatalog()
	indexCat.AddIndex(db, coll, mongo.IndexSpec{
		Name: "idx_value",
		Key:  []mongo.IndexKey{{Field: "value", Descending: false}},
	})

	statsMgr := newMockStatsManager()
	planner := NewPlanner(eng, indexCat, statsMgr)

	// Plan a query that should use the index
	filter := bson.NewDocument()
	filter.Set("value", bson.VInt32(5))

	planOpts := PlanOptions{
		Filter: filter,
	}

	plan, err := planner.Plan(context.Background(), db, coll, planOpts)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	// Check that it's an index scan (the planner should prefer index)
	explain := plan.Explain()
	t.Logf("Explain: %+v", explain)

	// Note: Index scan returns 0 results because actual index entries
	// don't exist in the engine. This test verifies index selection logic.
	// Full index integration would require inserting actual index entries.
	if explain.NodeType != "IXSCAN" {
		t.Errorf("expected IXSCAN, got %s", explain.NodeType)
	}

	if explain.Details["indexName"] != "idx_value" {
		t.Errorf("expected index idx_value, got %v", explain.Details["indexName"])
	}
}

func TestPlannerPlanWithSortOnly(t *testing.T) {
	// Create temp engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert test data in reverse order
	db, coll := "test", "sorted"
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

	// Create planner
	indexCat := newMockIndexCatalog()
	statsMgr := newMockStatsManager()
	planner := NewPlanner(eng, indexCat, statsMgr)

	// Plan a query with sort
	sort := bson.NewDocument()
	sort.Set("value", bson.VInt32(1)) // ascending

	planOpts := PlanOptions{
		Sort: sort,
	}

	plan, err := planner.Plan(context.Background(), db, coll, planOpts)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	// Execute the plan
	results, err := ExecPlan(context.Background(), plan)
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

func TestPlannerPlanWithLimit(t *testing.T) {
	// Create temp engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert test data
	db, coll := "test", "limited"
	for i := 0; i < 100; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Create planner
	indexCat := newMockIndexCatalog()
	statsMgr := newMockStatsManager()
	planner := NewPlanner(eng, indexCat, statsMgr)

	// Plan a query with limit
	planOpts := PlanOptions{
		Limit: 10,
	}

	plan, err := planner.Plan(context.Background(), db, coll, planOpts)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	// Execute the plan
	results, err := ExecPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	if len(results) != 10 {
		t.Errorf("expected 10 docs (limit), got %d", len(results))
	}
}

func TestPlannerPlanWithSkip(t *testing.T) {
	// Create temp engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Insert test data
	db, coll := "test", "skipped"
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

	// Create planner
	indexCat := newMockIndexCatalog()
	statsMgr := newMockStatsManager()
	planner := NewPlanner(eng, indexCat, statsMgr)

	// Plan a query with skip and limit
	planOpts := PlanOptions{
		Skip:  5,
		Limit: 5,
	}

	plan, err := planner.Plan(context.Background(), db, coll, planOpts)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	// Execute the plan
	results, err := ExecPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 docs (skip 5, limit 5), got %d", len(results))
	}
}

// ExecPlan is a helper to execute a plan node and collect results.
func ExecPlan(ctx context.Context, node interface {
	Open(ctx context.Context) error
	Next() (*bson.Document, error)
	Close() error
}) ([]*bson.Document, error) {
	if err := node.Open(ctx); err != nil {
		return nil, err
	}
	defer node.Close()

	var results []*bson.Document
	for {
		doc, err := node.Next()
		if err != nil {
			return nil, err
		}
		if doc == nil {
			break
		}
		results = append(results, doc)
	}
	return results, nil
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

// Test Plan with projection
func TestPlannerPlanWithProjection(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "projected"
	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("name", bson.VString("test"))
		doc.Set("value", bson.VInt32(int32(i)))
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	indexCat := newMockIndexCatalog()
	statsMgr := newMockStatsManager()
	planner := NewPlanner(eng, indexCat, statsMgr)

	// Create projection
	proj := bson.NewDocument()
	proj.Set("name", bson.VInt32(1))

	planOpts := PlanOptions{
		Projection: proj,
	}

	plan, err := planner.Plan(context.Background(), db, coll, planOpts)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	results, err := ExecPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 docs, got %d", len(results))
	}
}

// Test Plan with sort
func TestPlannerPlanWithSortNode(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "sorted"
	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		id := bson.NewObjectID()
		doc.Set("_id", bson.VObjectID(id))
		doc.Set("value", bson.VInt32(int32(5-i))) // reverse order
		key := mongo.EncodeDocumentKey(db, coll, id[:])
		if err := eng.Put(key, bson.Encode(doc)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	indexCat := newMockIndexCatalog()
	statsMgr := newMockStatsManager()
	planner := NewPlanner(eng, indexCat, statsMgr)

	// Create sort
	sort := bson.NewDocument()
	sort.Set("value", bson.VInt32(1))

	planOpts := PlanOptions{
		Sort: sort,
	}

	plan, err := planner.Plan(context.Background(), db, coll, planOpts)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	results, err := ExecPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 docs, got %d", len(results))
	}
}

// Test Plan with filter and no index (should use filter node)
func TestPlannerPlanWithFilterNoIndex(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	db, coll := "test", "filtered"
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

	indexCat := newMockIndexCatalog()
	statsMgr := newMockStatsManager()
	planner := NewPlanner(eng, indexCat, statsMgr)

	// Create filter
	filter := bson.NewDocument()
	filter.Set("value", bson.VInt32(5))

	planOpts := PlanOptions{
		Filter: filter,
	}

	plan, err := planner.Plan(context.Background(), db, coll, planOpts)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	results, err := ExecPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 doc with value=5, got %d", len(results))
	}
}
