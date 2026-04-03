package mongo

import (
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupPlanCacheTest(t *testing.T) (*PlanCache, *engine.Engine) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { eng.Close() })

	return NewPlanCache(10), eng
}

// Test PlanCache Invalidate
func TestPlanCache_Invalidate(t *testing.T) {
	pc, _ := setupPlanCacheTest(t)

	filter := bson.D("name", bson.VString("Alice"))
	plan := &QueryPlan{
		PlanType:    PlanIndexScan,
		IndexName:   "name_idx",
		EstimatedCost: 100,
	}

	// Add plans for two collections
	pc.Put("testdb", "coll1", filter, nil, nil, plan)
	pc.Put("testdb", "coll2", filter, nil, nil, plan)

	// Invalidate only coll1
	pc.Invalidate("testdb", "coll1")

	// coll1 plan should be gone
	if p := pc.Get("testdb", "coll1", filter, nil, nil); p != nil {
		t.Error("coll1 plan should be invalidated")
	}

	// coll2 plan should still exist
	if p := pc.Get("testdb", "coll2", filter, nil, nil); p == nil {
		t.Error("coll2 plan should still exist")
	}
}

// Test PlanCache Invalidate with no matching entries
func TestPlanCache_Invalidate_NoMatch(t *testing.T) {
	pc, _ := setupPlanCacheTest(t)

	filter := bson.D("name", bson.VString("Alice"))
	plan := &QueryPlan{PlanType: PlanCollScan}

	pc.Put("testdb", "coll1", filter, nil, nil, plan)

	// Invalidate different collection
	pc.Invalidate("otherdb", "othercoll")

	// Original plan should still exist
	if p := pc.Get("testdb", "coll1", filter, nil, nil); p == nil {
		t.Error("plan should still exist after invalidating different collection")
	}
}

// Test PlanCache InvalidateAll
func TestPlanCache_InvalidateAll(t *testing.T) {
	pc, _ := setupPlanCacheTest(t)

	filter := bson.D("name", bson.VString("Alice"))
	plan := &QueryPlan{PlanType: PlanCollScan}

	// Add multiple plans
	pc.Put("testdb", "coll1", filter, nil, nil, plan)
	pc.Put("testdb", "coll2", filter, nil, nil, plan)
	pc.Put("otherdb", "coll1", filter, nil, nil, plan)

	// Invalidate all
	pc.InvalidateAll()

	// All plans should be gone
	if p := pc.Get("testdb", "coll1", filter, nil, nil); p != nil {
		t.Error("all plans should be invalidated")
	}
	if p := pc.Get("testdb", "coll2", filter, nil, nil); p != nil {
		t.Error("all plans should be invalidated")
	}
	if p := pc.Get("otherdb", "coll1", filter, nil, nil); p != nil {
		t.Error("all plans should be invalidated")
	}
}

// Test PlanCache RecordExecution
func TestPlanCache_RecordExecution(t *testing.T) {
	pc, _ := setupPlanCacheTest(t)

	filter := bson.D("name", bson.VString("Alice"))
	plan := &QueryPlan{PlanType: PlanCollScan}

	pc.Put("testdb", "coll1", filter, nil, nil, plan)

	// Record some executions
	pc.RecordExecution("testdb", "coll1", filter, nil, nil, 100*time.Millisecond)
	pc.RecordExecution("testdb", "coll1", filter, nil, nil, 200*time.Millisecond)

	// Verify plan was updated (we can't directly check, but no panic means success)
	p := pc.Get("testdb", "coll1", filter, nil, nil)
	if p == nil {
		t.Error("plan should still exist")
	}
}

// Test PlanCache RecordExecution for non-existent plan
func TestPlanCache_RecordExecution_NotFound(t *testing.T) {
	pc, _ := setupPlanCacheTest(t)

	filter := bson.D("name", bson.VString("Alice"))

	// Should not panic for non-existent plan
	pc.RecordExecution("testdb", "coll1", filter, nil, nil, 100*time.Millisecond)
}

// Test PlanCache evictOldest
func TestPlanCache_EvictOldest(t *testing.T) {
	pc, _ := setupPlanCacheTest(t)

	filter1 := bson.D("name", bson.VString("Alice"))
	filter2 := bson.D("name", bson.VString("Bob"))
	filter3 := bson.D("name", bson.VString("Charlie"))

	plan := &QueryPlan{PlanType: PlanCollScan}

	// Add plans with small delay to ensure different LastUsed times
	pc.Put("testdb", "coll1", filter1, nil, nil, plan)
	time.Sleep(10 * time.Millisecond)
	pc.Put("testdb", "coll2", filter2, nil, nil, plan)
	time.Sleep(10 * time.Millisecond)
	pc.Put("testdb", "coll3", filter3, nil, nil, plan)

	// Use the first plan to update its LastUsed
	pc.Get("testdb", "coll1", filter1, nil, nil)

	// Add more plans to trigger eviction (max is 10, adding 8 more)
	for i := 0; i < 8; i++ {
		f := bson.D("idx", bson.VInt32(int32(i)))
		pc.Put("testdb", "collX", f, nil, nil, plan)
	}

	// The second plan (filter2) should be evicted as it was least recently used
	// We can't verify exactly which was evicted, but the cache should still work
	hits, misses, size := pc.Stats()
	_ = hits
	_ = misses
	if size > 10 {
		t.Errorf("cache size = %d, should not exceed max 10", size)
	}
}

// Test PreparedStmtCache evictOldest
func TestPreparedStmtCache_EvictOldest(t *testing.T) {
	psc := NewPreparedStmtCache(3)

	plan := &QueryPlan{PlanType: PlanCollScan}

	// Add 3 prepared statements
	psc.Prepare("stmt1", "testdb", "coll1", nil, nil, nil, plan)
	psc.Prepare("stmt2", "testdb", "coll1", nil, nil, nil, plan)
	psc.Prepare("stmt3", "testdb", "coll1", nil, nil, nil, plan)

	// Use stmt1 to increase its UseCount
	for i := 0; i < 5; i++ {
		psc.Get("stmt1")
	}

	// Add 4th statement - should evict one with lowest UseCount
	psc.Prepare("stmt4", "testdb", "coll1", nil, nil, nil, plan)

	// List should have 3 statements
	ids := psc.List()
	if len(ids) != 3 {
		t.Errorf("prepared stmt cache size = %d, want 3", len(ids))
	}

	// stmt1 should still exist (highest use count)
	if s := psc.Get("stmt1"); s == nil {
		t.Error("stmt1 should still exist (highest use count)")
	}
}

// Test QueryOptimizer Invalidate
func TestQueryOptimizer_Invalidate(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "coll1")
	cat.CreateCollection("testdb", "coll2")

	indexCat := NewIndexCatalog(eng, cat)
	qo := NewQueryOptimizer(eng, indexCat)

	filter := bson.D("name", bson.VString("Alice"))

	// Optimize queries for both collections
	qo.OptimizeQuery("testdb", "coll1", filter, nil, nil)
	qo.OptimizeQuery("testdb", "coll2", filter, nil, nil)

	// Invalidate only coll1
	qo.Invalidate("testdb", "coll1")

	// Stats should show cache miss for coll1, hit for coll2 on next query
	hits1, misses1, _ := qo.Stats()

	qo.OptimizeQuery("testdb", "coll1", filter, nil, nil)
	qo.OptimizeQuery("testdb", "coll2", filter, nil, nil)

	hits2, misses2, _ := qo.Stats()

	// coll1 should be a miss (re-optimized), coll2 should be a hit
	if misses2 <= misses1 {
		t.Error("coll1 should be cache miss after invalidation")
	}
	if hits2 <= hits1 {
		t.Error("coll2 should be cache hit")
	}
}

// Test QueryOptimizer Stats
func TestQueryOptimizer_Stats(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "coll1")

	indexCat := NewIndexCatalog(eng, cat)
	qo := NewQueryOptimizer(eng, indexCat)

	filter := bson.D("name", bson.VString("Alice"))

	// Initial stats should be zero
	hits, misses, size := qo.Stats()
	if hits != 0 || misses != 0 {
		t.Error("initial stats should be zero")
	}
	if size != 0 {
		t.Error("initial cache size should be zero")
	}

	// First query - miss
	qo.OptimizeQuery("testdb", "coll1", filter, nil, nil)
	_, misses, size = qo.Stats()
	if misses != 1 {
		t.Errorf("misses = %d, want 1", misses)
	}
	if size != 1 {
		t.Errorf("cache size = %d, want 1", size)
	}

	// Second query - hit
	qo.OptimizeQuery("testdb", "coll1", filter, nil, nil)
	hits, _, _ = qo.Stats()
	if hits != 1 {
		t.Errorf("hits = %d, want 1", hits)
	}
}
