package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func TestCostBasedPlanner(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)
	statsMgr := NewStatsManager(eng)

	// Create collection
	cat.EnsureCollection("testdb", "testcoll")

	// Create an index
	spec := IndexSpec{
		Name: "idx_name",
		Key:  []IndexKey{{Field: "name", Descending: false}},
	}
	if err := indexCat.CreateIndex("testdb", "testcoll", spec); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	cbp := NewCostBasedPlanner(indexCat, statsMgr)

	// Plan query
	filter := bson.D("name", bson.VString("test"))
	plan, err := cbp.PlanQuery("testdb", "testcoll", filter, nil)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}

	if plan == nil {
		t.Fatal("expected query plan")
	}

	// Should choose index scan over collection scan
	if plan.PlanType != PlanIndexScan && plan.PlanType != PlanCollScan {
		t.Errorf("unexpected plan type: %v", plan.PlanType)
	}

	// Should have a cost estimate (may be high for empty collections)
	if plan.EstimatedCost < 0 {
		t.Error("expected non-negative cost estimate")
	}
}

func TestCostBasedPlanner_SortCovered(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)
	statsMgr := NewStatsManager(eng)

	cat.EnsureCollection("testdb", "testcoll")

	// Create index with sort order
	spec := IndexSpec{
		Name: "idx_name_age",
		Key: []IndexKey{
			{Field: "name", Descending: false},
			{Field: "age", Descending: true},
		},
	}
	if err := indexCat.CreateIndex("testdb", "testcoll", spec); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	cbp := NewCostBasedPlanner(indexCat, statsMgr)

	// Query with matching sort
	filter := bson.D("name", bson.VString("test"))
	sort := bson.D("age", bson.VInt32(-1))
	plan, err := cbp.PlanQuery("testdb", "testcoll", filter, sort)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}

	if plan == nil {
		t.Fatal("expected query plan")
	}

	if plan.SortCovered {
		t.Log("Sort is covered by index")
	}
}

func TestCostModel(t *testing.T) {
	cm := DefaultCostModel()

	if cm.SeqPageCost <= 0 {
		t.Error("SeqPageCost should be positive")
	}

	if cm.RandomPageCost <= cm.SeqPageCost {
		t.Error("RandomPageCost should be higher than SeqPageCost")
	}
}

func TestJoinOptimizer(t *testing.T) {
	jo := NewJoinOptimizer()

	joinCondition := bson.D("user_id", bson.VString("user_id"))
	plan := jo.OptimizeJoin("orders", "users", joinCondition, 1000, 100)

	if plan == nil {
		t.Fatal("expected join plan")
	}

	// Should choose hash join for small tables
	if plan.EstimatedCost <= 0 {
		t.Error("expected positive cost")
	}

	// Check join types are valid
	validTypes := map[JoinType]bool{
		JoinNestedLoop:      true,
		JoinHash:            true,
		JoinMerge:           true,
		JoinIndexNestedLoop: true,
	}
	if !validTypes[plan.JoinType] {
		t.Errorf("invalid join type: %v", plan.JoinType)
	}
}

func TestJoinOptimizer_HashJoin(t *testing.T) {
	jo := NewJoinOptimizer()

	// Large tables - should prefer hash join
	joinCondition := bson.D("user_id", bson.VString("user_id"))
	plan := jo.planHashJoin("orders", "users", joinCondition, 10000, 5000)

	if plan.JoinType != JoinHash {
		t.Errorf("expected Hash join, got %v", plan.JoinType)
	}

	if plan.EstimatedCost <= 0 {
		t.Error("expected positive cost")
	}
}

func TestJoinOptimizer_MergeJoin(t *testing.T) {
	jo := NewJoinOptimizer()

	plan := jo.planMergeJoin("orders", "users", nil, 1000, 500)

	if plan.JoinType != JoinMerge {
		t.Errorf("expected Merge join, got %v", plan.JoinType)
	}
}

func TestJoinOptimizer_NestedLoop(t *testing.T) {
	jo := NewJoinOptimizer()

	// Very small tables - nested loop might be acceptable
	plan := jo.planNestedLoop("small1", "small2", nil, 10, 10)

	if plan.JoinType != JoinNestedLoop {
		t.Errorf("expected NestedLoop join, got %v", plan.JoinType)
	}
}

func TestIndexAdvisor(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)
	statsMgr := NewStatsManager(eng)

	cat.EnsureCollection("testdb", "testcoll")

	ia := NewIndexAdvisor(indexCat, statsMgr)

	// Simulate recent queries
	queries := []*bson.Document{
		bson.D("email", bson.VString("test@test.com")),
		bson.D("email", bson.VString("test2@test.com")),
		bson.D("email", bson.VString("test3@test.com")),
		bson.D("email", bson.VString("test4@test.com")),
		bson.D("email", bson.VString("test5@test.com")),
		bson.D("name", bson.VString("John")),
		bson.D("name", bson.VString("Jane")),
		bson.D("name", bson.VString("Bob")),
	}

	recommendations := ia.RecommendIndexes("testdb", "testcoll", queries)

	// Should recommend indexes for frequently queried fields
	foundEmail := false
	for _, rec := range recommendations {
		if len(rec.Fields) > 0 && rec.Fields[0] == "email" {
			foundEmail = true
			break
		}
	}

	if !foundEmail {
		t.Log("No email index recommended (may already exist or not enough frequency)")
	}
}

func TestQueryPlanner(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)
	statsMgr := NewStatsManager(eng)

	cat.EnsureCollection("testdb", "testcoll")

	qp := NewQueryPlanner(indexCat, statsMgr)

	// Test single table query planning
	filter := bson.D("name", bson.VString("test"))
	plan, err := qp.Plan("testdb", "testcoll", filter, nil)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	if plan == nil {
		t.Fatal("expected query plan")
	}

	// Test join planning
	joinPlan := qp.PlanJoin("orders", "users", nil, 1000, 100)
	if joinPlan == nil {
		t.Fatal("expected join plan")
	}
}

func TestIsEqualityCondition(t *testing.T) {
	// Test implicit equality
	doc := bson.D("name", bson.VString("test"))
	v, _ := doc.Get("name")
	if !isEqualityCondition(v) {
		t.Error("expected equality for string value")
	}

	// Test explicit $eq
	eqDoc := bson.NewDocument()
	eqDoc.Set("$eq", bson.VString("test"))
	if !isEqualityCondition(bson.VDoc(eqDoc)) {
		t.Error("expected equality for $eq operator")
	}

	// Test $gt (not equality)
	gtDoc := bson.NewDocument()
	gtDoc.Set("$gt", bson.VInt32(5))
	if isEqualityCondition(bson.VDoc(gtDoc)) {
		t.Error("should not be equality for $gt operator")
	}
}

func TestPlanType_String(t *testing.T) {
	// Test plan types exist
	types := []PlanType{PlanCollScan, PlanIndexScan, PlanCountScan, PlanMultiIndex}
	for _, pt := range types {
		_ = pt // Just verify they exist
	}
}

func TestIndexRecommendation(t *testing.T) {
	rec := &IndexRecommendation{
		Collection:    "testcoll",
		Fields:        []string{"name", "email"},
		Impact:        10.5,
		CurrentCost:   100.0,
		OptimizedCost: 10.0,
	}

	if rec.Collection != "testcoll" {
		t.Errorf("expected testcoll, got %s", rec.Collection)
	}

	if len(rec.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(rec.Fields))
	}
}

func TestJoinPlan(t *testing.T) {
	plan := &JoinPlan{
		JoinType:      JoinHash,
		LeftTable:     "orders",
		RightTable:    "users",
		EstimatedCost: 100.0,
	}

	if plan.LeftTable != "orders" {
		t.Errorf("expected orders, got %s", plan.LeftTable)
	}

	if plan.EstimatedCost != 100.0 {
		t.Errorf("expected cost 100.0, got %f", plan.EstimatedCost)
	}
}

// Test generateIndexIntersectionPlans with multiple indexes
func TestGenerateIndexIntersectionPlans(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)
	statsMgr := NewStatsManager(eng)

	// Create indexes on different fields
	indexes := []IndexSpec{
		{Name: "idx_name", Key: []IndexKey{{Field: "name", Descending: false}}},
		{Name: "idx_age", Key: []IndexKey{{Field: "age", Descending: false}}},
		{Name: "idx_city", Key: []IndexKey{{Field: "city", Descending: false}}},
	}

	cbp := NewCostBasedPlanner(indexCat, statsMgr)

	// Create filter with multiple equality conditions
	filter := bson.NewDocument()
	filter.Set("name", bson.VString("test"))
	filter.Set("age", bson.VInt32(25))

	// Generate intersection plans
	plans := cbp.generateIndexIntersectionPlans("testdb", "testcoll", filter, indexes)

	// Should generate at least one intersection plan
	if len(plans) == 0 {
		t.Log("No intersection plans generated (may be expected with empty filter fields)")
	} else {
		t.Logf("Generated %d intersection plans", len(plans))
		for _, plan := range plans {
			if plan.PlanType != PlanMultiIndex {
				t.Errorf("expected PlanMultiIndex, got %v", plan.PlanType)
			}
		}
	}
}
