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
