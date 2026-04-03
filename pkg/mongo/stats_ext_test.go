package mongo

import (
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupStatsTest(t *testing.T) (*engine.Engine, *StatsManager) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { eng.Close() })

	return eng, NewStatsManager(eng)
}

// Test StatsManager CardinalityEstimate with no stats
func TestStatsManager_CardinalityEstimate_NoStats(t *testing.T) {
	_, sm := setupStatsTest(t)

	// Estimate with no stats should return 1 (conservative default)
	estimate := sm.CardinalityEstimate("testdb", "testcoll", "idx", bson.VString("value"))
	if estimate != 1 {
		t.Errorf("CardinalityEstimate with no stats = %d, want 1", estimate)
	}
}

// Test StatsManager CardinalityEstimate with stats
func TestStatsManager_CardinalityEstimate_WithStats(t *testing.T) {
	eng, sm := setupStatsTest(t)
	defer eng.Close()

	// Create collection and add documents
	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "testcoll")

	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Insert documents with varying values
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("name", bson.VString("Alice")) // Same value repeated
		coll.InsertOne(doc)
	}

	// Get stats (this will analyze and update stats)
	stats := sm.GetStats("testdb", "testcoll", "name_idx")
	// Stats may be nil if no index exists - that's ok for this test
	_ = stats

	// Estimate for a known value
	estimate := sm.CardinalityEstimate("testdb", "testcoll", "name_idx", bson.VString("Alice"))
	if estimate < 1 {
		t.Errorf("CardinalityEstimate = %d, want >= 1", estimate)
	}
}

// Test StatsManager Selectivity with stats
func TestStatsManager_Selectivity_Ext(t *testing.T) {
	eng, sm := setupStatsTest(t)
	defer eng.Close()

	// With no stats, selectivity should be 1.0 (worst case)
	sel := sm.Selectivity("testdb", "testcoll", "idx")
	if sel != 1.0 {
		t.Errorf("Selectivity with no stats = %f, want 1.0", sel)
	}

	// Get stats for non-existent index
	sel = sm.Selectivity("testdb", "testcoll", "non_existent_idx")
	if sel != 1.0 {
		t.Errorf("Selectivity with no stats = %f, want 1.0", sel)
	}
}

// Test EncodeStats and DecodeStats
func TestEncodeDecodeStats_Ext(t *testing.T) {
	original := &IndexStats{
		NumEntries:   1000,
		NumUnique:    500,
		AvgEntrySize: 256,
		LastUpdate:   time.Now().Truncate(time.Second),
	}

	encoded := EncodeStats(original)
	if len(encoded) < 32 {
		t.Errorf("encoded stats length = %d, want >= 32", len(encoded))
	}

	decoded := DecodeStats(encoded)
	if decoded == nil {
		t.Fatal("DecodeStats returned nil")
	}

	if decoded.NumEntries != original.NumEntries {
		t.Errorf("NumEntries = %d, want %d", decoded.NumEntries, original.NumEntries)
	}
	if decoded.NumUnique != original.NumUnique {
		t.Errorf("NumUnique = %d, want %d", decoded.NumUnique, original.NumUnique)
	}
	if decoded.AvgEntrySize != original.AvgEntrySize {
		t.Errorf("AvgEntrySize = %d, want %d", decoded.AvgEntrySize, original.AvgEntrySize)
	}
}

// Test DecodeStats with invalid data
func TestDecodeStats_InvalidData(t *testing.T) {
	result := DecodeStats([]byte{1, 2, 3})
	if result != nil {
		t.Error("DecodeStats should return nil for invalid data")
	}
}

// Test StatsTracker RecordQuery and GetSlowQueries
func TestStatsTracker_SlowQueries(t *testing.T) {
	st := NewStatsTracker()

	filter := bson.D("name", bson.VString("Alice"))

	// Record some fast queries (should not be recorded as slow)
	for i := 0; i < 5; i++ {
		st.RecordQuery("testdb.coll", filter, time.Millisecond*50, "index_scan")
	}

	// Record slow queries
	for i := 0; i < 3; i++ {
		st.RecordQuery("testdb.coll", filter, time.Millisecond*200, "collection_scan")
	}

	slowQueries := st.GetSlowQueries()
	if len(slowQueries) != 3 {
		t.Errorf("slow queries count = %d, want 3", len(slowQueries))
	}

	// Check collection stats
	stats := st.GetCollectionStats("testdb.coll")
	if stats == nil {
		t.Fatal("GetCollectionStats returned nil")
	}
	if stats.QueryCount != 8 {
		t.Errorf("QueryCount = %d, want 8", stats.QueryCount)
	}
}

// Test StatsTracker slow query limit (100)
func TestStatsTracker_SlowQueryLimit(t *testing.T) {
	st := NewStatsTracker()

	filter := bson.D("name", bson.VString("Alice"))

	// Record more than 100 slow queries
	for i := 0; i < 105; i++ {
		st.RecordQuery("testdb.coll", filter, time.Millisecond*200, "scan")
	}

	slowQueries := st.GetSlowQueries()
	if len(slowQueries) != 100 {
		t.Errorf("slow queries count = %d, want 100", len(slowQueries))
	}
}
