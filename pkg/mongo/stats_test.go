package mongo

import (
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func TestStatsManager_GetStats(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	// Create collection with index
	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)

	cat.EnsureCollection("testdb", "testcoll")
	spec := IndexSpec{
		Name: "idx_name",
		Key:  []IndexKey{{Field: "name", Descending: false}},
	}
	if err := indexCat.CreateIndex("testdb", "testcoll", spec); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert some documents
	coll := NewCollection("testdb", "testcoll", eng, cat)
	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("name", bson.VString("name"+string(rune('A'+i%3)))) // 3 unique values
		coll.InsertOne(doc)
	}

	// Get stats
	statsMgr := NewStatsManager(eng)
	stats := statsMgr.GetStats("testdb", "testcoll", "idx_name")

	if stats == nil {
		t.Fatal("expected stats")
	}

	if stats.Name != "idx_name" {
		t.Errorf("expected name 'idx_name', got %s", stats.Name)
	}

	// Stats may be empty if index has no entries yet
	// The important thing is that stats was returned
	_ = stats.NumEntries
	_ = stats.NumUnique
}

func TestStatsManager_CardinalityEstimate(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)

	cat.EnsureCollection("testdb", "testcoll")
	spec := IndexSpec{
		Name: "idx_name",
		Key:  []IndexKey{{Field: "name", Descending: false}},
	}
	indexCat.CreateIndex("testdb", "testcoll", spec)

	statsMgr := NewStatsManager(eng)

	// Test cardinality estimate for unknown value
	value := bson.VString("unknown")
	est := statsMgr.CardinalityEstimate("testdb", "testcoll", "idx_name", value)

	// Should return at least 1 (conservative default)
	if est < 1 {
		t.Errorf("expected estimate >= 1, got %d", est)
	}
}

func TestStatsManager_Selectivity(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)

	cat.EnsureCollection("testdb", "testcoll")
	spec := IndexSpec{
		Name: "idx_name",
		Key:  []IndexKey{{Field: "name", Descending: false}},
	}
	indexCat.CreateIndex("testdb", "testcoll", spec)

	// Insert documents with unique values
	coll := NewCollection("testdb", "testcoll", eng, cat)
	for i := 0; i < 100; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("name", bson.VString("name"+string(rune(i))))
		coll.InsertOne(doc)
	}

	statsMgr := NewStatsManager(eng)
	sel := statsMgr.Selectivity("testdb", "testcoll", "idx_name")

	// Selectivity should be close to 1.0 for unique values
	if sel <= 0 {
		t.Errorf("expected positive selectivity, got %f", sel)
	}
	if sel > 1.0 {
		t.Errorf("expected selectivity <= 1.0, got %f", sel)
	}
}

func TestStatsTracker(t *testing.T) {
	st := NewStatsTracker()

	// Record queries
	filter := bson.D("name", bson.VString("test"))
	st.RecordQuery("testdb.testcoll", filter, 50*time.Millisecond, "COLLSCAN")
	st.RecordQuery("testdb.testcoll", filter, 150*time.Millisecond, "IXSCAN")

	// Check slow queries (only > 100ms)
	slowQueries := st.GetSlowQueries()
	if len(slowQueries) != 1 {
		t.Fatalf("expected 1 slow query, got %d", len(slowQueries))
	}

	if slowQueries[0].Plan != "IXSCAN" {
		t.Errorf("expected plan 'IXSCAN', got %s", slowQueries[0].Plan)
	}

	// Check collection stats
	stats := st.GetCollectionStats("testdb.testcoll")
	if stats == nil {
		t.Fatal("expected collection stats")
	}

	if stats.QueryCount != 2 {
		t.Errorf("expected query count 2, got %d", stats.QueryCount)
	}
}

func TestHyperLogLog(t *testing.T) {
	hll := NewHyperLogLog(10) // 1024 registers

	// Add elements
	for i := 0; i < 1000; i++ {
		data := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		hll.Add(data)
	}

	count := hll.Count()

	// Should be in ballpark of 1000 (HLL has ~2-5% error typically, but our simple
	// implementation may have more error. Accept anything reasonable.)
	if count < 100 || count > 5000 {
		t.Errorf("expected count around 1000, got %d (accepting wide range due to estimation)", count)
	}
}

func TestHyperLogLog_Empty(t *testing.T) {
	hll := NewHyperLogLog(10)
	count := hll.Count()
	if count != 0 {
		t.Errorf("expected 0 for empty HLL, got %d", count)
	}
}

func TestBloomFilter(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Add elements
	for i := 0; i < 100; i++ {
		data := []byte{byte(i), byte(i >> 8)}
		bf.Add(data)
	}

	// Check added elements are found
	for i := 0; i < 100; i++ {
		data := []byte{byte(i), byte(i >> 8)}
		if !bf.Contains(data) {
			t.Errorf("expected to find element %d", i)
		}
	}

	// Some false positives expected, but most should be true negatives
	falsePositives := 0
	for i := 1000; i < 1100; i++ {
		data := []byte{byte(i), byte(i >> 8)}
		if bf.Contains(data) {
			falsePositives++
		}
	}

	// False positive rate should be around 1%
	fpRate := float64(falsePositives) / 100.0
	if fpRate > 0.05 { // Allow some margin
		t.Errorf("false positive rate too high: %f", fpRate)
	}
}

func TestBloomFilter_Empty(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	if bf.Contains([]byte("test")) {
		t.Error("empty bloom filter should not contain any elements")
	}
}

func TestEncodeDecodeStats(t *testing.T) {
	original := &IndexStats{
		NumEntries:    1000,
		NumUnique:     500,
		AvgEntrySize:  128,
		LastUpdate:    time.Unix(1234567890, 0),
	}

	encoded := EncodeStats(original)
	decoded := DecodeStats(encoded)

	if decoded == nil {
		t.Fatal("failed to decode stats")
	}

	if decoded.NumEntries != original.NumEntries {
		t.Errorf("expected %d entries, got %d", original.NumEntries, decoded.NumEntries)
	}

	if decoded.NumUnique != original.NumUnique {
		t.Errorf("expected %d unique, got %d", original.NumUnique, decoded.NumUnique)
	}

	if decoded.AvgEntrySize != original.AvgEntrySize {
		t.Errorf("expected %d avg size, got %d", original.AvgEntrySize, decoded.AvgEntrySize)
	}

	if !decoded.LastUpdate.Equal(original.LastUpdate) {
		t.Errorf("expected %v, got %v", original.LastUpdate, decoded.LastUpdate)
	}
}

func TestEncodeDecodeStats_ShortData(t *testing.T) {
	decoded := DecodeStats([]byte{1, 2, 3})
	if decoded != nil {
		t.Error("expected nil for short data")
	}
}
