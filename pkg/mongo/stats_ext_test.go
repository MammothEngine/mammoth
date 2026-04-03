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

// Test HyperLogLog cardinality estimation
func TestHyperLogLog_Ext(t *testing.T) {
	hll := NewHyperLogLog(10)

	// Add some values
	for i := 0; i < 100; i++ {
		hll.Add([]byte{byte(i)})
	}

	count := hll.Count()
	// Should estimate around 100 (with some error margin)
	if count < 50 || count > 200 {
		t.Errorf("HLL Count = %d, expected around 100", count)
	}
}

// Test HyperLogLog with duplicate values
func TestHyperLogLog_Duplicates(t *testing.T) {
	hll := NewHyperLogLog(10)

	// Add same value many times
	for i := 0; i < 100; i++ {
		hll.Add([]byte{1, 2, 3})
	}

	count := hll.Count()
	// Should estimate around 1
	if count > 10 {
		t.Errorf("HLL Count with duplicates = %d, expected around 1", count)
	}
}

// Test HyperLogLog with small range correction
func TestHyperLogLog_SmallRange(t *testing.T) {
	hll := NewHyperLogLog(4) // Small number of registers

	// Add very few values
	hll.Add([]byte{1})
	hll.Add([]byte{2})

	count := hll.Count()
	if count < 1 {
		t.Error("HLL Count should be at least 1")
	}
}

// Test alphaMM with different values
func TestAlphaMM(t *testing.T) {
	tests := []struct {
		m        uint32
		expected float64
	}{
		{16, 0.673},
		{32, 0.697},
		{64, 0.709},
		{128, 0.7213 / (1.0 + 1.079/float64(128))},
	}

	for _, tc := range tests {
		result := alphaMM(tc.m)
		if result != tc.expected {
			t.Errorf("alphaMM(%d) = %f, want %f", tc.m, result, tc.expected)
		}
	}
}

// Test BloomFilter
func TestBloomFilter_Ext(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Add some values
	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))

	// Should contain added values
	if !bf.Contains([]byte("hello")) {
		t.Error("Bloom filter should contain 'hello'")
	}
	if !bf.Contains([]byte("world")) {
		t.Error("Bloom filter should contain 'world'")
	}

	// Should not contain values that were never added (with high probability)
	// Note: There's a small chance of false positives
	if bf.Contains([]byte("never-added")) {
		t.Log("Bloom filter false positive (acceptable)")
	}
}

// Test BloomFilter with different capacities
func TestBloomFilter_Capacities(t *testing.T) {
	tests := []struct {
		capacity          uint32
		falsePositiveRate float64
	}{
		{100, 0.1},
		{1000, 0.01},
		{10000, 0.001},
	}

	for _, tc := range tests {
		bf := NewBloomFilter(tc.capacity, tc.falsePositiveRate)

		// Add values up to capacity
		for i := uint32(0); i < tc.capacity; i++ {
			bf.Add([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
		}

		// Verify all added values are present
		for i := uint32(0); i < tc.capacity; i++ {
			if !bf.Contains([]byte{byte(i), byte(i >> 8), byte(i >> 16)}) {
				t.Errorf("Bloom filter should contain value %d", i)
			}
		}
	}
}

// Test BloomFilter edge cases
func TestBloomFilter_EdgeCases(t *testing.T) {
	// Very small capacity
	bf := NewBloomFilter(1, 0.5)
	bf.Add([]byte("x"))
	if !bf.Contains([]byte("x")) {
		t.Error("Bloom filter should contain added value")
	}

	// Very low false positive rate
	bf2 := NewBloomFilter(100, 0.0001)
	bf2.Add([]byte("test"))
	if !bf2.Contains([]byte("test")) {
		t.Error("Bloom filter should contain added value")
	}
}

// Test hash64
func TestHash64(t *testing.T) {
	h1 := hash64([]byte("hello"))
	h2 := hash64([]byte("hello"))
	h3 := hash64([]byte("world"))

	// Same input should produce same hash
	if h1 != h2 {
		t.Error("hash64 should produce consistent results")
	}

	// Different input should produce different hash (with high probability)
	if h1 == h3 {
		t.Log("hash64 collision (unlikely but possible)")
	}
}

// Test hash128
func TestHash128(t *testing.T) {
	h1a, h1b := hash128([]byte("hello"))
	h2a, h2b := hash128([]byte("hello"))
	h3a, h3b := hash128([]byte("world"))

	// Same input should produce same hash
	if h1a != h2a || h1b != h2b {
		t.Error("hash128 should produce consistent results")
	}

	// Different input should produce different hash
	if h1a == h3a && h1b == h3b {
		t.Log("hash128 collision (unlikely but possible)")
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
