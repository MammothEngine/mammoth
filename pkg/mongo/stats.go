package mongo

import (
	"encoding/binary"
	"math"
	"sync"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// IndexStats tracks statistics for an index.
type IndexStats struct {
	Name           string
	DB             string
	Collection     string
	NumEntries     int64     // Total index entries
	NumUnique      int64     // Number of unique values
	AvgEntrySize   int64     // Average entry size in bytes
	LastUpdate     time.Time // Last time stats were updated
	SampleData     map[string]int64 // Sample of value frequencies (hashed values)
}

// StatsManager manages index statistics for query optimization.
type StatsManager struct {
	mu       sync.RWMutex
	engine   *engine.Engine
	stats    map[string]*IndexStats // key: db.coll.indexName
	sampleSize int                  // Number of samples to keep per index
}

// NewStatsManager creates a new statistics manager.
func NewStatsManager(eng *engine.Engine) *StatsManager {
	return &StatsManager{
		engine:     eng,
		stats:      make(map[string]*IndexStats),
		sampleSize: 1000,
	}
}

// GetStats returns statistics for an index.
func (sm *StatsManager) GetStats(db, coll, indexName string) *IndexStats {
	key := db + "." + coll + "." + indexName
	sm.mu.RLock()
	stats := sm.stats[key]
	sm.mu.RUnlock()

	if stats != nil && time.Since(stats.LastUpdate) < time.Hour {
		return stats
	}

	// Update stats if stale or missing
	return sm.updateStats(db, coll, indexName)
}

// updateStats updates statistics for an index by sampling.
func (sm *StatsManager) updateStats(db, coll, indexName string) *IndexStats {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	key := db + "." + coll + "." + indexName

	// Build spec manually
	spec := &IndexSpec{Name: indexName}
	idx := NewIndex(db, coll, spec, sm.engine)

	stats := &IndexStats{
		Name:       indexName,
		DB:         db,
		Collection: coll,
		LastUpdate: time.Now(),
		SampleData: make(map[string]int64),
	}

	// Sample index entries
	prefix := idx.ScanPrefix()
	uniqueVals := make(map[string]bool)
	var totalSize int64
	var entryCount int64

	sm.engine.Scan(prefix, func(k, v []byte) bool {
		entryCount++
		totalSize += int64(len(k) + len(v))

		// Hash the key (excluding document ID suffix) for uniqueness tracking
		if len(k) > 24 {
			hashKey := string(k[:len(k)-24])
			uniqueVals[hashKey] = true

			// Sample for frequency estimation
			if entryCount <= int64(sm.sampleSize) {
				stats.SampleData[hashKey]++
			}
		}

		return entryCount < int64(sm.sampleSize*10) // Sample up to 10x sample size
	})

	stats.NumEntries = entryCount
	stats.NumUnique = int64(len(uniqueVals))
	if entryCount > 0 {
		stats.AvgEntrySize = totalSize / entryCount
	}

	sm.stats[key] = stats
	return stats
}

// CardinalityEstimate estimates the cardinality of an indexed field for a given value.
// Returns estimated number of documents matching the value.
func (sm *StatsManager) CardinalityEstimate(db, coll, indexName string, value bson.Value) int64 {
	stats := sm.GetStats(db, coll, indexName)
	if stats == nil || stats.NumEntries == 0 {
		return 1 // Conservative default
	}

	// Encode value for lookup
	encoded := encodeIndexValue(value)
	hashKey := string(encoded)

	// Check sample data first
	if freq, ok := stats.SampleData[hashKey]; ok {
		// Scale up from sample
		scale := float64(stats.NumEntries) / float64(len(stats.SampleData))
		return int64(float64(freq) * scale)
	}

	// Estimate using unique count ratio
	if stats.NumUnique > 0 {
		avgDocsPerValue := float64(stats.NumEntries) / float64(stats.NumUnique)
		return int64(math.Max(1, avgDocsPerValue))
	}

	return 1
}

// Selectivity returns the selectivity of an index (0.0 to 1.0).
// Lower is more selective (fewer documents per unique value).
func (sm *StatsManager) Selectivity(db, coll, indexName string) float64 {
	stats := sm.GetStats(db, coll, indexName)
	if stats == nil || stats.NumUnique == 0 {
		return 1.0 // Worst case: no selectivity
	}

	return float64(stats.NumUnique) / float64(stats.NumEntries)
}

// CollectionStats tracks statistics for a collection.
type CollectionStats struct {
	DocumentCount    int64
	AvgDocumentSize  int64
	DataSize         int64
	IndexSize        int64
	LastAccess       time.Time
	QueryCount       int64
}

// StatsTracker tracks runtime statistics.
type StatsTracker struct {
	mu         sync.RWMutex
	collections map[string]*CollectionStats
	slowQueries []SlowQuery
}

// SlowQuery represents a slow query for analysis.
type SlowQuery struct {
	Namespace string
	Filter    string
	Duration  time.Duration
	Timestamp time.Time
	Plan      string
}

// NewStatsTracker creates a new statistics tracker.
func NewStatsTracker() *StatsTracker {
	return &StatsTracker{
		collections: make(map[string]*CollectionStats),
		slowQueries: make([]SlowQuery, 0, 100),
	}
}

// RecordQuery records query execution statistics.
func (st *StatsTracker) RecordQuery(ns string, filter *bson.Document, duration time.Duration, plan string) {
	st.mu.Lock()
	defer st.mu.Unlock()

	// Update collection stats
	stats, ok := st.collections[ns]
	if !ok {
		stats = &CollectionStats{}
		st.collections[ns] = stats
	}
	stats.LastAccess = time.Now()
	stats.QueryCount++

	// Record slow queries (slower than 100ms)
	if duration > 100*time.Millisecond {
		filterStr := "{}"
		if filter != nil {
			filterStr = filter.String()
		}

		slow := SlowQuery{
			Namespace: ns,
			Filter:    filterStr,
			Duration:  duration,
			Timestamp: time.Now(),
			Plan:      plan,
		}

		st.slowQueries = append(st.slowQueries, slow)
		if len(st.slowQueries) > 100 {
			st.slowQueries = st.slowQueries[1:]
		}
	}
}

// GetSlowQueries returns recorded slow queries.
func (st *StatsTracker) GetSlowQueries() []SlowQuery {
	st.mu.RLock()
	defer st.mu.RUnlock()

	result := make([]SlowQuery, len(st.slowQueries))
	copy(result, st.slowQueries)
	return result
}

// GetCollectionStats returns statistics for a collection.
func (st *StatsTracker) GetCollectionStats(ns string) *CollectionStats {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.collections[ns]
}

// HyperLogLog is a simple cardinality estimator using HyperLogLog algorithm.
type HyperLogLog struct {
	registers []uint8
	m         uint32 // Number of registers
	alpha     float64
}

// NewHyperLogLog creates a new HLL estimator.
func NewHyperLogLog(precision uint8) *HyperLogLog {
	m := uint32(1 << precision)
	return &HyperLogLog{
		registers: make([]uint8, m),
		m:         m,
		alpha:     alphaMM(m),
	}
}

// Add adds a value to the estimator.
func (hll *HyperLogLog) Add(data []byte) {
	x := hash64(data)
	// Use first 'p' bits for register index (p = log2(m))
	p := uint32(0)
	for m := hll.m; m > 1; m >>= 1 {
		p++
	}
	j := uint32(x) & (hll.m - 1) // Register index
	// Remaining bits for zero counting
	w := x >> p

	// Count leading zeros in remaining bits
	lz := uint8(1)
	maxLz := uint8(64 - p)
	for i := uint8(63 - p); i >= 0 && i < 64; i-- {
		if (w>>i)&1 == 1 {
			break
		}
		lz++
		if lz > maxLz {
			break
		}
	}

	if lz > hll.registers[j] {
		hll.registers[j] = lz
	}
}

// Count returns the estimated cardinality.
func (hll *HyperLogLog) Count() uint64 {
	sum := 0.0
	for _, v := range hll.registers {
		sum += 1.0 / float64(uint64(1)<<v)
	}

	zm := 1.0 / sum
	estimate := hll.alpha * float64(hll.m*hll.m) * zm

	// Small range correction
	if estimate <= 2.5*float64(hll.m) {
		v := 0
		for _, r := range hll.registers {
			if r == 0 {
				v++
			}
		}
		if v != 0 {
			return uint64(float64(hll.m) * math.Log(float64(hll.m)/float64(v)))
		}
	}

	return uint64(estimate)
}

// hash64 is a simple FNV-1a hash for HLL.
func hash64(data []byte) uint64 {
	const (
		offset uint64 = 14695981039346656037
		prime  uint64 = 1099511628211
	)
	h := offset
	for _, b := range data {
		h ^= uint64(b)
		h *= prime
	}
	return h
}

// alphaMM computes the alpha constant for HLL.
func alphaMM(m uint32) float64 {
	switch m {
	case 16:
		return 0.673
	case 32:
		return 0.697
	case 64:
		return 0.709
	default:
		return 0.7213 / (1.0 + 1.079/float64(m))
	}
}

// BloomFilter is a simple bloom filter for membership testing.
type BloomFilter struct {
	bits    []uint64
	n       uint32 // Number of bits
	k       uint32 // Number of hash functions
	count   uint64
}

// NewBloomFilter creates a new bloom filter.
func NewBloomFilter(capacity uint32, falsePositiveRate float64) *BloomFilter {
	// n = -capacity * ln(p) / (ln(2)^2)
	n := uint32(-float64(capacity) * math.Log(falsePositiveRate) / (0.4804530139182014))
	n = ((n + 63) / 64) * 64 // Round up to multiple of 64

	// k = n/capacity * ln(2)
	k := uint32(float64(n) / float64(capacity) * 0.6931471805599453)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	return &BloomFilter{
		bits: make([]uint64, n/64),
		n:    n,
		k:    k,
	}
}

// Add adds an element to the filter.
func (bf *BloomFilter) Add(data []byte) {
	h1, h2 := hash128(data)
	for i := uint32(0); i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(bf.n)
		bf.bits[idx/64] |= 1 << (idx % 64)
	}
	bf.count++
}

// Contains checks if an element might be in the filter.
func (bf *BloomFilter) Contains(data []byte) bool {
	h1, h2 := hash128(data)
	for i := uint32(0); i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(bf.n)
		if bf.bits[idx/64]&(1<<(idx%64)) == 0 {
			return false
		}
	}
	return true
}

// hash128 returns two 64-bit hashes from data.
func hash128(data []byte) (uint64, uint64) {
	h1 := hash64(data)
	// FNV-1a with different offset
	h2 := uint64(0xcbf29ce484222325)
	for _, b := range data {
		h2 ^= uint64(b)
		h2 *= 1099511628211
	}
	return h1, h2
}

// EncodeStats serializes index statistics.
func EncodeStats(stats *IndexStats) []byte {
	buf := make([]byte, 8+8+8+8+8)
	binary.BigEndian.PutUint64(buf[0:8], uint64(stats.NumEntries))
	binary.BigEndian.PutUint64(buf[8:16], uint64(stats.NumUnique))
	binary.BigEndian.PutUint64(buf[16:24], uint64(stats.AvgEntrySize))
	binary.BigEndian.PutUint64(buf[24:32], uint64(stats.LastUpdate.Unix()))
	return buf
}

// DecodeStats deserializes index statistics.
func DecodeStats(data []byte) *IndexStats {
	if len(data) < 32 {
		return nil
	}
	return &IndexStats{
		NumEntries:   int64(binary.BigEndian.Uint64(data[0:8])),
		NumUnique:    int64(binary.BigEndian.Uint64(data[8:16])),
		AvgEntrySize: int64(binary.BigEndian.Uint64(data[16:24])),
		LastUpdate:   time.Unix(int64(binary.BigEndian.Uint64(data[24:32])), 0),
	}
}
