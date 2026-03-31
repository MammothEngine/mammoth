package mongo

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// PlanType represents the type of query plan.
type PlanType int

const (
	PlanCollScan PlanType = iota // Collection scan
	PlanIndexScan                // Index scan
	PlanCountScan                // Count optimization
	PlanMultiIndex               // Index intersection
)

// QueryPlan represents an execution plan for a query.
type QueryPlan struct {
	PlanType    PlanType
	IndexName   string
	IndexSpec   *IndexSpec
	ScanPrefix  []byte
	SortCovered bool
	EstimatedCost float64
	CreatedAt   time.Time
	UseCount    int64
	AvgExecTime time.Duration
}

// PlanCacheEntry represents a cached query plan.
type PlanCacheEntry struct {
	QueryHash   string
	Plan        *QueryPlan
	CreatedAt   time.Time
	LastUsed    time.Time
	UseCount    int64
	TotalExecTime time.Duration
}

// PlanCache caches compiled query plans.
type PlanCache struct {
	mu          sync.RWMutex
	entries     map[string]*PlanCacheEntry // key: query hash
	maxEntries  int
	defaultTTL  time.Duration
	hits        int64
	misses      int64
}

// NewPlanCache creates a new plan cache.
func NewPlanCache(maxEntries int) *PlanCache {
	return &PlanCache{
		entries:    make(map[string]*PlanCacheEntry),
		maxEntries: maxEntries,
		defaultTTL: 10 * time.Minute,
	}
}

// Get retrieves a cached plan for a query.
func (pc *PlanCache) Get(db, coll string, filter, sort, projection *bson.Document) *QueryPlan {
	hash := pc.hashQuery(db, coll, filter, sort, projection)

	pc.mu.RLock()
	entry, ok := pc.entries[hash]
	pc.mu.RUnlock()

	if !ok {
		pc.recordMiss()
		return nil
	}

	// Check TTL
	if time.Since(entry.CreatedAt) > pc.defaultTTL {
		pc.mu.Lock()
		delete(pc.entries, hash)
		pc.mu.Unlock()
		pc.recordMiss()
		return nil
	}

	// Update usage stats
	pc.mu.Lock()
	entry.LastUsed = time.Now()
	entry.UseCount++
	pc.mu.Unlock()

	pc.recordHit()
	return entry.Plan
}

// Put stores a plan in the cache.
func (pc *PlanCache) Put(db, coll string, filter, sort, projection *bson.Document, plan *QueryPlan) {
	hash := pc.hashQuery(db, coll, filter, sort, projection)

	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Evict oldest if at capacity
	if len(pc.entries) >= pc.maxEntries {
		pc.evictOldest()
	}

	pc.entries[hash] = &PlanCacheEntry{
		QueryHash:   hash,
		Plan:        plan,
		CreatedAt:   time.Now(),
		LastUsed:    time.Now(),
		UseCount:    0,
	}
}

// Invalidate removes cached plans for a collection.
func (pc *PlanCache) Invalidate(db, coll string) {
	prefix := db + "." + coll + "."

	pc.mu.Lock()
	defer pc.mu.Unlock()

	for hash := range pc.entries {
		if len(hash) >= len(prefix) && hash[:len(prefix)] == prefix {
			delete(pc.entries, hash)
		}
	}
}

// InvalidateAll clears the entire cache.
func (pc *PlanCache) InvalidateAll() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.entries = make(map[string]*PlanCacheEntry)
}

// RecordExecution records execution statistics for a plan.
func (pc *PlanCache) RecordExecution(db, coll string, filter, sort, projection *bson.Document, execTime time.Duration) {
	hash := pc.hashQuery(db, coll, filter, sort, projection)

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if entry, ok := pc.entries[hash]; ok {
		entry.TotalExecTime += execTime
		if entry.UseCount > 0 {
			entry.Plan.AvgExecTime = entry.TotalExecTime / time.Duration(entry.UseCount)
		}
	}
}

// hashQuery creates a hash of the query for cache lookup.
func (pc *PlanCache) hashQuery(db, coll string, filter, sort, projection *bson.Document) string {
	h := sha256.New()

	h.Write([]byte(db))
	h.Write([]byte{0})
	h.Write([]byte(coll))
	h.Write([]byte{0})

	if filter != nil {
		h.Write(bson.Encode(filter))
	}
	h.Write([]byte{0})

	if sort != nil {
		h.Write(bson.Encode(sort))
	}
	h.Write([]byte{0})

	if projection != nil {
		h.Write(bson.Encode(projection))
	}

	return db + "." + coll + "." + hex.EncodeToString(h.Sum(nil)[:16])
}

// evictOldest removes the least recently used entry.
func (pc *PlanCache) evictOldest() {
	var oldest *PlanCacheEntry
	var oldestKey string

	for k, e := range pc.entries {
		if oldest == nil || e.LastUsed.Before(oldest.LastUsed) {
			oldest = e
			oldestKey = k
		}
	}

	if oldestKey != "" {
		delete(pc.entries, oldestKey)
	}
}

// recordHit records a cache hit.
func (pc *PlanCache) recordHit() {
	pc.mu.Lock()
	pc.hits++
	pc.mu.Unlock()
}

// recordMiss records a cache miss.
func (pc *PlanCache) recordMiss() {
	pc.mu.Lock()
	pc.misses++
	pc.mu.Unlock()
}

// Stats returns cache statistics.
func (pc *PlanCache) Stats() (hits, misses int64, size int) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.hits, pc.misses, len(pc.entries)
}

// PreparedStatement represents a prepared query with bound parameters.
type PreparedStatement struct {
	ID          string
	DB          string
	Collection  string
	Filter      *bson.Document
	Sort        *bson.Document
	Projection  *bson.Document
	Plan        *QueryPlan
	CreatedAt   time.Time
	UseCount    int64
}

// PreparedStmtCache caches prepared statements.
type PreparedStmtCache struct {
	mu       sync.RWMutex
	stmts    map[string]*PreparedStatement
	maxStmts int
}

// NewPreparedStmtCache creates a new prepared statement cache.
func NewPreparedStmtCache(maxStmts int) *PreparedStmtCache {
	return &PreparedStmtCache{
		stmts:    make(map[string]*PreparedStatement),
		maxStmts: maxStmts,
	}
}

// Prepare creates a prepared statement for a query.
func (psc *PreparedStmtCache) Prepare(id, db, coll string, filter, sort, projection *bson.Document, plan *QueryPlan) *PreparedStatement {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	// Evict oldest if at capacity
	if len(psc.stmts) >= psc.maxStmts {
		psc.evictOldest()
	}

	stmt := &PreparedStatement{
		ID:         id,
		DB:         db,
		Collection: coll,
		Filter:     filter,
		Sort:       sort,
		Projection: projection,
		Plan:       plan,
		CreatedAt:  time.Now(),
	}

	psc.stmts[id] = stmt
	return stmt
}

// Get retrieves a prepared statement by ID.
func (psc *PreparedStmtCache) Get(id string) *PreparedStatement {
	psc.mu.RLock()
	defer psc.mu.RUnlock()

	stmt := psc.stmts[id]
	if stmt != nil {
		stmt.UseCount++
	}
	return stmt
}

// Remove removes a prepared statement.
func (psc *PreparedStmtCache) Remove(id string) {
	psc.mu.Lock()
	defer psc.mu.Unlock()
	delete(psc.stmts, id)
}

// evictOldest removes the least used prepared statement.
func (psc *PreparedStmtCache) evictOldest() {
	var oldest *PreparedStatement
	var oldestKey string

	for k, s := range psc.stmts {
		if oldest == nil || s.UseCount < oldest.UseCount {
			oldest = s
			oldestKey = k
		}
	}

	if oldestKey != "" {
		delete(psc.stmts, oldestKey)
	}
}

// List returns all prepared statement IDs.
func (psc *PreparedStmtCache) List() []string {
	psc.mu.RLock()
	defer psc.mu.RUnlock()

	ids := make([]string, 0, len(psc.stmts))
	for id := range psc.stmts {
		ids = append(ids, id)
	}
	return ids
}

// QueryOptimizer is the main query optimization interface.
type QueryOptimizer struct {
	eng         *engine.Engine
	indexCat    *IndexCatalog
	planCache   *PlanCache
	statsMgr    *StatsManager
	prepared    *PreparedStmtCache
}

// NewQueryOptimizer creates a new query optimizer.
func NewQueryOptimizer(eng *engine.Engine, indexCat *IndexCatalog) *QueryOptimizer {
	return &QueryOptimizer{
		eng:       eng,
		indexCat:  indexCat,
		planCache: NewPlanCache(1000),
		statsMgr:  NewStatsManager(eng),
		prepared:  NewPreparedStmtCache(100),
	}
}

// OptimizeQuery generates or retrieves an optimized query plan.
func (qo *QueryOptimizer) OptimizeQuery(db, coll string, filter, sort, projection *bson.Document) *QueryPlan {
	// Check plan cache first
	if plan := qo.planCache.Get(db, coll, filter, sort, projection); plan != nil {
		return plan
	}

	// Generate new plan
	plan := qo.generatePlan(db, coll, filter, sort)

	// Cache the plan
	qo.planCache.Put(db, coll, filter, sort, projection, plan)

	return plan
}

// generatePlan creates a new query plan.
func (qo *QueryOptimizer) generatePlan(db, coll string, filter, sort *bson.Document) *QueryPlan {
	// Try to find best index
	if spec, prefixKey, ok := qo.indexCat.FindBestIndex(db, coll, filter); ok && spec != nil {
		plan := &QueryPlan{
			PlanType:    PlanIndexScan,
			IndexName:   spec.Name,
			IndexSpec:   spec,
			ScanPrefix:  prefixKey,
			CreatedAt:   time.Now(),
		}

		// Check if sort is covered by index
		if sort != nil && sort.Len() > 0 {
			plan.SortCovered = isSortCoveredByIndex(spec, sort, filter)
		}

		// Estimate cost
		plan.EstimatedCost = qo.estimateCost(db, coll, spec, filter)

		return plan
	}

	// Fall back to collection scan
	return &QueryPlan{
		PlanType:      PlanCollScan,
		EstimatedCost: 1000000, // High cost for collection scan
		CreatedAt:     time.Now(),
	}
}

// estimateCost estimates the cost of using an index.
func (qo *QueryOptimizer) estimateCost(db, coll string, spec *IndexSpec, filter *bson.Document) float64 {
	selectivity := qo.statsMgr.Selectivity(db, coll, spec.Name)

	// Base cost: sequential index scan
	cost := 100.0

	// Adjust for selectivity
	cost *= float64(selectivity)

	// Prefer indexes that match more filter fields
	filterFields := analyzeFilterFields(filter)
	matchingFields := 0
	for _, k := range spec.Key {
		if _, ok := filterFields[k.Field]; ok {
			matchingFields++
		}
	}
	cost /= float64(matchingFields + 1)

	return cost
}

// Prepare creates a prepared statement.
func (qo *QueryOptimizer) Prepare(id, db, coll string, filter, sort, projection *bson.Document) *PreparedStatement {
	plan := qo.OptimizeQuery(db, coll, filter, sort, projection)
	return qo.prepared.Prepare(id, db, coll, filter, sort, projection, plan)
}

// ExecutePrepared executes a prepared statement.
func (qo *QueryOptimizer) ExecutePrepared(id string) *PreparedStatement {
	return qo.prepared.Get(id)
}

// Invalidate invalidates cached plans for a collection.
func (qo *QueryOptimizer) Invalidate(db, coll string) {
	qo.planCache.Invalidate(db, coll)
}

// Stats returns optimizer statistics.
func (qo *QueryOptimizer) Stats() (hits, misses, size int) {
	h, m, s := qo.planCache.Stats()
	return int(h), int(m), s
}

// isSortCoveredByIndex checks if the sort can be served by the index.
// Accounts for filter fields that are covered by the index prefix.
func isSortCoveredByIndex(spec *IndexSpec, sortDoc *bson.Document, filter *bson.Document) bool {
	if len(spec.Key) < sortDoc.Len() {
		return false
	}

	// Determine how many index keys are used for equality filtering
	filterFields := make(map[string]bool)
	if filter != nil {
		for _, e := range filter.Elements() {
			// Check if it's an equality filter (implicit $eq or explicit $eq)
			isEquality := true
			if e.Value.Type == bson.TypeDocument {
				opDoc := e.Value.DocumentValue()
				// Check if it has only $eq or no operators
				for _, op := range opDoc.Elements() {
					if op.Key != "$eq" {
						isEquality = false
						break
					}
				}
			}
			if isEquality {
				filterFields[e.Key] = true
			}
		}
	}

	// Skip index keys that are equality-filtered
	idxPos := 0
	for idxPos < len(spec.Key) && filterFields[spec.Key[idxPos].Field] {
		idxPos++
	}

	// Now check if remaining sort keys match remaining index keys
	sortKeys := sortDoc.Keys()
	for _, sk := range sortKeys {
		if idxPos >= len(spec.Key) {
			return false
		}
		sortVal, _ := sortDoc.Get(sk)
		descending := sortVal.Type == bson.TypeInt32 && sortVal.Int32() == -1
		if spec.Key[idxPos].Field != sk || spec.Key[idxPos].Descending != descending {
			return false
		}
		idxPos++
	}

	return true
}
