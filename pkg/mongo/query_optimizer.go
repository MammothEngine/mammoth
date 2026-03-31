package mongo

import (
	"math"
	"sort"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// CostModel defines cost constants for query planning.
type CostModel struct {
	SeqPageCost      float64 // Cost of sequential page read
	RandomPageCost   float64 // Cost of random page read
	CpuTupleCost     float64 // Cost of processing one tuple
	CpuIndexTupleCost float64 // Cost of processing one index tuple
	CpuOperatorCost  float64 // Cost of one operator evaluation
	IndexPageCost    float64 // Cost of reading one index page
}

// DefaultCostModel returns the default cost model.
func DefaultCostModel() *CostModel {
	return &CostModel{
		SeqPageCost:       1.0,
		RandomPageCost:    4.0,
		CpuTupleCost:      0.01,
		CpuIndexTupleCost: 0.005,
		CpuOperatorCost:   0.0025,
		IndexPageCost:     0.5,
	}
}

// CostBasedPlanner implements cost-based query optimization.
type CostBasedPlanner struct {
	indexCat   *IndexCatalog
	statsMgr   *StatsManager
	costModel  *CostModel
}

// NewCostBasedPlanner creates a new cost-based planner.
func NewCostBasedPlanner(indexCat *IndexCatalog, statsMgr *StatsManager) *CostBasedPlanner {
	return &CostBasedPlanner{
		indexCat:  indexCat,
		statsMgr:  statsMgr,
		costModel: DefaultCostModel(),
	}
}

// PlanQuery generates an optimized query plan using cost-based analysis.
func (cbp *CostBasedPlanner) PlanQuery(db, coll string, filter, sort *bson.Document) (*QueryPlan, error) {
	// Get available indexes
	indexes, err := cbp.indexCat.ListIndexes(db, coll)
	if err != nil {
		return nil, err
	}

	// Generate candidate plans
	candidates := cbp.generateCandidates(db, coll, filter, sort, indexes)

	// Cost each candidate
	var bestPlan *QueryPlan
	bestCost := math.MaxFloat64

	for _, candidate := range candidates {
		cost := cbp.estimateCost(db, coll, candidate, filter)
		candidate.EstimatedCost = cost

		if cost < bestCost {
			bestCost = cost
			bestPlan = candidate
		}
	}

	return bestPlan, nil
}

// generateCandidates generates candidate plans for a query.
func (cbp *CostBasedPlanner) generateCandidates(db, coll string, filter, sort *bson.Document, indexes []IndexSpec) []*QueryPlan {
	var candidates []*QueryPlan

	// Always consider collection scan
	candidates = append(candidates, &QueryPlan{
		PlanType:    PlanCollScan,
		CreatedAt:   time.Now(),
	})

	// Generate single index plans
	for _, idx := range indexes {
		if spec, prefixKey, ok := cbp.matchesIndex(db, coll, filter, &idx); ok {
			plan := &QueryPlan{
				PlanType:   PlanIndexScan,
				IndexName:  idx.Name,
				IndexSpec:  spec,
				ScanPrefix: prefixKey,
				CreatedAt:  time.Now(),
			}

			// Check if sort is covered
			if sort != nil && sort.Len() > 0 {
				plan.SortCovered = isSortCoveredByIndex(spec, sort, filter)
			}

			candidates = append(candidates, plan)
		}
	}

	// Generate index intersection plans for complex filters
	if len(indexes) >= 2 && filter != nil && filter.Len() >= 2 {
		intersectionPlans := cbp.generateIndexIntersectionPlans(db, coll, filter, indexes)
		candidates = append(candidates, intersectionPlans...)
	}

	return candidates
}

// generateIndexIntersectionPlans generates plans using multiple indexes.
func (cbp *CostBasedPlanner) generateIndexIntersectionPlans(db, coll string, filter *bson.Document, indexes []IndexSpec) []*QueryPlan {
	var plans []*QueryPlan

	// Find indexes that cover different filter fields
	fieldIndexes := make(map[string][]IndexSpec)

	for _, idx := range indexes {
		if len(idx.Key) > 0 {
			field := idx.Key[0].Field
			if _, ok := fieldIndexes[field]; !ok {
				fieldIndexes[field] = []IndexSpec{}
			}
			fieldIndexes[field] = append(fieldIndexes[field], idx)
		}
	}

	// For queries with multiple equality conditions on different fields
	filterFields := make([]string, 0, filter.Len())
	for _, e := range filter.Elements() {
		if isEqualityCondition(e.Value) {
			filterFields = append(filterFields, e.Key)
		}
	}

	// Generate intersection plans for pairs of fields
	if len(filterFields) >= 2 {
		for i := 0; i < len(filterFields) && i < 3; i++ {
			for j := i + 1; j < len(filterFields) && j < 3; j++ {
				field1 := filterFields[i]
				field2 := filterFields[j]

				if idxes1, ok1 := fieldIndexes[field1]; ok1 {
					if idxes2, ok2 := fieldIndexes[field2]; ok2 {
						if len(idxes1) > 0 && len(idxes2) > 0 {
							plan := &QueryPlan{
								PlanType:    PlanMultiIndex,
								IndexName:   idxes1[0].Name + "/" + idxes2[0].Name,
								IndexSpec:   &idxes1[0],
								CreatedAt:   time.Now(),
							}
							plans = append(plans, plan)
						}
					}
				}
			}
		}
	}

	return plans
}

// isEqualityCondition checks if a value is an equality condition.
func isEqualityCondition(v bson.Value) bool {
	if v.Type != bson.TypeDocument {
		return true // Implicit $eq
	}

	opDoc := v.DocumentValue()
	// Check if it's only $eq operator
	for _, e := range opDoc.Elements() {
		if e.Key != "$eq" {
			return false
		}
	}
	return true
}

// matchesIndex checks if an index can be used for the filter.
func (cbp *CostBasedPlanner) matchesIndex(db, coll string, filter *bson.Document, spec *IndexSpec) (*IndexSpec, []byte, bool) {
	if filter == nil || filter.Len() == 0 {
		return nil, nil, false
	}

	// Build the prefix key for index scanning
	prefixKey := buildIndexScanKey(db, coll, spec, filter)
	if prefixKey == nil {
		return nil, nil, false
	}

	return spec, prefixKey, true
}

// estimateCost estimates the cost of executing a query plan.
func (cbp *CostBasedPlanner) estimateCost(db, coll string, plan *QueryPlan, filter *bson.Document) float64 {
	switch plan.PlanType {
	case PlanCollScan:
		return cbp.estimateCollScanCost(db, coll, filter)
	case PlanIndexScan:
		return cbp.estimateIndexScanCost(db, coll, plan, filter)
	case PlanMultiIndex:
		return cbp.estimateIndexIntersectionCost(db, coll, plan, filter)
	default:
		return math.MaxFloat64
	}
}

// estimateCollScanCost estimates cost of collection scan.
func (cbp *CostBasedPlanner) estimateCollScanCost(db, coll string, filter *bson.Document) float64 {
	// Get collection stats
	stats := cbp.statsMgr.GetStats(db, coll, "_collection_")
	if stats == nil {
		// Conservative estimate
		return 1000000.0
	}

	numPages := float64(stats.NumEntries) / 100.0 // Assume 100 entries per page
	seqIOCost := numPages * cbp.costModel.SeqPageCost
	cpuCost := float64(stats.NumEntries) * cbp.costModel.CpuTupleCost

	// Add filter evaluation cost
	if filter != nil {
		cpuCost += float64(filter.Len()) * float64(stats.NumEntries) * cbp.costModel.CpuOperatorCost
	}

	return seqIOCost + cpuCost
}

// estimateIndexScanCost estimates cost of index scan.
func (cbp *CostBasedPlanner) estimateIndexScanCost(db, coll string, plan *QueryPlan, filter *bson.Document) float64 {
	if plan.IndexSpec == nil {
		return math.MaxFloat64
	}

	// Get index selectivity
	selectivity := cbp.statsMgr.Selectivity(db, coll, plan.IndexSpec.Name)
	if selectivity == 0 {
		selectivity = 0.1 // Default 10% selectivity
	}

	// Index stats
	stats := cbp.statsMgr.GetStats(db, coll, plan.IndexSpec.Name)
	if stats == nil {
		return math.MaxFloat64
	}

	numIndexTuples := float64(stats.NumEntries)
	numTableTuples := numIndexTuples * selectivity

	// Index scan cost
	indexHeight := 3.0 // Assume B-tree height of 3
	indexScanCost := indexHeight * cbp.costModel.IndexPageCost
	indexScanCost += numIndexTuples * cbp.costModel.CpuIndexTupleCost

	// Table access cost (random I/O for each matching tuple)
	tableAccessCost := numTableTuples * cbp.costModel.RandomPageCost
	tableAccessCost += numTableTuples * cbp.costModel.CpuTupleCost

	// Filter evaluation cost for remaining predicates
	filterCost := float64(filter.Len()) * numTableTuples * cbp.costModel.CpuOperatorCost

	return indexScanCost + tableAccessCost + filterCost
}

// estimateIndexIntersectionCost estimates cost of index intersection.
func (cbp *CostBasedPlanner) estimateIndexIntersectionCost(db, coll string, plan *QueryPlan, filter *bson.Document) float64 {
	// Index intersection is more expensive due to:
	// 1. Multiple index scans
	// 2. Set intersection operation
	// 3. But can be cheaper if each index is very selective

	singleCost := cbp.estimateIndexScanCost(db, coll, plan, filter)

	// Add overhead for intersection (assume 2 indexes)
	intersectionOverhead := singleCost * 0.3

	// Benefit from higher selectivity
	selectivityBonus := singleCost * 0.4 // Assume 40% benefit

	return singleCost + intersectionOverhead - selectivityBonus
}

// JoinOptimizer optimizes join operations.
type JoinOptimizer struct {
	costModel *CostModel
}

// NewJoinOptimizer creates a new join optimizer.
func NewJoinOptimizer() *JoinOptimizer {
	return &JoinOptimizer{
		costModel: DefaultCostModel(),
	}
}

// JoinType represents the type of join.
type JoinType int

const (
	JoinNestedLoop JoinType = iota
	JoinHash
	JoinMerge
	JoinIndexNestedLoop
)

// JoinPlan represents a join execution plan.
type JoinPlan struct {
	JoinType      JoinType
	LeftTable     string
	RightTable    string
	JoinCondition *bson.Document
	EstimatedCost float64
}

// OptimizeJoin selects the best join strategy.
func (jo *JoinOptimizer) OptimizeJoin(leftTable, rightTable string, joinCondition *bson.Document, leftSize, rightSize int64) *JoinPlan {
	candidates := []*JoinPlan{
		jo.planNestedLoop(leftTable, rightTable, joinCondition, leftSize, rightSize),
		jo.planHashJoin(leftTable, rightTable, joinCondition, leftSize, rightSize),
		jo.planMergeJoin(leftTable, rightTable, joinCondition, leftSize, rightSize),
	}

	// Select cheapest plan
	var best *JoinPlan
	bestCost := math.MaxFloat64

	for _, candidate := range candidates {
		if candidate.EstimatedCost < bestCost {
			bestCost = candidate.EstimatedCost
			best = candidate
		}
	}

	return best
}

// planNestedLoop plans a nested loop join.
func (jo *JoinOptimizer) planNestedLoop(leftTable, rightTable string, joinCondition *bson.Document, leftSize, rightSize int64) *JoinPlan {
	// Cost: O(M * N) where M and N are table sizes
	cost := float64(leftSize) * float64(rightSize) * jo.costModel.CpuTupleCost

	return &JoinPlan{
		JoinType:      JoinNestedLoop,
		LeftTable:     leftTable,
		RightTable:    rightTable,
		JoinCondition: joinCondition,
		EstimatedCost: cost,
	}
}

// planHashJoin plans a hash join.
func (jo *JoinOptimizer) planHashJoin(leftTable, rightTable string, joinCondition *bson.Document, leftSize, rightSize int64) *JoinPlan {
	// Cost: O(M + N) for building and probing hash table
	buildCost := float64(leftSize) * (jo.costModel.CpuTupleCost + jo.costModel.CpuOperatorCost)
	probeCost := float64(rightSize) * jo.costModel.CpuTupleCost

	return &JoinPlan{
		JoinType:      JoinHash,
		LeftTable:     leftTable,
		RightTable:    rightTable,
		JoinCondition: joinCondition,
		EstimatedCost: buildCost + probeCost,
	}
}

// planMergeJoin plans a merge join.
func (jo *JoinOptimizer) planMergeJoin(leftTable, rightTable string, joinCondition *bson.Document, leftSize, rightSize int64) *JoinPlan {
	// Cost: O(M + N) if sorted, O(M log M + N log N) if not
	// Assume inputs are sorted (cost already paid in index scan)
	cost := float64(leftSize+rightSize) * jo.costModel.CpuTupleCost

	return &JoinPlan{
		JoinType:      JoinMerge,
		LeftTable:     leftTable,
		RightTable:    rightTable,
		JoinCondition: joinCondition,
		EstimatedCost: cost,
	}
}

// QueryPlanner is the main query planning interface.
type QueryPlanner struct {
	costPlanner *CostBasedPlanner
	joinPlanner *JoinOptimizer
}

// NewQueryPlanner creates a new query planner.
func NewQueryPlanner(indexCat *IndexCatalog, statsMgr *StatsManager) *QueryPlanner {
	return &QueryPlanner{
		costPlanner: NewCostBasedPlanner(indexCat, statsMgr),
		joinPlanner: NewJoinOptimizer(),
	}
}

// Plan generates an execution plan for a query.
func (qp *QueryPlanner) Plan(db, coll string, filter, sort *bson.Document) (*QueryPlan, error) {
	return qp.costPlanner.PlanQuery(db, coll, filter, sort)
}

// PlanJoin generates an execution plan for a join.
func (qp *QueryPlanner) PlanJoin(leftTable, rightTable string, joinCondition *bson.Document, leftSize, rightSize int64) *JoinPlan {
	return qp.joinPlanner.OptimizeJoin(leftTable, rightTable, joinCondition, leftSize, rightSize)
}

// IndexAdvisor provides index recommendations.
type IndexAdvisor struct {
	indexCat *IndexCatalog
	statsMgr *StatsManager
}

// NewIndexAdvisor creates a new index advisor.
func NewIndexAdvisor(indexCat *IndexCatalog, statsMgr *StatsManager) *IndexAdvisor {
	return &IndexAdvisor{
		indexCat: indexCat,
		statsMgr: statsMgr,
	}
}

// IndexRecommendation represents an index recommendation.
type IndexRecommendation struct {
	Collection    string
	Fields        []string
	Impact        float64 // Estimated query speedup
	CurrentCost   float64
	OptimizedCost float64
}

// RecommendIndexes analyzes queries and recommends new indexes.
func (ia *IndexAdvisor) RecommendIndexes(db, coll string, recentQueries []*bson.Document) []*IndexRecommendation {
	var recommendations []*IndexRecommendation

	// Analyze query patterns
	fieldFrequency := make(map[string]int)
	fieldCombinations := make(map[string]int)

	for _, query := range recentQueries {
		if query == nil {
			continue
		}

		fields := make([]string, 0, query.Len())
		for _, e := range query.Elements() {
			fields = append(fields, e.Key)
			fieldFrequency[e.Key]++
		}

		// Track field combinations
		if len(fields) >= 2 {
			combo := fields[0] + "," + fields[1]
			fieldCombinations[combo]++
		}
	}

	// Check for missing indexes on frequently queried fields
	for field, freq := range fieldFrequency {
		if freq < 5 {
			continue // Not frequent enough
		}

		// Check if index exists
		_, err := ia.indexCat.GetIndex(db, coll, "idx_"+field)
		if err == nil {
			continue // Index already exists
		}

		recommendation := &IndexRecommendation{
			Collection: coll,
			Fields:     []string{field},
			Impact:     float64(freq) * 0.1, // Simple impact estimation
		}
		recommendations = append(recommendations, recommendation)
	}

	// Sort by impact (descending)
	sort.Slice(recommendations, func(i, j int) bool {
		return recommendations[i].Impact > recommendations[j].Impact
	})

	return recommendations
}
