package mongo

import (
	"math"
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

