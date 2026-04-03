// Package planner converts AST query nodes to executable PlanNodes.
// It bridges the query parser and executor, handling index selection.
package planner

import (
	"context"
	"fmt"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
	"github.com/mammothengine/mammoth/pkg/query/executor"
	"github.com/mammothengine/mammoth/pkg/query/parser"
)

// Planner converts parsed queries to executable plans.
type Planner struct {
	eng       *engine.Engine
	indexCat  IndexCatalog
	statsMgr  StatsManager
}

// IndexCatalog provides index metadata access.
type IndexCatalog interface {
	ListIndexes(db, coll string) ([]mongo.IndexSpec, error)
	GetIndex(db, coll, name string) (*mongo.IndexSpec, error)
}

// StatsManager provides collection statistics.
type StatsManager interface {
	GetStats(db, coll, component string) *CollectionStats
	Selectivity(db, coll, indexName string) float64
}

// CollectionStats contains statistics for cost estimation.
type CollectionStats struct {
	NumEntries int64
	SizeBytes  int64
}

// PlanOptions contains query planning options.
type PlanOptions struct {
	Filter     *bson.Document
	Projection *bson.Document
	Sort       *bson.Document
	Skip       int64
	Limit      int64
}

// NewPlanner creates a new query planner.
func NewPlanner(eng *engine.Engine, indexCat IndexCatalog, statsMgr StatsManager) *Planner {
	return &Planner{
		eng:      eng,
		indexCat: indexCat,
		statsMgr: statsMgr,
	}
}

// Plan creates an execution plan for a find query.
func (p *Planner) Plan(ctx context.Context, db, coll string, opts PlanOptions) (executor.PlanNode, error) {
	// Parse filter to AST for evaluation
	var matchFunc executor.MatchFunc
	if opts.Filter != nil && opts.Filter.Len() > 0 {
		ast, err := parser.Parse(opts.Filter)
		if err != nil {
			return nil, fmt.Errorf("parse filter: %w", err)
		}
		matchFunc = func(doc *bson.Document) bool {
			return ast.Evaluate(doc)
		}
	}

	// Choose between index scan and collection scan
	scanNode := p.createScanNode(db, coll, opts.Filter, matchFunc)

	// Apply projection if specified
	var node executor.PlanNode = scanNode
	if opts.Projection != nil && opts.Projection.Len() > 0 {
		projNode, err := executor.NewProjectNode(node, opts.Projection)
		if err != nil {
			return nil, fmt.Errorf("create projection: %w", err)
		}
		node = projNode
	}

	// Apply filter if no index was used for it
	if matchFunc != nil {
		// Check if we can use the index for filtering
		if scanNode.Filter() == nil {
			// No filter in scan, apply filter node
			filterNode, err := executor.NewFilterNode(node, opts.Filter)
			if err != nil {
				return nil, err
			}
			node = filterNode
		}
	}

	// Apply sort
	if opts.Sort != nil && opts.Sort.Len() > 0 {
		sortNode, err := executor.NewSortNode(node, opts.Sort)
		if err != nil {
			return nil, err
		}
		node = sortNode
	}

	// Apply skip + limit (order matters: skip then limit)
	if opts.Skip > 0 || opts.Limit > 0 {
		limitSkipNode, err := executor.NewLimitSkipNode(node, opts.Limit, opts.Skip)
		if err != nil {
			return nil, err
		}
		node = limitSkipNode
	}

	return node, nil
}

// scannableNode wraps a scan node to provide filter access.
type scannableNode struct {
	executor.PlanNode
	filter executor.MatchFunc
}

func (s *scannableNode) Filter() executor.MatchFunc {
	return s.filter
}

// createScanNode creates the appropriate scan node (index or collection).
func (p *Planner) createScanNode(db, coll string, filter *bson.Document, matchFunc executor.MatchFunc) *scannableNode {
	// Try to find a usable index
	indexSpec, prefixKey := p.selectIndex(db, coll, filter)
	if indexSpec != nil {
		// Use index scan with proper bounds
		bounds := executor.IndexBounds{
			LowerBound: prefixKey,
			UpperBound: prefixKey,
			LowerInc:   true,
			UpperInc:   true,
			Equality:   true,
		}
		scan := executor.NewIndexScanNode(p.eng, db, coll, indexSpec, bounds)
		scan.SetFilter(matchFunc)
		return &scannableNode{PlanNode: scan, filter: matchFunc}
	}

	// Fall back to collection scan
	scan := executor.NewCollScanNode(p.eng, db, coll)
	scan.SetFilter(matchFunc)
	return &scannableNode{PlanNode: scan, filter: matchFunc}
}

// selectIndex chooses the best index for the filter.
func (p *Planner) selectIndex(db, coll string, filter *bson.Document) (*mongo.IndexSpec, []byte) {
	if filter == nil || filter.Len() == 0 {
		return nil, nil
	}

	indexes, err := p.indexCat.ListIndexes(db, coll)
	if err != nil || len(indexes) == 0 {
		return nil, nil
	}

	// Find the best matching index
	var bestIndex *mongo.IndexSpec
	var bestPrefix []byte
	bestScore := 0

	for i := range indexes {
		idx := &indexes[i]
		if prefix, score := p.matchIndex(idx, filter); score > bestScore {
			bestIndex = idx
			bestPrefix = prefix
			bestScore = score
		}
	}

	return bestIndex, bestPrefix
}

// matchIndex scores how well an index matches the filter.
// Returns the prefix key for scanning and a match score.
func (p *Planner) matchIndex(idx *mongo.IndexSpec, filter *bson.Document) ([]byte, int) {
	if len(idx.Key) == 0 {
		return nil, 0
	}

	// Count how many leading index fields are in the filter
	matchedFields := 0
	for _, key := range idx.Key {
		if _, ok := filter.Get(key.Field); ok {
			matchedFields++
		} else {
			break // Must be prefix match
		}
	}

	if matchedFields == 0 {
		return nil, 0
	}

	// Build prefix key from matched fields
	// This is a simplified version - full implementation would
	// encode values properly for the index scan
	prefix := make([]byte, 0)
	for i := 0; i < matchedFields && i < len(idx.Key); i++ {
		key := idx.Key[i]
		if val, ok := filter.Get(key.Field); ok {
			// Encode the value for index lookup
			prefix = append(prefix, encodeIndexPrefix(val, key.Descending)...)
		}
	}

	return prefix, matchedFields
}

// encodeIndexPrefix creates an index key prefix for scanning.
func encodeIndexPrefix(val bson.Value, descending bool) []byte {
	// This is a placeholder - full implementation would use
	// the proper index encoding from the mongo package
	return nil
}
