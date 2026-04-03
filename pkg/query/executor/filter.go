package executor

import (
	"context"
	"fmt"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// FilterNode applies a predicate to filter documents from its child node.
type FilterNode struct {
	child  PlanNode
	filter MatchFunc

	// State
	open   bool
	stats  NodeStats
}

// NewFilterNode creates a filter node.
func NewFilterNode(child PlanNode, filter *bson.Document) (*FilterNode, error) {
	if filter == nil || filter.Len() == 0 {
		return nil, fmt.Errorf("filter: empty filter predicate")
	}

	matcher := mongo.NewMatcher(filter)
	return &FilterNode{
		child:  child,
		filter: matcher.Match,
	}, nil
}

// Open initializes the filter node.
func (n *FilterNode) Open(ctx context.Context) error {
	if n.open {
		return fmt.Errorf("filter: already open")
	}
	if err := n.child.Open(ctx); err != nil {
		return fmt.Errorf("filter: child open: %w", err)
	}
	n.open = true
	return nil
}

// Next returns the next document that matches the filter.
func (n *FilterNode) Next() (*bson.Document, error) {
	if !n.open {
		return nil, fmt.Errorf("filter: not open")
	}

	for {
		doc, err := n.child.Next()
		if err != nil {
			return nil, fmt.Errorf("filter: child next: %w", err)
		}
		if doc == nil {
			return nil, nil // Exhausted
		}

		n.stats.RowsIn++

		if n.filter(doc) {
			n.stats.RowsOut++
			return doc, nil
		}
		// Filter rejected, continue to next
	}
}

// Close releases resources.
func (n *FilterNode) Close() error {
	if !n.open {
		return nil
	}
	n.open = false
	return n.child.Close()
}

// Explain returns plan description.
func (n *FilterNode) Explain() ExplainNode {
	return ExplainNode{
		NodeType: "FILTER",
		EstCost:  n.child.Explain().EstCost * 1.1, // 10% overhead
		EstRows:  int64(float64(n.child.Explain().EstRows) * 0.3), // Assume 30% selectivity
		Children: []ExplainNode{n.child.Explain()},
		Details: map[string]any{
			"selectivity": 0.3,
		},
	}
}

// Stats returns execution statistics.
func (n *FilterNode) Stats() NodeStats {
	childStats := NodeStats{}
	if s, ok := n.child.(PlanNodeWithStats); ok {
		childStats = s.Stats()
	}
	return NodeStats{
		RowsIn:     n.stats.RowsIn,
		RowsOut:    n.stats.RowsOut,
		ExecTimeMs: n.stats.ExecTimeMs + childStats.ExecTimeMs,
	}
}

// ProjectNode applies field projection (inclusion/exclusion).
type ProjectNode struct {
	child      PlanNode
	projection *bson.Document
	inclusion  bool // true = include only these fields, false = exclude these fields

	open  bool
	stats NodeStats
}

// NewProjectNode creates a projection node.
func NewProjectNode(child PlanNode, projection *bson.Document) (*ProjectNode, error) {
	if projection == nil || projection.Len() == 0 {
		return nil, fmt.Errorf("project: empty projection")
	}

	// Detect inclusion vs exclusion mode
	// MongoDB: _id: 1 is inclusion, _id: 0 is exclusion
	// If any field is 1/exclude: false -> inclusion mode
	// If any field is 0/exclude: true -> exclusion mode
	inclusion := false
	for _, e := range projection.Elements() {
		if e.Key != "_id" {
			if e.Value.Type == bson.TypeInt32 && e.Value.Int32() == 1 {
				inclusion = true
				break
			}
			if e.Value.Type == bson.TypeBoolean && e.Value.Boolean() {
				inclusion = true
				break
			}
		}
	}

	return &ProjectNode{
		child:      child,
		projection: projection,
		inclusion:  inclusion,
	}, nil
}

// Open initializes the projection node.
func (n *ProjectNode) Open(ctx context.Context) error {
	if n.open {
		return fmt.Errorf("project: already open")
	}
	if err := n.child.Open(ctx); err != nil {
		return fmt.Errorf("project: child open: %w", err)
	}
	n.open = true
	return nil
}

// Next returns the next projected document.
func (n *ProjectNode) Next() (*bson.Document, error) {
	if !n.open {
		return nil, fmt.Errorf("project: not open")
	}

	doc, err := n.child.Next()
	if err != nil {
		return nil, fmt.Errorf("project: child next: %w", err)
	}
	if doc == nil {
		return nil, nil
	}

	n.stats.RowsIn++
	n.stats.RowsOut++

	// Apply projection
	projected := mongo.ApplyProjection(doc, n.projection)
	return projected, nil
}

// Close releases resources.
func (n *ProjectNode) Close() error {
	if !n.open {
		return nil
	}
	n.open = false
	return n.child.Close()
}

// Explain returns plan description.
func (n *ProjectNode) Explain() ExplainNode {
	return ExplainNode{
		NodeType: "PROJECT",
		EstCost:  n.child.Explain().EstCost * 1.05, // 5% overhead
		EstRows:  n.child.Explain().EstRows,
		Children: []ExplainNode{n.child.Explain()},
		Details: map[string]any{
			"inclusion": n.inclusion,
		},
	}
}

// Stats returns execution statistics.
func (n *ProjectNode) Stats() NodeStats {
	childStats := NodeStats{}
	if s, ok := n.child.(PlanNodeWithStats); ok {
		childStats = s.Stats()
	}
	return NodeStats{
		RowsIn:     n.stats.RowsIn,
		RowsOut:    n.stats.RowsOut,
		ExecTimeMs: n.stats.ExecTimeMs + childStats.ExecTimeMs,
	}
}
