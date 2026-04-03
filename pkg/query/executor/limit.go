package executor

import (
	"context"
	"fmt"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// LimitNode limits the number of output rows.
type LimitNode struct {
	child  PlanNode
	limit  int64
	offset int64

	// State
	open   bool
	seen   int64
	stats  NodeStats
}

// NewLimitNode creates a limit node.
func NewLimitNode(child PlanNode, limit int64) (*LimitNode, error) {
	if limit < 0 {
		return nil, fmt.Errorf("limit: negative limit")
	}
	return &LimitNode{
		child: child,
		limit: limit,
	}, nil
}

// WithOffset adds an offset (skip first N rows).
func (n *LimitNode) WithOffset(offset int64) *LimitNode {
	n.offset = offset
	return n
}

// Open initializes the limit node.
func (n *LimitNode) Open(ctx context.Context) error {
	if n.open {
		return fmt.Errorf("limit: already open")
	}
	if err := n.child.Open(ctx); err != nil {
		return fmt.Errorf("limit: child open: %w", err)
	}

	// Skip offset rows
	for n.offset > 0 {
		doc, err := n.child.Next()
		if err != nil {
			n.child.Close()
			return fmt.Errorf("limit: skip: %w", err)
		}
		if doc == nil {
			break // Child exhausted
		}
		n.offset--
		n.stats.RowsIn++
	}

	n.open = true
	n.seen = 0
	return nil
}

// Next returns the next document up to the limit.
func (n *LimitNode) Next() (*bson.Document, error) {
	if !n.open {
		return nil, fmt.Errorf("limit: not open")
	}
	if n.seen >= n.limit {
		return nil, nil // Limit reached
	}

	doc, err := n.child.Next()
	if err != nil {
		return nil, fmt.Errorf("limit: child next: %w", err)
	}
	if doc == nil {
		return nil, nil // Child exhausted
	}

	n.seen++
	n.stats.RowsIn++
	n.stats.RowsOut++
	return doc, nil
}

// Close releases resources.
func (n *LimitNode) Close() error {
	if !n.open {
		return nil
	}
	n.open = false
	return n.child.Close()
}

// Explain returns plan description.
func (n *LimitNode) Explain() ExplainNode {
	childExplain := n.child.Explain()
	return ExplainNode{
		NodeType: "LIMIT",
		EstCost:  childExplain.EstCost,
		EstRows:  minInt64(childExplain.EstRows, n.limit),
		Children: []ExplainNode{childExplain},
		Details: map[string]any{
			"limit":  n.limit,
			"offset": n.offset,
		},
	}
}

// Stats returns execution statistics.
func (n *LimitNode) Stats() NodeStats {
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

// SkipNode skips the first N rows (equivalent to MongoDB's skip).
type SkipNode struct {
	child PlanNode
	skip  int64

	// State
	open  bool
	seen  int64
	stats NodeStats
}

// NewSkipNode creates a skip node.
func NewSkipNode(child PlanNode, skip int64) (*SkipNode, error) {
	if skip < 0 {
		return nil, fmt.Errorf("skip: negative skip")
	}
	return &SkipNode{
		child: child,
		skip:  skip,
	}, nil
}

// Open initializes the skip node.
func (n *SkipNode) Open(ctx context.Context) error {
	if n.open {
		return fmt.Errorf("skip: already open")
	}
	if err := n.child.Open(ctx); err != nil {
		return fmt.Errorf("skip: child open: %w", err)
	}

	// Skip first N rows
	for n.seen < n.skip {
		doc, err := n.child.Next()
		if err != nil {
			n.child.Close()
			return fmt.Errorf("skip: skip: %w", err)
		}
		if doc == nil {
			break // Child exhausted before skip completed
		}
		n.seen++
		n.stats.RowsIn++
	}

	n.open = true
	return nil
}

// Next returns the next document after skip.
func (n *SkipNode) Next() (*bson.Document, error) {
	if !n.open {
		return nil, fmt.Errorf("skip: not open")
	}

	doc, err := n.child.Next()
	if err != nil {
		return nil, fmt.Errorf("skip: child next: %w", err)
	}
	if doc == nil {
		return nil, nil
	}

	n.stats.RowsIn++
	n.stats.RowsOut++
	return doc, nil
}

// Close releases resources.
func (n *SkipNode) Close() error {
	if !n.open {
		return nil
	}
	n.open = false
	return n.child.Close()
}

// Explain returns plan description.
func (n *SkipNode) Explain() ExplainNode {
	childExplain := n.child.Explain()
	return ExplainNode{
		NodeType: "SKIP",
		EstCost:  childExplain.EstCost,
		EstRows:  maxInt64(0, childExplain.EstRows-n.skip),
		Children: []ExplainNode{childExplain},
		Details: map[string]any{
			"skip": n.skip,
		},
	}
}

// Stats returns execution statistics.
func (n *SkipNode) Stats() NodeStats {
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

// LimitSkipNode combines limit and skip (common optimization).
type LimitSkipNode struct {
	child  PlanNode
	limit  int64
	offset int64

	// State
	open   bool
	seen   int64
	passed int64
	stats  NodeStats
}

// NewLimitSkipNode creates a combined limit/skip node.
func NewLimitSkipNode(child PlanNode, limit, offset int64) (*LimitSkipNode, error) {
	if limit < 0 {
		return nil, fmt.Errorf("limitskip: negative limit")
	}
	if offset < 0 {
		return nil, fmt.Errorf("limitskip: negative offset")
	}
	return &LimitSkipNode{
		child:  child,
		limit:  limit,
		offset: offset,
	}, nil
}

// Open initializes the node.
func (n *LimitSkipNode) Open(ctx context.Context) error {
	if n.open {
		return fmt.Errorf("limitskip: already open")
	}
	if err := n.child.Open(ctx); err != nil {
		return fmt.Errorf("limitskip: child open: %w", err)
	}
	n.open = true
	n.seen = 0
	n.passed = 0
	return nil
}

// Next returns the next document.
func (n *LimitSkipNode) Next() (*bson.Document, error) {
	if !n.open {
		return nil, fmt.Errorf("limitskip: not open")
	}
	if n.passed >= n.limit {
		return nil, nil
	}

	for {
		doc, err := n.child.Next()
		if err != nil {
			return nil, fmt.Errorf("limitskip: child next: %w", err)
		}
		if doc == nil {
			return nil, nil
		}

		n.seen++
		n.stats.RowsIn++

		// Skip until offset
		if n.seen <= n.offset {
			continue
		}

		n.passed++
		n.stats.RowsOut++
		return doc, nil
	}
}

// Close releases resources.
func (n *LimitSkipNode) Close() error {
	if !n.open {
		return nil
	}
	n.open = false
	return n.child.Close()
}

// Explain returns plan description.
func (n *LimitSkipNode) Explain() ExplainNode {
	childExplain := n.child.Explain()
	return ExplainNode{
		NodeType: "LIMIT_SKIP",
		EstCost:  childExplain.EstCost,
		EstRows:  minInt64(maxInt64(0, childExplain.EstRows-n.offset), n.limit),
		Children: []ExplainNode{childExplain},
		Details: map[string]any{
			"limit":  n.limit,
			"offset": n.offset,
		},
	}
}

// Stats returns execution statistics.
func (n *LimitSkipNode) Stats() NodeStats {
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

// Helper functions
func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
