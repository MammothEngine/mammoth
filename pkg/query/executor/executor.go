// Package executor implements the Volcano iterator model for query execution.
// Each operation is a node in a tree that produces tuples one at a time via Next().
package executor

import (
	"context"
	"fmt"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// PlanNode is the interface for all query execution operators.
// Implements the Volcano iterator model: Open -> Next -> Close.
type PlanNode interface {
	// Open initializes the operator. Called once before first Next().
	Open(ctx context.Context) error

	// Next returns the next tuple (document) or nil if exhausted.
	// Returns (nil, nil) when no more rows.
	Next() (*bson.Document, error)

	// Close cleans up resources. Called once after iteration.
	Close() error

	// Explain returns execution plan description for debugging.
	Explain() ExplainNode
}

// ExplainNode describes a node in the execution plan.
type ExplainNode struct {
	NodeType string
	EstCost  float64
	EstRows  int64
	Children []ExplainNode
	Details  map[string]any
}

// ExplainResult is the root of an explain tree.
type ExplainResult struct {
	Root       ExplainNode
	TotalCost  float64
	PlanType   string // "COLLSCAN", "IXSCAN", etc.
}

// ResultSet holds the output of query execution.
type ResultSet struct {
	docs     []*bson.Document
	position int
	closed   bool
}

// NewResultSet creates a result set from documents.
func NewResultSet(docs []*bson.Document) *ResultSet {
	return &ResultSet{docs: docs}
}

// Next returns the next document.
func (r *ResultSet) Next() (*bson.Document, error) {
	if r.closed || r.position >= len(r.docs) {
		return nil, nil
	}
	doc := r.docs[r.position]
	r.position++
	return doc, nil
}

// HasNext returns true if there are more documents.
func (r *ResultSet) HasNext() bool {
	return !r.closed && r.position < len(r.docs)
}

// Close releases the result set.
func (r *ResultSet) Close() {
	r.closed = true
	r.docs = nil
}

// All returns all remaining documents.
func (r *ResultSet) All() ([]*bson.Document, error) {
	if r.closed {
		return nil, fmt.Errorf("result set closed")
	}
	result := r.docs[r.position:]
	r.position = len(r.docs)
	return result, nil
}

// Exec executes a plan node and returns all results.
func Exec(ctx context.Context, node PlanNode) ([]*bson.Document, error) {
	if err := node.Open(ctx); err != nil {
		return nil, fmt.Errorf("executor: open: %w", err)
	}
	defer node.Close()

	var results []*bson.Document
	for {
		doc, err := node.Next()
		if err != nil {
			return nil, fmt.Errorf("executor: next: %w", err)
		}
		if doc == nil {
			break
		}
		results = append(results, doc)
	}
	return results, nil
}

// ExecLimit executes with a limit (for memory safety).
func ExecLimit(ctx context.Context, node PlanNode, limit int) ([]*bson.Document, error) {
	if err := node.Open(ctx); err != nil {
		return nil, fmt.Errorf("executor: open: %w", err)
	}
	defer node.Close()

	var results []*bson.Document
	count := 0
	for count < limit {
		doc, err := node.Next()
		if err != nil {
			return nil, fmt.Errorf("executor: next: %w", err)
		}
		if doc == nil {
			break
		}
		results = append(results, doc)
		count++
	}
	return results, nil
}

// PlanNodeWithStats is a node that can report statistics.
type PlanNodeWithStats interface {
	PlanNode
	Stats() NodeStats
}

// NodeStats contains execution statistics.
type NodeStats struct {
	RowsIn     int64
	RowsOut    int64
	ExecTimeMs int64
	SpillCount int64 // For external sort
}
