package executor

import (
	"context"
	"fmt"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// MatchFunc is a function that matches documents.
type MatchFunc func(doc *bson.Document) bool

// CollScanNode performs a full collection scan (table scan).
type CollScanNode struct {
	engine     *engine.Engine
	prefix     []byte // Namespace prefix for the collection
	filter     MatchFunc
	projection *bson.Document

	// Iterator state
	scanning bool
	results  []*bson.Document
	pos      int

	// Stats
	stats NodeStats
}

// NewCollScanNode creates a new collection scan node.
func NewCollScanNode(eng *engine.Engine, db, coll string) *CollScanNode {
	return &CollScanNode{
		engine: eng,
		prefix: mongo.EncodeNamespacePrefix(db, coll),
	}
}

// WithFilter adds a filter predicate.
func (n *CollScanNode) WithFilter(filter *bson.Document) *CollScanNode {
	if filter != nil && filter.Len() > 0 {
		n.filter = mongo.NewMatcher(filter).Match
	}
	return n
}

// SetFilter sets a raw match function.
func (n *CollScanNode) SetFilter(fn MatchFunc) {
	n.filter = fn
}

// Filter returns the current filter.
func (n *CollScanNode) Filter() MatchFunc {
	return n.filter
}

// WithProjection adds field projection.
func (n *CollScanNode) WithProjection(proj *bson.Document) *CollScanNode {
	n.projection = proj
	return n
}

// Open initializes the scan by reading all matching documents.
func (n *CollScanNode) Open(ctx context.Context) error {
	if n.scanning {
		return fmt.Errorf("collscan: already open")
	}

	n.results = nil
	n.pos = 0

	// Scan all documents in collection
	err := n.engine.Scan(n.prefix, func(key, value []byte) bool {
		n.stats.RowsIn++

		doc, err := bson.Decode(value)
		if err != nil {
			return true // Skip corrupt documents
		}

		// Apply filter if present
		if n.filter != nil && !n.filter(doc) {
			return true
		}

		// Apply projection if present
		if n.projection != nil && n.projection.Len() > 0 {
			doc = mongo.ApplyProjection(doc, n.projection)
		}

		n.results = append(n.results, doc)
		n.stats.RowsOut++
		return true
	})

	if err != nil {
		return fmt.Errorf("collscan: engine scan: %w", err)
	}

	n.scanning = true
	return nil
}

// Next returns the next document.
func (n *CollScanNode) Next() (*bson.Document, error) {
	if !n.scanning {
		return nil, fmt.Errorf("collscan: not open")
	}
	if n.pos >= len(n.results) {
		return nil, nil
	}
	doc := n.results[n.pos]
	n.pos++
	return doc, nil
}

// Close releases resources.
func (n *CollScanNode) Close() error {
	n.scanning = false
	n.results = nil
	n.pos = 0
	return nil
}

// Explain returns plan description.
func (n *CollScanNode) Explain() ExplainNode {
	return ExplainNode{
		NodeType: "COLLSCAN",
		EstCost:  float64(len(n.results)) * 10, // Sequential scan cost
		EstRows:  int64(len(n.results)),
		Details: map[string]any{
			"prefix":    string(n.prefix),
			"hasFilter": n.filter != nil,
		},
	}
}

// Stats returns execution statistics.
func (n *CollScanNode) Stats() NodeStats {
	return n.stats
}

// IndexScanNode performs an index-based lookup.
type IndexScanNode struct {
	engine     *engine.Engine
	db         string
	coll       string
	spec       *mongo.IndexSpec
	bounds     IndexBounds
	filter     MatchFunc
	projection *bson.Document

	// Iterator state
	keys     [][]byte
	docs     []*bson.Document
	pos      int
	open     bool

	// Stats
	stats NodeStats
}

// IndexBounds describes the scan range for an index.
type IndexBounds struct {
	LowerBound []byte
	UpperBound []byte
	LowerInc   bool // Lower bound inclusive
	UpperInc   bool // Upper bound inclusive
	Equality   bool // Exact match lookup
}

// NewIndexScanNode creates a new index scan node.
func NewIndexScanNode(eng *engine.Engine, db, coll string, spec *mongo.IndexSpec, bounds IndexBounds) *IndexScanNode {
	return &IndexScanNode{
		engine: eng,
		db:     db,
		coll:   coll,
		spec:   spec,
		bounds: bounds,
	}
}

// WithFilter adds a residual filter predicate.
func (n *IndexScanNode) WithFilter(filter *bson.Document) *IndexScanNode {
	if filter != nil && filter.Len() > 0 {
		n.filter = mongo.NewMatcher(filter).Match
	}
	return n
}

// SetFilter sets a raw match function.
func (n *IndexScanNode) SetFilter(fn MatchFunc) {
	n.filter = fn
}

// Filter returns the current filter.
func (n *IndexScanNode) Filter() MatchFunc {
	return n.filter
}

// WithProjection adds field projection.
func (n *IndexScanNode) WithProjection(proj *bson.Document) *IndexScanNode {
	n.projection = proj
	return n
}

// Open initializes the index scan.
func (n *IndexScanNode) Open(ctx context.Context) error {
	if n.open {
		return fmt.Errorf("indexscan: already open")
	}

	// Build index prefix for scanning
	ns := mongo.EncodeNamespacePrefix(n.db, n.coll)
	prefix := make([]byte, 0, len(ns)+len(indexSeparator)+len(n.spec.Name))
	prefix = append(prefix, ns...)
	prefix = append(prefix, indexSeparator...)
	prefix = append(prefix, n.spec.Name...)

	// Scan index entries
	var ids [][]byte
	_ = n.engine.Scan(prefix, func(key, _ []byte) bool {
		if len(key) > len(prefix) {
			idBytes := key[len(prefix):]
			ids = append(ids, append([]byte{}, idBytes...))
		}
		return true
	})

	// Fetch documents by ID
	for _, id := range ids {
		n.stats.RowsIn++

		key := mongo.EncodeDocumentKey(n.db, n.coll, id)
		val, err := n.engine.Get(key)
		if err != nil {
			continue // Skip missing documents
		}

		doc, err := bson.Decode(val)
		if err != nil {
			continue // Skip corrupt documents
		}

		// Apply residual filter if present
		if n.filter != nil && !n.filter(doc) {
			continue
		}

		// Apply projection
		if n.projection != nil && n.projection.Len() > 0 {
			doc = mongo.ApplyProjection(doc, n.projection)
		}

		n.docs = append(n.docs, doc)
		n.stats.RowsOut++
	}

	n.open = true
	n.pos = 0
	return nil
}

// Next returns the next document.
func (n *IndexScanNode) Next() (*bson.Document, error) {
	if !n.open {
		return nil, fmt.Errorf("indexscan: not open")
	}
	if n.pos >= len(n.docs) {
		return nil, nil
	}
	doc := n.docs[n.pos]
	n.pos++
	return doc, nil
}

// Close releases resources.
func (n *IndexScanNode) Close() error {
	n.open = false
	n.docs = nil
	n.keys = nil
	n.pos = 0
	return nil
}

// Explain returns plan description.
func (n *IndexScanNode) Explain() ExplainNode {
	node := ExplainNode{
		NodeType: "IXSCAN",
		EstCost:  float64(len(n.docs)) * 2, // Index lookup is cheaper
		EstRows:  int64(len(n.docs)),
		Details: map[string]any{
			"indexName": n.spec.Name,
			"equality":  n.bounds.Equality,
		},
	}
	if n.bounds.LowerBound != nil {
		node.Details["lowerBound"] = string(n.bounds.LowerBound)
	}
	if n.bounds.UpperBound != nil {
		node.Details["upperBound"] = string(n.bounds.UpperBound)
	}
	return node
}

// Stats returns execution statistics.
func (n *IndexScanNode) Stats() NodeStats {
	return n.stats
}

// EmptyScanNode returns no documents (for optimizations).
type EmptyScanNode struct{}

// NewEmptyScanNode creates an empty scan node.
func NewEmptyScanNode() *EmptyScanNode {
	return &EmptyScanNode{}
}

// Open initializes the node.
func (n *EmptyScanNode) Open(ctx context.Context) error { return nil }

// Next always returns nil (no rows).
func (n *EmptyScanNode) Next() (*bson.Document, error) { return nil, nil }

// Close releases resources.
func (n *EmptyScanNode) Close() error { return nil }

// Explain returns plan description.
func (n *EmptyScanNode) Explain() ExplainNode {
	return ExplainNode{
		NodeType: "EMPTY",
		EstCost:  0,
		EstRows:  0,
	}
}

// indexSeparator is used to separate namespace from index name in keys.
var indexSeparator = []byte{0x00, 'i', 'd', 'x'}
