package executor

import (
	"context"
	"fmt"
	"sort"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// SortNode sorts documents from its child node.
type SortNode struct {
	child     PlanNode
	sortSpec  *bson.Document
	limit     int64 // Optimization: if limit is small, use top-K

	// State
	open     bool
	sorted   []*bson.Document
	pos      int
	stats    NodeStats
}

// NewSortNode creates a sort node.
func NewSortNode(child PlanNode, sortSpec *bson.Document) (*SortNode, error) {
	if sortSpec == nil || sortSpec.Len() == 0 {
		return nil, fmt.Errorf("sort: empty sort specification")
	}
	return &SortNode{
		child:    child,
		sortSpec: sortSpec,
		limit:    -1,
	}, nil
}

// WithLimit adds a limit for top-K optimization.
// If limit > 0, only keeps the top K elements during sort.
func (n *SortNode) WithLimit(limit int64) *SortNode {
	n.limit = limit
	return n
}

// Open initializes the sort node.
func (n *SortNode) Open(ctx context.Context) error {
	if n.open {
		return fmt.Errorf("sort: already open")
	}
	if err := n.child.Open(ctx); err != nil {
		return fmt.Errorf("sort: child open: %w", err)
	}

	// Collect all documents
	var docs []*bson.Document
	for {
		doc, err := n.child.Next()
		if err != nil {
			n.child.Close()
			return fmt.Errorf("sort: collect: %w", err)
		}
		if doc == nil {
			break
		}
		n.stats.RowsIn++
		docs = append(docs, doc)
	}

	// Sort documents
	if n.limit > 0 && int64(len(docs)) > n.limit {
		// Top-K optimization using heap sort
		n.sorted = n.topK(docs, int(n.limit))
	} else {
		// Full sort
		sort.Slice(docs, func(i, j int) bool {
			return compareDocs(docs[i], docs[j], n.sortSpec) < 0
		})
		n.sorted = docs
	}

	n.stats.RowsOut = int64(len(n.sorted))
	n.open = true
	n.pos = 0
	return nil
}

// topK finds the top K elements using a partial sort.
func (n *SortNode) topK(docs []*bson.Document, k int) []*bson.Document {
	if len(docs) <= k {
		sort.Slice(docs, func(i, j int) bool {
			return compareDocs(docs[i], docs[j], n.sortSpec) < 0
		})
		return docs
	}

	// Use selection algorithm to find k-th smallest
	// Then sort only the top k
	// For simplicity, we'll use a heap-based approach
	heap := make([]*bson.Document, 0, k)

	for i, doc := range docs {
		if i < k {
			heap = append(heap, doc)
			if i == k-1 {
				// Build max-heap
				n.heapify(heap)
			}
		} else {
			// Compare with heap root (largest in top-k)
			if compareDocs(doc, heap[0], n.sortSpec) < 0 {
				heap[0] = doc
				n.siftDown(heap, 0)
			}
		}
	}

	// Sort the final result
	sort.Slice(heap, func(i, j int) bool {
		return compareDocs(heap[i], heap[j], n.sortSpec) < 0
	})

	return heap
}

// heapify builds a max-heap.
func (n *SortNode) heapify(docs []*bson.Document) {
	for i := len(docs)/2 - 1; i >= 0; i-- {
		n.siftDown(docs, i)
	}
}

// siftDown maintains heap property.
func (n *SortNode) siftDown(docs []*bson.Document, i int) {
	for {
		largest := i
		left := 2*i + 1
		right := 2*i + 2

		if left < len(docs) && compareDocs(docs[left], docs[largest], n.sortSpec) > 0 {
			largest = left
		}
		if right < len(docs) && compareDocs(docs[right], docs[largest], n.sortSpec) > 0 {
			largest = right
		}
		if largest == i {
			break
		}
		docs[i], docs[largest] = docs[largest], docs[i]
		i = largest
	}
}

// Next returns the next sorted document.
func (n *SortNode) Next() (*bson.Document, error) {
	if !n.open {
		return nil, fmt.Errorf("sort: not open")
	}
	if n.pos >= len(n.sorted) {
		return nil, nil
	}
	doc := n.sorted[n.pos]
	n.pos++
	return doc, nil
}

// Close releases resources.
func (n *SortNode) Close() error {
	if !n.open {
		return nil
	}
	n.open = false
	n.sorted = nil
	n.pos = 0
	return n.child.Close()
}

// Explain returns plan description.
func (n *SortNode) Explain() ExplainNode {
	childExplain := n.child.Explain()
	return ExplainNode{
		NodeType: "SORT",
		EstCost:  childExplain.EstCost + float64(childExplain.EstRows)*5, // Sort cost
		EstRows:  childExplain.EstRows,
		Children: []ExplainNode{childExplain},
		Details: map[string]any{
			"sortSpec": n.sortSpec,
			"limit":    n.limit,
		},
	}
}

// Stats returns execution statistics.
func (n *SortNode) Stats() NodeStats {
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

// compareDocs compares two documents according to sortSpec.
// Returns < 0 if a < b, 0 if equal, > 0 if a > b.
func compareDocs(a, b *bson.Document, sortSpec *bson.Document) int {
	for _, e := range sortSpec.Elements() {
		field := e.Key
		ascending := true

		// Parse sort direction
		switch e.Value.Type {
		case bson.TypeInt32:
			ascending = e.Value.Int32() > 0
		case bson.TypeInt64:
			ascending = e.Value.Int64() > 0
		case bson.TypeDouble:
			ascending = e.Value.Double() > 0
		case bson.TypeBoolean:
			ascending = e.Value.Boolean()
		}

		aVal, aOk := a.Get(field)
		bVal, bOk := b.Get(field)

		// Handle missing fields
		if !aOk && !bOk {
			continue // Both missing, compare next field
		}
		if !aOk {
			if ascending {
				return -1 // Missing < any value
			}
			return 1
		}
		if !bOk {
			if ascending {
				return 1 // Any value > missing
			}
			return -1
		}

		// Compare values
		cmp := bson.CompareValues(aVal, bVal)
		if cmp == 0 {
			continue // Equal, compare next field
		}
		if !ascending {
			cmp = -cmp
		}
		return cmp
	}
	return 0 // All fields equal
}
