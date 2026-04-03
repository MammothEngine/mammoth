// Package parser converts BSON query documents to an Abstract Syntax Tree (AST).
// The AST is used by the query planner and executor for efficient query processing.
package parser

import (
	"fmt"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// QueryNode is the interface for all nodes in the query AST.
type QueryNode interface {
	// Type returns the node type identifier.
	Type() NodeType

	// Evaluate evaluates this node against a document.
	// Returns true if the document matches.
	Evaluate(doc *bson.Document) bool

	// String returns a human-readable representation.
	String() string
}

// NodeType identifies the type of query node.
type NodeType string

const (
	// Comparison operators
	NodeEq    NodeType = "eq"    // $eq
	NodeNe    NodeType = "ne"    // $ne
	NodeGt    NodeType = "gt"    // $gt
	NodeGte   NodeType = "gte"   // $gte
	NodeLt    NodeType = "lt"    // $lt
	NodeLte   NodeType = "lte"   // $lte
	NodeIn    NodeType = "in"    // $in
	NodeNin   NodeType = "nin"   // $nin

	// Logical operators
	NodeAnd   NodeType = "and"   // $and
	NodeOr    NodeType = "or"    // $or
	NodeNot   NodeType = "not"   // $not
	NodeNor   NodeType = "nor"   // $nor

	// Element operators
	NodeExists NodeType = "exists" // $exists
	NodeTypeOp NodeType = "type"   // $type

	// Evaluation operators
	NodeRegex NodeType = "regex" // $regex
	NodeMod   NodeType = "mod"   // $mod
	NodeExpr  NodeType = "expr"  // $expr

	// Array operators
	NodeAll       NodeType = "all"       // $all
	NodeElemMatch NodeType = "elemMatch" // $elemMatch
	NodeSize      NodeType = "size"      // $size

	// Geospatial operators
	NodeNear        NodeType = "near"        // $near
	NodeNearSphere  NodeType = "nearSphere"  // $nearSphere
	NodeGeoWithin   NodeType = "geoWithin"   // $geoWithin
	NodeGeoIntersects NodeType = "geoIntersects" // $geoIntersects
)

// ComparisonNode represents a comparison operation on a field.
type ComparisonNode struct {
	Op    NodeType
	Field string
	Value bson.Value
}

// Type returns the node type.
func (n *ComparisonNode) Type() NodeType { return n.Op }

// Evaluate implements QueryNode.
func (n *ComparisonNode) Evaluate(doc *bson.Document) bool {
	fieldVal, found := resolveField(doc, n.Field)

	switch n.Op {
	case NodeEq:
		if !found {
			return n.Value.Type == bson.TypeNull
		}
		return compareValues(fieldVal, n.Value) == 0
	case NodeNe:
		if !found {
			return n.Value.Type != bson.TypeNull
		}
		return compareValues(fieldVal, n.Value) != 0
	case NodeGt:
		if !found {
			return false
		}
		return compareValues(fieldVal, n.Value) > 0
	case NodeGte:
		if !found {
			return false
		}
		return compareValues(fieldVal, n.Value) >= 0
	case NodeLt:
		if !found {
			return false
		}
		return compareValues(fieldVal, n.Value) < 0
	case NodeLte:
		if !found {
			return false
		}
		return compareValues(fieldVal, n.Value) <= 0
	default:
		return false
	}
}

// String implements QueryNode.
func (n *ComparisonNode) String() string {
	return fmt.Sprintf("{%s: {$%s: %v}}", n.Field, n.Op, n.Value.Interface())
}

// InNode represents $in and $nin operations.
type InNode struct {
	Op     NodeType // NodeIn or NodeNin
	Field  string
	Values []bson.Value
}

// Type returns the node type.
func (n *InNode) Type() NodeType { return n.Op }

// Evaluate implements QueryNode.
func (n *InNode) Evaluate(doc *bson.Document) bool {
	fieldVal, found := resolveField(doc, n.Field)

	// Check if field value matches any value in the array
	matches := func() bool {
		if !found {
			// For $in, null matches if null is in the array
			for _, v := range n.Values {
				if v.Type == bson.TypeNull {
					return true
				}
			}
			return false
		}

		// If field is an array, check if any element matches
		if fieldVal.Type == bson.TypeArray {
			arr := fieldVal.ArrayValue()
			for _, elem := range arr {
				for _, v := range n.Values {
					if compareValues(elem, v) == 0 {
						return true
					}
				}
			}
			return false
		}

		// Scalar comparison
		for _, v := range n.Values {
			if compareValues(fieldVal, v) == 0 {
				return true
			}
		}
		return false
	}

	if n.Op == NodeIn {
		return matches()
	}
	return !matches() // NodeNin
}

// String implements QueryNode.
func (n *InNode) String() string {
	return fmt.Sprintf("{%s: {$%s: %v}}", n.Field, n.Op, n.Values)
}

// LogicalNode represents logical combination of query nodes.
type LogicalNode struct {
	Op       NodeType
	Children []QueryNode
}

// Type returns the node type.
func (n *LogicalNode) Type() NodeType { return n.Op }

// Evaluate implements QueryNode.
func (n *LogicalNode) Evaluate(doc *bson.Document) bool {
	switch n.Op {
	case NodeAnd:
		for _, child := range n.Children {
			if !child.Evaluate(doc) {
				return false
			}
		}
		return true
	case NodeOr:
		for _, child := range n.Children {
			if child.Evaluate(doc) {
				return true
			}
		}
		return false
	case NodeNot:
		if len(n.Children) > 0 {
			return !n.Children[0].Evaluate(doc)
		}
		return true
	case NodeNor:
		for _, child := range n.Children {
			if child.Evaluate(doc) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// String implements QueryNode.
func (n *LogicalNode) String() string {
	return fmt.Sprintf("{$%s: %v}", n.Op, n.Children)
}

// ExistsNode represents an $exists operation.
type ExistsNode struct {
	Field  string
	Exists bool // true = must exist, false = must not exist
}

// Type returns the node type.
func (n *ExistsNode) Type() NodeType { return NodeExists }

// Evaluate implements QueryNode.
func (n *ExistsNode) Evaluate(doc *bson.Document) bool {
	_, found := resolveField(doc, n.Field)
	return found == n.Exists
}

// String implements QueryNode.
func (n *ExistsNode) String() string {
	return fmt.Sprintf("{%s: {$exists: %v}}", n.Field, n.Exists)
}

// TypeNode represents a $type operation.
type TypeNode struct {
	Field     string
	TypeAlias string // "double", "string", "object", "array", "bool", "null", etc.
}

// Type returns the node type.
func (n *TypeNode) Type() NodeType { return NodeTypeOp }

// Evaluate implements QueryNode.
func (n *TypeNode) Evaluate(doc *bson.Document) bool {
	fieldVal, found := resolveField(doc, n.Field)
	if !found {
		return n.TypeAlias == "null"
	}
	return bsonTypeName(fieldVal.Type) == n.TypeAlias ||
		bsonTypeAlias(fieldVal.Type) == n.TypeAlias
}

// String implements QueryNode.
func (n *TypeNode) String() string {
	return fmt.Sprintf("{%s: {$type: %s}}", n.Field, n.TypeAlias)
}

// RegexNode represents a $regex operation.
type RegexNode struct {
	Field   string
	Pattern string
	Options string // "i"=case-insensitive, "m"=multiline, etc.
}

// Type returns the node type.
func (n *RegexNode) Type() NodeType { return NodeRegex }

// Evaluate implements QueryNode.
func (n *RegexNode) Evaluate(doc *bson.Document) bool {
	fieldVal, found := resolveField(doc, n.Field)
	if !found || fieldVal.Type != bson.TypeString {
		return false
	}

	re := getCachedRegex(n.Pattern, n.Options)
	if re == nil {
		return false
	}
	return re.MatchString(fieldVal.String())
}

// String implements QueryNode.
func (n *RegexNode) String() string {
	if n.Options != "" {
		return fmt.Sprintf("{%s: {$regex: \"%s\", $options: \"%s\"}}", n.Field, n.Pattern, n.Options)
	}
	return fmt.Sprintf("{%s: {$regex: \"%s\"}}", n.Field, n.Pattern)
}

// AllNode represents an $all operation.
type AllNode struct {
	Field  string
	Values []bson.Value
}

// Type returns the node type.
func (n *AllNode) Type() NodeType { return NodeAll }

// Evaluate implements QueryNode.
func (n *AllNode) Evaluate(doc *bson.Document) bool {
	fieldVal, found := resolveField(doc, n.Field)
	if !found || fieldVal.Type != bson.TypeArray {
		return false
	}

	arr := fieldVal.ArrayValue()
	for _, requiredVal := range n.Values {
		found := false
		for _, elem := range arr {
			if compareValues(elem, requiredVal) == 0 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// String implements QueryNode.
func (n *AllNode) String() string {
	return fmt.Sprintf("{%s: {$all: %v}}", n.Field, n.Values)
}

// ElemMatchNode represents an $elemMatch operation.
type ElemMatchNode struct {
	Field string
	Query QueryNode
}

// Type returns the node type.
func (n *ElemMatchNode) Type() NodeType { return NodeElemMatch }

// Evaluate implements QueryNode.
func (n *ElemMatchNode) Evaluate(doc *bson.Document) bool {
	fieldVal, found := resolveField(doc, n.Field)
	if !found || fieldVal.Type != bson.TypeArray {
		return false
	}

	arr := fieldVal.ArrayValue()
	for _, elem := range arr {
		if elem.Type != bson.TypeDocument {
			continue
		}
		if n.Query.Evaluate(elem.DocumentValue()) {
			return true
		}
	}
	return false
}

// String implements QueryNode.
func (n *ElemMatchNode) String() string {
	return fmt.Sprintf("{%s: {$elemMatch: %s}}", n.Field, n.Query.String())
}

// SizeNode represents a $size operation.
type SizeNode struct {
	Field string
	Size  int
}

// Type returns the node type.
func (n *SizeNode) Type() NodeType { return NodeSize }

// Evaluate implements QueryNode.
func (n *SizeNode) Evaluate(doc *bson.Document) bool {
	fieldVal, found := resolveField(doc, n.Field)
	if !found || fieldVal.Type != bson.TypeArray {
		return false
	}
	return len(fieldVal.ArrayValue()) == n.Size
}

// String implements QueryNode.
func (n *SizeNode) String() string {
	return fmt.Sprintf("{%s: {$size: %d}}", n.Field, n.Size)
}

// And combines multiple query nodes with AND logic.
func And(nodes ...QueryNode) QueryNode {
	if len(nodes) == 0 {
		return &LogicalNode{Op: NodeAnd, Children: nil}
	}
	if len(nodes) == 1 {
		return nodes[0]
	}
	return &LogicalNode{Op: NodeAnd, Children: nodes}
}

// Or combines multiple query nodes with OR logic.
func Or(nodes ...QueryNode) QueryNode {
	if len(nodes) == 0 {
		return &LogicalNode{Op: NodeOr, Children: nil}
	}
	if len(nodes) == 1 {
		return nodes[0]
	}
	return &LogicalNode{Op: NodeOr, Children: nodes}
}

// Not negates a query node.
func Not(node QueryNode) QueryNode {
	return &LogicalNode{Op: NodeNot, Children: []QueryNode{node}}
}

// Nor combines multiple query nodes with NOR logic (NOT OR).
func Nor(nodes ...QueryNode) QueryNode {
	return &LogicalNode{Op: NodeNor, Children: nodes}
}
