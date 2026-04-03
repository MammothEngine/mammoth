package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Parse converts a BSON filter document to a QueryNode AST.
// This is the main entry point for query parsing.
func Parse(filter *bson.Document) (QueryNode, error) {
	if filter == nil || filter.Len() == 0 {
		// Empty filter matches everything
		return &LogicalNode{Op: NodeAnd, Children: nil}, nil
	}

	nodes := make([]QueryNode, 0, filter.Len())

	for _, elem := range filter.Elements() {
		if strings.HasPrefix(elem.Key, "$") {
			// Top-level operator ($and, $or, $nor, $text, etc.)
			node, err := parseTopLevelOperator(elem.Key, elem.Value)
			if err != nil {
				return nil, fmt.Errorf("parse top-level %s: %w", elem.Key, err)
			}
			nodes = append(nodes, node)
		} else {
			// Field condition
			node, err := parseFieldCondition(elem.Key, elem.Value)
			if err != nil {
				return nil, fmt.Errorf("parse field %s: %w", elem.Key, err)
			}
			nodes = append(nodes, node)
		}
	}

	// Combine multiple field conditions with AND
	if len(nodes) == 1 {
		return nodes[0], nil
	}
	return And(nodes...), nil
}

// parseTopLevelOperator parses operators like $and, $or, $nor.
func parseTopLevelOperator(op string, value bson.Value) (QueryNode, error) {
	switch op {
	case "$and":
		return parseLogicalArray(NodeAnd, value)
	case "$or":
		return parseLogicalArray(NodeOr, value)
	case "$nor":
		return parseLogicalArray(NodeNor, value)
	case "$not":
		if value.Type != bson.TypeDocument {
			return nil, fmt.Errorf("$not requires a document")
		}
		child, err := Parse(value.DocumentValue())
		if err != nil {
			return nil, err
		}
		return Not(child), nil
	case "$text":
		// $text search is handled separately
		return parseTextSearch(value)
	case "$where":
		// $where is deprecated and not supported
		return nil, fmt.Errorf("$where is not supported")
	case "$comment":
		// $comment is ignored (it's just for documentation)
		return &LogicalNode{Op: NodeAnd}, nil
	default:
		return nil, fmt.Errorf("unknown top-level operator: %s", op)
	}
}

// parseLogicalArray parses [$and, $or, $nor] with an array of expressions.
func parseLogicalArray(op NodeType, value bson.Value) (QueryNode, error) {
	if value.Type != bson.TypeArray {
		return nil, fmt.Errorf("%s requires an array", op)
	}

	arr := value.ArrayValue()
	if len(arr) == 0 {
		return nil, fmt.Errorf("%s array cannot be empty", op)
	}

	children := make([]QueryNode, 0, len(arr))
	for i, elem := range arr {
		if elem.Type != bson.TypeDocument {
			return nil, fmt.Errorf("%s[%d] must be a document", op, i)
		}
		child, err := Parse(elem.DocumentValue())
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", op, i, err)
		}
		children = append(children, child)
	}

	return &LogicalNode{Op: op, Children: children}, nil
}

// parseFieldCondition parses a condition on a field.
func parseFieldCondition(field string, value bson.Value) (QueryNode, error) {
	// Check if value is an operator document
	if value.Type == bson.TypeDocument {
		opDoc := value.DocumentValue()

		// Check if this is an operator expression or a literal document
		isOperatorDoc := false
		for _, key := range opDoc.Keys() {
			if strings.HasPrefix(key, "$") {
				isOperatorDoc = true
				break
			}
		}

		if isOperatorDoc {
			return parseOperatorDocument(field, opDoc)
		}
	}

	// Implicit $eq: {field: value}
	return &ComparisonNode{
		Op:    NodeEq,
		Field: field,
		Value: value,
	}, nil
}

// parseOperatorDocument parses an operator document like {$gt: 5, $lt: 10}.
func parseOperatorDocument(field string, opDoc *bson.Document) (QueryNode, error) {
	if opDoc.Len() == 0 {
		return nil, fmt.Errorf("empty operator document for field %s", field)
	}

	// If there's only one operator, return it directly
	if opDoc.Len() == 1 {
		key := opDoc.Keys()[0]
		val, _ := opDoc.Get(key)
		return parseOperator(field, key, val, opDoc)
	}

	// Multiple operators: combine with AND
	nodes := make([]QueryNode, 0, opDoc.Len())
	for _, elem := range opDoc.Elements() {
		if !strings.HasPrefix(elem.Key, "$") {
			return nil, fmt.Errorf("invalid operator: %s", elem.Key)
		}
		// Skip $options - it's handled as part of $regex
		if elem.Key == "$options" {
			continue
		}
		node, err := parseOperator(field, elem.Key, elem.Value, opDoc)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return And(nodes...), nil
}

// parseOperator parses a single operator.
func parseOperator(field, op string, value bson.Value, parentDoc *bson.Document) (QueryNode, error) {
	switch op {
	// Comparison operators
	case "$eq":
		return &ComparisonNode{Op: NodeEq, Field: field, Value: value}, nil
	case "$ne":
		return &ComparisonNode{Op: NodeNe, Field: field, Value: value}, nil
	case "$gt":
		return &ComparisonNode{Op: NodeGt, Field: field, Value: value}, nil
	case "$gte":
		return &ComparisonNode{Op: NodeGte, Field: field, Value: value}, nil
	case "$lt":
		return &ComparisonNode{Op: NodeLt, Field: field, Value: value}, nil
	case "$lte":
		return &ComparisonNode{Op: NodeLte, Field: field, Value: value}, nil
	case "$in":
		return parseInOperator(field, value, false)
	case "$nin":
		return parseInOperator(field, value, true)

	// Element operators
	case "$exists":
		exists := toBool(value)
		return &ExistsNode{Field: field, Exists: exists}, nil
	case "$type":
		typeAlias := toString(value)
		if typeAlias == "" {
			return nil, fmt.Errorf("$type requires a string or number")
		}
		return &TypeNode{Field: field, TypeAlias: typeAlias}, nil

	// Evaluation operators
	case "$regex":
		return parseRegexOperator(field, value, parentDoc)
	case "$options":
		// $options is only valid with $regex
		return nil, fmt.Errorf("$options must be paired with $regex")
	case "$mod":
		return parseModOperator(field, value)
	case "$expr":
		return nil, fmt.Errorf("$expr not yet implemented")

	// Array operators
	case "$all":
		return parseAllOperator(field, value)
	case "$elemMatch":
		return parseElemMatchOperator(field, value)
	case "$size":
		size := toInt(value)
		if size < 0 {
			return nil, fmt.Errorf("$size requires a non-negative integer")
		}
		return &SizeNode{Field: field, Size: size}, nil

	// Geospatial operators
	case "$near":
		return parseNearOperator(field, value)
	case "$nearSphere":
		return parseNearSphereOperator(field, value)
	case "$geoWithin":
		return parseGeoWithinOperator(field, value)
	case "$geoIntersects":
		return parseGeoIntersectsOperator(field, value)

	default:
		return nil, fmt.Errorf("unknown operator: %s", op)
	}
}

// parseInOperator parses $in or $nin.
func parseInOperator(field string, value bson.Value, negate bool) (QueryNode, error) {
	if value.Type != bson.TypeArray {
		return nil, fmt.Errorf("$in/$nin requires an array")
	}

	op := NodeIn
	if negate {
		op = NodeNin
	}

	return &InNode{
		Op:     op,
		Field:  field,
		Values: value.ArrayValue(),
	}, nil
}

// parseRegexOperator parses $regex with optional $options.
func parseRegexOperator(field string, value bson.Value, parentDoc *bson.Document) (QueryNode, error) {
	if value.Type != bson.TypeString && value.Type != bson.TypeRegex {
		return nil, fmt.Errorf("$regex requires a string or regex")
	}

	pattern := ""
	options := ""

	if value.Type == bson.TypeRegex {
		re := value.Regex()
		pattern = re.Pattern
		options = re.Options
	} else {
		pattern = value.String()
	}

	// Check parent document for $options
	if parentDoc != nil {
		if optVal, ok := parentDoc.Get("$options"); ok {
			options = toString(optVal)
		}
	}

	return &RegexNode{
		Field:   field,
		Pattern: pattern,
		Options: options,
	}, nil
}

// parseModOperator parses $mod: [divisor, remainder].
func parseModOperator(field string, value bson.Value) (QueryNode, error) {
	if value.Type != bson.TypeArray {
		return nil, fmt.Errorf("$mod requires an array [divisor, remainder]")
	}

	arr := value.ArrayValue()
	if len(arr) != 2 {
		return nil, fmt.Errorf("$mod requires exactly 2 elements [divisor, remainder]")
	}

	// For now, return a placeholder - full implementation would need ModNode
	return &ComparisonNode{
		Op:    NodeMod,
		Field: field,
		Value: value,
	}, nil
}

// parseAllOperator parses $all.
func parseAllOperator(field string, value bson.Value) (QueryNode, error) {
	if value.Type != bson.TypeArray {
		return nil, fmt.Errorf("$all requires an array")
	}

	return &AllNode{
		Field:  field,
		Values: value.ArrayValue(),
	}, nil
}

// parseElemMatchOperator parses $elemMatch.
func parseElemMatchOperator(field string, value bson.Value) (QueryNode, error) {
	if value.Type != bson.TypeDocument {
		return nil, fmt.Errorf("$elemMatch requires a document")
	}

	query, err := Parse(value.DocumentValue())
	if err != nil {
		return nil, fmt.Errorf("$elemMatch: %w", err)
	}

	return &ElemMatchNode{
		Field: field,
		Query: query,
	}, nil
}

// parseTextSearch parses $text search.
func parseTextSearch(value bson.Value) (QueryNode, error) {
	if value.Type != bson.TypeDocument {
		return nil, fmt.Errorf("$text requires a document")
	}
	// Return a placeholder node for now
	// Full implementation would create a TextSearchNode
	return &LogicalNode{Op: NodeAnd}, nil
}

// Geospatial operators (placeholders)
func parseNearOperator(field string, value bson.Value) (QueryNode, error) {
	return &ComparisonNode{Op: NodeNear, Field: field, Value: value}, nil
}

func parseNearSphereOperator(field string, value bson.Value) (QueryNode, error) {
	return &ComparisonNode{Op: NodeNearSphere, Field: field, Value: value}, nil
}

func parseGeoWithinOperator(field string, value bson.Value) (QueryNode, error) {
	return &ComparisonNode{Op: NodeGeoWithin, Field: field, Value: value}, nil
}

func parseGeoIntersectsOperator(field string, value bson.Value) (QueryNode, error) {
	return &ComparisonNode{Op: NodeGeoIntersects, Field: field, Value: value}, nil
}

// toBool converts a value to boolean.
func toBool(v bson.Value) bool {
	switch v.Type {
	case bson.TypeBoolean:
		return v.Boolean()
	case bson.TypeInt32:
		return v.Int32() != 0
	case bson.TypeInt64:
		return v.Int64() != 0
	case bson.TypeDouble:
		return v.Double() != 0
	case bson.TypeNull:
		return false
	default:
		return true // Non-empty values are truthy
	}
}

// toString converts a value to string.
func toString(v bson.Value) string {
	switch v.Type {
	case bson.TypeString:
		return v.String()
	case bson.TypeInt32:
		return strconv.Itoa(int(v.Int32()))
	case bson.TypeInt64:
		return strconv.FormatInt(v.Int64(), 10)
	case bson.TypeDouble:
		return strconv.FormatFloat(v.Double(), 'f', -1, 64)
	default:
		return ""
	}
}

// toInt converts a value to int.
func toInt(v bson.Value) int {
	switch v.Type {
	case bson.TypeInt32:
		return int(v.Int32())
	case bson.TypeInt64:
		return int(v.Int64())
	case bson.TypeDouble:
		return int(v.Double())
	default:
		return -1
	}
}
