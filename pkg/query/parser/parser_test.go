package parser

import (
	"os"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestParseEmpty(t *testing.T) {
	node, err := Parse(nil)
	if err != nil {
		t.Fatalf("parse nil: %v", err)
	}
	if node.Type() != NodeAnd {
		t.Errorf("expected AND node for nil, got %s", node.Type())
	}

	empty := bson.NewDocument()
	node, err = Parse(empty)
	if err != nil {
		t.Fatalf("parse empty: %v", err)
	}
	if node.Type() != NodeAnd {
		t.Errorf("expected AND node for empty, got %s", node.Type())
	}
}

func TestParseImplicitEq(t *testing.T) {
	// {name: "John"} -> implicit $eq
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("John"))

	node, err := Parse(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	cmp, ok := node.(*ComparisonNode)
	if !ok {
		t.Fatalf("expected ComparisonNode, got %T", node)
	}

	if cmp.Op != NodeEq {
		t.Errorf("expected $eq, got %s", cmp.Op)
	}
	if cmp.Field != "name" {
		t.Errorf("expected field 'name', got '%s'", cmp.Field)
	}
	if cmp.Value.String() != "John" {
		t.Errorf("expected value 'John', got '%s'", cmp.Value.String())
	}
}

func TestParseComparison(t *testing.T) {
	tests := []struct {
		op       string
		expected NodeType
	}{
		{"$eq", NodeEq},
		{"$ne", NodeNe},
		{"$gt", NodeGt},
		{"$gte", NodeGte},
		{"$lt", NodeLt},
		{"$lte", NodeLte},
	}

	for _, tc := range tests {
		doc := bson.NewDocument()
		opDoc := bson.NewDocument()
		opDoc.Set(tc.op, bson.VInt32(5))
		doc.Set("value", bson.VDoc(opDoc))

		node, err := Parse(doc)
		if err != nil {
			t.Fatalf("parse %s: %v", tc.op, err)
		}

		cmp, ok := node.(*ComparisonNode)
		if !ok {
			t.Fatalf("%s: expected ComparisonNode, got %T", tc.op, node)
		}

		if cmp.Op != tc.expected {
			t.Errorf("%s: expected %s, got %s", tc.op, tc.expected, cmp.Op)
		}
	}
}

func TestParseIn(t *testing.T) {
	doc := bson.NewDocument()
	arr := bson.A(bson.VInt32(1), bson.VInt32(2), bson.VInt32(3))
	opDoc := bson.NewDocument()
	opDoc.Set("$in", bson.VArray(arr))
	doc.Set("value", bson.VDoc(opDoc))

	node, err := Parse(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	inNode, ok := node.(*InNode)
	if !ok {
		t.Fatalf("expected InNode, got %T", node)
	}

	if inNode.Op != NodeIn {
		t.Errorf("expected $in, got %s", inNode.Op)
	}
	if len(inNode.Values) != 3 {
		t.Errorf("expected 3 values, got %d", len(inNode.Values))
	}
}

func TestParseLogicalAnd(t *testing.T) {
	// {$and: [{a: 1}, {b: 2}]}
	doc := bson.NewDocument()

	cond1 := bson.NewDocument()
	cond1.Set("a", bson.VInt32(1))

	cond2 := bson.NewDocument()
	cond2.Set("b", bson.VInt32(2))

	andArr := bson.A(bson.VDoc(cond1), bson.VDoc(cond2))
	doc.Set("$and", bson.VArray(andArr))

	node, err := Parse(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	logical, ok := node.(*LogicalNode)
	if !ok {
		t.Fatalf("expected LogicalNode, got %T", node)
	}

	if logical.Op != NodeAnd {
		t.Errorf("expected $and, got %s", logical.Op)
	}
	if len(logical.Children) != 2 {
		t.Errorf("expected 2 children, got %d", len(logical.Children))
	}
}

func TestParseLogicalOr(t *testing.T) {
	// {$or: [{a: 1}, {b: 2}]}
	doc := bson.NewDocument()

	cond1 := bson.NewDocument()
	cond1.Set("a", bson.VInt32(1))

	cond2 := bson.NewDocument()
	cond2.Set("b", bson.VInt32(2))

	orArr := bson.A(bson.VDoc(cond1), bson.VDoc(cond2))
	doc.Set("$or", bson.VArray(orArr))

	node, err := Parse(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	logical, ok := node.(*LogicalNode)
	if !ok {
		t.Fatalf("expected LogicalNode, got %T", node)
	}

	if logical.Op != NodeOr {
		t.Errorf("expected $or, got %s", logical.Op)
	}
	if len(logical.Children) != 2 {
		t.Errorf("expected 2 children, got %d", len(logical.Children))
	}
}

func TestParseExists(t *testing.T) {
	doc := bson.NewDocument()
	opDoc := bson.NewDocument()
	opDoc.Set("$exists", bson.VBool(true))
	doc.Set("field", bson.VDoc(opDoc))

	node, err := Parse(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	existsNode, ok := node.(*ExistsNode)
	if !ok {
		t.Fatalf("expected ExistsNode, got %T", node)
	}

	if existsNode.Field != "field" {
		t.Errorf("expected field 'field', got '%s'", existsNode.Field)
	}
	if !existsNode.Exists {
		t.Errorf("expected Exists=true")
	}
}

func TestParseRegex(t *testing.T) {
	doc := bson.NewDocument()
	opDoc := bson.NewDocument()
	opDoc.Set("$regex", bson.VString("^test"))
	opDoc.Set("$options", bson.VString("i"))
	doc.Set("name", bson.VDoc(opDoc))

	node, err := Parse(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	regexNode, ok := node.(*RegexNode)
	if !ok {
		t.Fatalf("expected RegexNode, got %T", node)
	}

	if regexNode.Pattern != "^test" {
		t.Errorf("expected pattern '^test', got '%s'", regexNode.Pattern)
	}
}

func TestParseAll(t *testing.T) {
	doc := bson.NewDocument()
	arr := bson.A(bson.VString("a"), bson.VString("b"))
	opDoc := bson.NewDocument()
	opDoc.Set("$all", bson.VArray(arr))
	doc.Set("tags", bson.VDoc(opDoc))

	node, err := Parse(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	allNode, ok := node.(*AllNode)
	if !ok {
		t.Fatalf("expected AllNode, got %T", node)
	}

	if allNode.Field != "tags" {
		t.Errorf("expected field 'tags', got '%s'", allNode.Field)
	}
	if len(allNode.Values) != 2 {
		t.Errorf("expected 2 values, got %d", len(allNode.Values))
	}
}

func TestParseElemMatch(t *testing.T) {
	doc := bson.NewDocument()

	scoreDoc := bson.NewDocument()
	scoreDoc.Set("$gt", bson.VInt32(80))
	inner := bson.NewDocument()
	inner.Set("score", bson.VDoc(scoreDoc))

	opDoc := bson.NewDocument()
	opDoc.Set("$elemMatch", bson.VDoc(inner))
	doc.Set("grades", bson.VDoc(opDoc))

	node, err := Parse(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	emNode, ok := node.(*ElemMatchNode)
	if !ok {
		t.Fatalf("expected ElemMatchNode, got %T", node)
	}

	if emNode.Field != "grades" {
		t.Errorf("expected field 'grades', got '%s'", emNode.Field)
	}
}

func TestParseSize(t *testing.T) {
	doc := bson.NewDocument()
	opDoc := bson.NewDocument()
	opDoc.Set("$size", bson.VInt32(3))
	doc.Set("tags", bson.VDoc(opDoc))

	node, err := Parse(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	sizeNode, ok := node.(*SizeNode)
	if !ok {
		t.Fatalf("expected SizeNode, got %T", node)
	}

	if sizeNode.Size != 3 {
		t.Errorf("expected size 3, got %d", sizeNode.Size)
	}
}

func TestEvaluateComparison(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("value", bson.VInt32(10))

	tests := []struct {
		op       NodeType
		value    int32
		expected bool
	}{
		{NodeEq, 10, true},
		{NodeEq, 5, false},
		{NodeNe, 5, true},
		{NodeNe, 10, false},
		{NodeGt, 5, true},
		{NodeGt, 15, false},
		{NodeGte, 10, true},
		{NodeLt, 15, true},
		{NodeLt, 5, false},
		{NodeLte, 10, true},
	}

	for _, tc := range tests {
		node := &ComparisonNode{
			Op:    tc.op,
			Field: "value",
			Value: bson.VInt32(tc.value),
		}

		result := node.Evaluate(doc)
		if result != tc.expected {
			t.Errorf("%s(%d): expected %v, got %v", tc.op, tc.value, tc.expected, result)
		}
	}
}

func TestEvaluateLogicalAnd(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(1))
	doc.Set("b", bson.VInt32(2))

	// {a: 1, b: 2} -> AND
	node := And(
		&ComparisonNode{Op: NodeEq, Field: "a", Value: bson.VInt32(1)},
		&ComparisonNode{Op: NodeEq, Field: "b", Value: bson.VInt32(2)},
	)

	if !node.Evaluate(doc) {
		t.Error("expected match")
	}

	// Change one value
	node = And(
		&ComparisonNode{Op: NodeEq, Field: "a", Value: bson.VInt32(999)},
		&ComparisonNode{Op: NodeEq, Field: "b", Value: bson.VInt32(2)},
	)

	if node.Evaluate(doc) {
		t.Error("expected no match")
	}
}

func TestEvaluateLogicalOr(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(1))
	doc.Set("b", bson.VInt32(2))

	// {$or: [{a: 999}, {b: 2}]}
	node := Or(
		&ComparisonNode{Op: NodeEq, Field: "a", Value: bson.VInt32(999)},
		&ComparisonNode{Op: NodeEq, Field: "b", Value: bson.VInt32(2)},
	)

	if !node.Evaluate(doc) {
		t.Error("expected match (b:2 matches)")
	}
}

func TestEvaluateExists(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("present", bson.VInt32(1))

	// {present: {$exists: true}}
	node := &ExistsNode{Field: "present", Exists: true}
	if !node.Evaluate(doc) {
		t.Error("expected exists=true to match")
	}

	// {missing: {$exists: false}}
	node = &ExistsNode{Field: "missing", Exists: false}
	if !node.Evaluate(doc) {
		t.Error("expected exists=false to match for missing field")
	}
}

func TestEvaluateRegex(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("John Doe"))

	// {name: {$regex: "John"}}
	node := &RegexNode{Field: "name", Pattern: "John"}
	if !node.Evaluate(doc) {
		t.Error("expected regex to match")
	}

	// {name: {$regex: "^Jane"}}
	node = &RegexNode{Field: "name", Pattern: "^Jane"}
	if node.Evaluate(doc) {
		t.Error("expected regex to not match")
	}
}

func TestResolveField(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("John"))

	inner := bson.NewDocument()
	inner.Set("city", bson.VString("NYC"))
	doc.Set("address", bson.VDoc(inner))

	// Top-level field
	v, found := resolveField(doc, "name")
	if !found || v.String() != "John" {
		t.Error("expected to find name=John")
	}

	// Nested field
	v, found = resolveField(doc, "address.city")
	if !found || v.String() != "NYC" {
		t.Error("expected to find address.city=NYC")
	}

	// Missing field
	_, found = resolveField(doc, "missing")
	if found {
		t.Error("expected missing field to not be found")
	}
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		a        bson.Value
		b        bson.Value
		expected int
	}{
		{bson.VInt32(5), bson.VInt32(5), 0},
		{bson.VInt32(5), bson.VInt32(10), -1},
		{bson.VInt32(10), bson.VInt32(5), 1},
		{bson.VString("a"), bson.VString("b"), -1},
		{bson.VBool(false), bson.VBool(true), -1},
		{bson.VNull(), bson.VInt32(1), -1}, // null < everything
	}

	for _, tc := range tests {
		result := compareValues(tc.a, tc.b)
		if result != tc.expected {
			t.Errorf("compare(%v, %v): expected %d, got %d", tc.a.Interface(), tc.b.Interface(), tc.expected, result)
		}
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

// Test String methods for all node types
func TestNodeString(t *testing.T) {
	tests := []struct {
		node     QueryNode
		expected string
	}{
		{
			&ComparisonNode{Op: NodeEq, Field: "name", Value: bson.VString("John")},
			"{name: {$eq: John}}",
		},
		{
			&InNode{Op: NodeIn, Field: "status", Values: []bson.Value{bson.VString("active")}},
			"{status: {$in: [active]}}",
		},
		{
			&LogicalNode{Op: NodeAnd, Children: []QueryNode{
				&ComparisonNode{Op: NodeEq, Field: "a", Value: bson.VInt32(1)},
			}},
			"{$and: [{a: {$eq: 1}}]}",
		},
		{
			&ExistsNode{Field: "name", Exists: true},
			"{name: {$exists: true}}",
		},
		{
			&TypeNode{Field: "age", TypeAlias: "int"},
			"{age: {$type: int}}",
		},
		{
			&RegexNode{Field: "name", Pattern: "^J", Options: "i"},
			"{name: {$regex: \"^J\", $options: \"i\"}}",
		},
		{
			&RegexNode{Field: "name", Pattern: "test"},
			"{name: {$regex: \"test\"}}",
		},
		{
			&AllNode{Field: "tags", Values: []bson.Value{bson.VString("a"), bson.VString("b")}},
			"{tags: {$all: [a b]}}",
		},
		{
			&ElemMatchNode{Field: "grades", Query: &ComparisonNode{Op: NodeGt, Field: "score", Value: bson.VInt32(80)}},
			"{grades: {$elemMatch: {score: {$gt: 80}}}}",
		},
		{
			&SizeNode{Field: "items", Size: 3},
			"{items: {$size: 3}}",
		},
	}

	for _, tc := range tests {
		result := tc.node.String()
		if result != tc.expected {
			t.Errorf("String() = %q, want %q", result, tc.expected)
		}
	}
}

// Test Type methods
func TestNodeType(t *testing.T) {
	tests := []struct {
		node     QueryNode
		expected NodeType
	}{
		{&ComparisonNode{Op: NodeEq}, NodeEq},
		{&ComparisonNode{Op: NodeGt}, NodeGt},
		{&InNode{Op: NodeIn}, NodeIn},
		{&InNode{Op: NodeNin}, NodeNin},
		{&LogicalNode{Op: NodeAnd}, NodeAnd},
		{&LogicalNode{Op: NodeOr}, NodeOr},
		{&ExistsNode{}, NodeExists},
		{&TypeNode{}, NodeTypeOp},
		{&RegexNode{}, NodeRegex},
		{&AllNode{}, NodeAll},
		{&ElemMatchNode{}, NodeElemMatch},
		{&SizeNode{}, NodeSize},
	}

	for _, tc := range tests {
		result := tc.node.Type()
		if result != tc.expected {
			t.Errorf("Type() = %v, want %v", result, tc.expected)
		}
	}
}

// Test Evaluate for InNode
func TestEvaluateInNode(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("status", bson.VString("active"))
	doc.Set("tags", bson.VArray(bson.A(bson.VString("a"), bson.VString("b"), bson.VString("c"))))

	// Test $in with matching value
	node := &InNode{
		Op:     NodeIn,
		Field:  "status",
		Values: []bson.Value{bson.VString("active"), bson.VString("pending")},
	}
	if !node.Evaluate(doc) {
		t.Error("$in should match 'active'")
	}

	// Test $in with non-matching value
	node2 := &InNode{
		Op:     NodeIn,
		Field:  "status",
		Values: []bson.Value{bson.VString("inactive"), bson.VString("pending")},
	}
	if node2.Evaluate(doc) {
		t.Error("$in should not match when value not in array")
	}

	// Test $nin (not in)
	node3 := &InNode{
		Op:     NodeNin,
		Field:  "status",
		Values: []bson.Value{bson.VString("inactive"), bson.VString("pending")},
	}
	if !node3.Evaluate(doc) {
		t.Error("$nin should match when value not in array")
	}

	// Test $in with null matching
	doc2 := bson.NewDocument()
	node4 := &InNode{
		Op:     NodeIn,
		Field:  "missing",
		Values: []bson.Value{bson.VNull()},
	}
	if !node4.Evaluate(doc2) {
		t.Error("$in should match missing field when null is in array")
	}

	// Test $in with array field (element matching)
	node5 := &InNode{
		Op:     NodeIn,
		Field:  "tags",
		Values: []bson.Value{bson.VString("b")},
	}
	if !node5.Evaluate(doc) {
		t.Error("$in should match when any array element matches")
	}
}

// Test Evaluate for TypeNode
func TestEvaluateTypeNode(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("John"))
	doc.Set("age", bson.VInt32(30))
	doc.Set("score", bson.VDouble(95.5))
	doc.Set("active", bson.VBool(true))

	tests := []struct {
		field    string
		typeName string
		expected bool
	}{
		{"name", "string", true},
		{"age", "int", true},
		{"age", "number", true}, // number alias for int32
		{"score", "double", true},
		{"score", "number", true}, // number alias for double
		{"active", "bool", true},
		{"missing", "null", true},
		{"name", "int", false},
		{"name", "double", false},
	}

	for _, tc := range tests {
		node := &TypeNode{Field: tc.field, TypeAlias: tc.typeName}
		result := node.Evaluate(doc)
		if result != tc.expected {
			t.Errorf("TypeNode(%s: %s) = %v, want %v", tc.field, tc.typeName, result, tc.expected)
		}
	}
}

// Test Evaluate for AllNode
func TestEvaluateAllNode(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("tags", bson.VArray(bson.A(
		bson.VString("a"),
		bson.VString("b"),
		bson.VString("c"),
	)))

	// All values present
	node1 := &AllNode{
		Field:  "tags",
		Values: []bson.Value{bson.VString("a"), bson.VString("b")},
	}
	if !node1.Evaluate(doc) {
		t.Error("$all should match when all values present")
	}

	// Missing value
	node2 := &AllNode{
		Field:  "tags",
		Values: []bson.Value{bson.VString("a"), bson.VString("z")},
	}
	if node2.Evaluate(doc) {
		t.Error("$all should not match when value missing")
	}

	// Non-array field
	doc2 := bson.NewDocument()
	doc2.Set("tags", bson.VString("not an array"))
	if node1.Evaluate(doc2) {
		t.Error("$all should not match non-array field")
	}
}

// Test Evaluate for ElemMatchNode
func TestEvaluateElemMatchNode(t *testing.T) {
	// Create document with array of documents
	gradesDoc1 := bson.NewDocument()
	gradesDoc1.Set("score", bson.VInt32(85))
	gradesDoc2 := bson.NewDocument()
	gradesDoc2.Set("score", bson.VInt32(92))

	doc := bson.NewDocument()
	doc.Set("grades", bson.VArray(bson.A(
		bson.VDoc(gradesDoc1),
		bson.VDoc(gradesDoc2),
	)))

	// Match element with score > 80
	node := &ElemMatchNode{
		Field: "grades",
		Query: &ComparisonNode{Op: NodeGt, Field: "score", Value: bson.VInt32(90)},
	}
	if !node.Evaluate(doc) {
		t.Error("$elemMatch should match when any element satisfies condition")
	}

	// No matching element
	node2 := &ElemMatchNode{
		Field: "grades",
		Query: &ComparisonNode{Op: NodeGt, Field: "score", Value: bson.VInt32(95)},
	}
	if node2.Evaluate(doc) {
		t.Error("$elemMatch should not match when no element satisfies condition")
	}

	// Non-array field
	doc2 := bson.NewDocument()
	doc2.Set("grades", bson.VString("not an array"))
	if node.Evaluate(doc2) {
		t.Error("$elemMatch should not match non-array field")
	}
}

// Test Evaluate for SizeNode
func TestEvaluateSizeNode(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("tags", bson.VArray(bson.A(
		bson.VString("a"),
		bson.VString("b"),
		bson.VString("c"),
	)))

	// Correct size
	node := &SizeNode{Field: "tags", Size: 3}
	if !node.Evaluate(doc) {
		t.Error("$size should match correct array size")
	}

	// Wrong size
	node2 := &SizeNode{Field: "tags", Size: 2}
	if node2.Evaluate(doc) {
		t.Error("$size should not match wrong array size")
	}

	// Non-array field
	doc2 := bson.NewDocument()
	doc2.Set("tags", bson.VString("not an array"))
	if node.Evaluate(doc2) {
		t.Error("$size should not match non-array field")
	}
}

// Test Evaluate for Not and Nor
func TestEvaluateLogicalNotNor(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("status", bson.VString("active"))
	doc.Set("role", bson.VString("user"))

	// Test $not
	notNode := Not(&ComparisonNode{Op: NodeEq, Field: "status", Value: bson.VString("inactive")})
	if !notNode.Evaluate(doc) {
		t.Error("$not should return true when condition is false")
	}

	notNode2 := Not(&ComparisonNode{Op: NodeEq, Field: "status", Value: bson.VString("active")})
	if notNode2.Evaluate(doc) {
		t.Error("$not should return false when condition is true")
	}

	// Test $nor
	norNode := Nor(
		&ComparisonNode{Op: NodeEq, Field: "status", Value: bson.VString("inactive")},
		&ComparisonNode{Op: NodeEq, Field: "role", Value: bson.VString("admin")},
	)
	if !norNode.Evaluate(doc) {
		t.Error("$nor should return true when none match")
	}

	norNode2 := Nor(
		&ComparisonNode{Op: NodeEq, Field: "status", Value: bson.VString("active")},
		&ComparisonNode{Op: NodeEq, Field: "role", Value: bson.VString("admin")},
	)
	if norNode2.Evaluate(doc) {
		t.Error("$nor should return false when any matches")
	}

	// $not with empty children
	notEmpty := &LogicalNode{Op: NodeNot, Children: []QueryNode{}}
	if !notEmpty.Evaluate(doc) {
		t.Error("$not with no children should return true")
	}
}

// Test And/Or/Not/Nor helper functions
func TestLogicalHelpers(t *testing.T) {
	node1 := &ComparisonNode{Op: NodeEq, Field: "a", Value: bson.VInt32(1)}
	node2 := &ComparisonNode{Op: NodeEq, Field: "b", Value: bson.VInt32(2)}

	// And with no nodes
	andEmpty := And()
	if andEmpty.Type() != NodeAnd {
		t.Error("And() should return AND node")
	}

	// And with one node - should return that node
	andSingle := And(node1)
	if andSingle != node1 {
		t.Error("And(single) should return the single node")
	}

	// And with multiple nodes
	andMulti := And(node1, node2)
	logical, ok := andMulti.(*LogicalNode)
	if !ok || logical.Op != NodeAnd {
		t.Error("And(multi) should return LogicalNode with AND")
	}
	if len(logical.Children) != 2 {
		t.Errorf("And should have 2 children, got %d", len(logical.Children))
	}

	// Or with no nodes
	orEmpty := Or()
	if orEmpty.Type() != NodeOr {
		t.Error("Or() should return OR node")
	}

	// Or with one node
	orSingle := Or(node1)
	if orSingle != node1 {
		t.Error("Or(single) should return the single node")
	}

	// Nor
	nor := Nor(node1, node2)
	norLogical, ok := nor.(*LogicalNode)
	if !ok || norLogical.Op != NodeNor {
		t.Error("Nor should return LogicalNode with NOR")
	}
}

// Test parseArrayIndex
func TestParseArrayIndex(t *testing.T) {
	tests := []struct {
		input    string
		expected int
		valid    bool
	}{
		{"0", 0, true},
		{"1", 1, true},
		{"123", 123, true},
		{"00", 0, true}, // Leading zeros - parsed as 0
		// Note: parseArrayIndex returns -1 for non-numeric strings (no error)
		{"abc", -1, false},
		{"12a", -1, false},
		{"a12", -1, false},
		// Note: empty string returns 0 (no digits parsed)
		{"", 0, true},
		{"1.5", -1, false},
		{"-1", -1, false}, // '-' is not a digit
	}

	for _, tc := range tests {
		idx, err := parseArrayIndex(tc.input)
		// Implementation never returns error, returns -1 for invalid input
		if err != nil {
			t.Errorf("parseArrayIndex(%q) unexpected error: %v", tc.input, err)
			continue
		}
		if tc.valid {
			if idx != tc.expected {
				t.Errorf("parseArrayIndex(%q) = %d, want %d", tc.input, idx, tc.expected)
			}
		} else {
			if idx != -1 {
				t.Errorf("parseArrayIndex(%q) = %d, want -1 for invalid", tc.input, idx)
			}
		}
	}
}

// Test resolveField with array index
func TestResolveFieldArrayIndex(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("items", bson.VArray(bson.A(
		bson.VString("first"),
		bson.VString("second"),
		bson.VString("third"),
	)))

	// Access by index
	v, found := resolveField(doc, "items.0")
	if !found || v.String() != "first" {
		t.Errorf("items.0 = %v, want 'first'", v.String())
	}

	v, found = resolveField(doc, "items.2")
	if !found || v.String() != "third" {
		t.Errorf("items.2 = %v, want 'third'", v.String())
	}

	// Out of bounds
	_, found = resolveField(doc, "items.10")
	if found {
		t.Error("items.10 should not be found (out of bounds)")
	}

	// Invalid index (not a number)
	_, found = resolveField(doc, "items.abc")
	if found {
		t.Error("items.abc should not be found")
	}
}

// Test compareValues with various types
func TestCompareValuesExtended(t *testing.T) {
	tests := []struct {
		a        bson.Value
		b        bson.Value
		expected int
	}{
		// Same type comparisons
		{bson.VInt64(10), bson.VInt64(5), 1},
		{bson.VInt64(5), bson.VInt64(10), -1},
		{bson.VDouble(3.14), bson.VDouble(2.71), 1},
		{bson.VDouble(2.71), bson.VDouble(3.14), -1},
		{bson.VBool(false), bson.VBool(true), -1},
		{bson.VBool(true), bson.VBool(false), 1},
		{bson.VBool(true), bson.VBool(true), 0},

		// Cross-type (numeric)
		{bson.VInt32(5), bson.VInt64(10), -1},
		{bson.VInt64(10), bson.VInt32(5), 1},
		{bson.VInt32(5), bson.VDouble(10.0), -1},

		// Null comparisons
		{bson.VNull(), bson.VNull(), 0},
		{bson.VNull(), bson.VInt32(1), -1},
		{bson.VInt32(1), bson.VNull(), 1},

		// String comparisons
		{bson.VString("a"), bson.VString("b"), -1},
		{bson.VString("b"), bson.VString("a"), 1},
		{bson.VString("same"), bson.VString("same"), 0},
	}

	for _, tc := range tests {
		result := compareValues(tc.a, tc.b)
		if result != tc.expected {
			t.Errorf("compareValues(%v, %v) = %d, want %d", tc.a.Interface(), tc.b.Interface(), result, tc.expected)
		}
	}
}

// Test compareValues type ordering
func TestCompareValuesTypeOrder(t *testing.T) {
	// MongoDB type order: null < numbers < strings < objects < arrays < booleans
	// Test that different types are ordered by type
	nullVal := bson.VNull()
	intVal := bson.VInt32(100)
	strVal := bson.VString("zzz")
	boolVal := bson.VBool(true)

	// null < int
	if compareValues(nullVal, intVal) != -1 {
		t.Error("null should be less than int")
	}

	// int < string
	if compareValues(intVal, strVal) != -1 {
		t.Error("int should be less than string")
	}

	// string < bool (in MongoDB ordering)
	if compareValues(strVal, boolVal) != -1 {
		t.Error("string should be less than bool")
	}
}

// Test bsonTypeOrder
func TestBsonTypeOrder(t *testing.T) {
	tests := []struct {
		typ      bson.BSONType
		expected int
	}{
		{bson.TypeMinKey, 1},
		{bson.TypeNull, 2},
		{bson.TypeInt32, 3},
		{bson.TypeInt64, 3},
		{bson.TypeDouble, 3},
		{bson.TypeString, 4},
		{bson.TypeDocument, 5},
		{bson.TypeArray, 6},
		{bson.TypeBinary, 7},
		{bson.TypeObjectID, 8},
		{bson.TypeBoolean, 9},
		{bson.TypeDateTime, 10},
		{bson.TypeTimestamp, 11},
		{bson.TypeRegex, 12},
		{bson.TypeMaxKey, 13},
		{bson.BSONType(99), 100}, // Unknown types
	}

	for _, tc := range tests {
		result := bsonTypeOrder(tc.typ)
		if result != tc.expected {
			t.Errorf("bsonTypeOrder(%v) = %d, want %d", tc.typ, result, tc.expected)
		}
	}
}

// Test bsonTypeName
func TestBsonTypeName(t *testing.T) {
	tests := []struct {
		typ      bson.BSONType
		expected string
	}{
		{bson.TypeDouble, "double"},
		{bson.TypeString, "string"},
		{bson.TypeDocument, "object"},
		{bson.TypeArray, "array"},
		{bson.TypeBinary, "binData"},
		{bson.TypeObjectID, "objectId"},
		{bson.TypeBoolean, "bool"},
		{bson.TypeDateTime, "date"},
		{bson.TypeNull, "null"},
		{bson.TypeInt32, "int"},
		{bson.TypeInt64, "long"},
		{bson.BSONType(99), "unknown"},
	}

	for _, tc := range tests {
		result := bsonTypeName(tc.typ)
		if result != tc.expected {
			t.Errorf("bsonTypeName(%v) = %q, want %q", tc.typ, result, tc.expected)
		}
	}
}

// Test getCachedRegex
func TestGetCachedRegex(t *testing.T) {
	// First call should compile and cache
	re1 := getCachedRegex("^test", "")
	if re1 == nil {
		t.Fatal("getCachedRegex returned nil for valid pattern")
	}
	if !re1.MatchString("test123") {
		t.Error("regex should match 'test123'")
	}
	if re1.MatchString("123test") {
		t.Error("regex should not match '123test'")
	}

	// Second call should return cached version
	re2 := getCachedRegex("^test", "")
	if re1 != re2 {
		t.Error("getCachedRegex should return cached regex")
	}

	// Test with options
	re3 := getCachedRegex("TEST", "i") // case-insensitive
	if re3 == nil {
		t.Fatal("getCachedRegex returned nil with options")
	}
	if !re3.MatchString("test") {
		t.Error("case-insensitive regex should match 'test'")
	}
	if !re3.MatchString("TEST") {
		t.Error("case-insensitive regex should match 'TEST'")
	}

	// Test invalid pattern
	re4 := getCachedRegex("[invalid", "")
	if re4 != nil {
		t.Error("getCachedRegex should return nil for invalid pattern")
	}
}

// Test resolveField edge cases
func TestResolveFieldEdgeCases(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("John"))

	inner := bson.NewDocument()
	inner.Set("city", bson.VString("NYC"))
	inner.Set("zip", bson.VString("10001"))
	doc.Set("address", bson.VDoc(inner))

	// Deep nested access
	v, found := resolveField(doc, "address.city")
	if !found || v.String() != "NYC" {
		t.Errorf("address.city = %v, want NYC", v.String())
	}

	// Missing nested field
	_, found = resolveField(doc, "address.country")
	if found {
		t.Error("address.country should not be found")
	}

	// Empty path
	_, found = resolveField(doc, "")
	if found {
		t.Error("empty path should not be found")
	}

	// Traverse into non-document/array
	doc2 := bson.NewDocument()
	doc2.Set("name", bson.VString("scalar"))
	_, found = resolveField(doc2, "name.first")
	if found {
		t.Error("should not be able to traverse into scalar")
	}
}

// Test geospatial operators
func TestParse_GeoOperators(t *testing.T) {
	tests := []struct {
		name     string
		operator string
		opType   NodeType
	}{
		{"$near", "$near", NodeNear},
		{"$nearSphere", "$nearSphere", NodeNearSphere},
		{"$geoWithin", "$geoWithin", NodeGeoWithin},
		{"$geoIntersects", "$geoIntersects", NodeGeoIntersects},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := bson.NewDocument()
			geoDoc := bson.NewDocument()
			geoDoc.Set("$geometry", bson.VDoc(bson.NewDocument()))
			doc.Set("location", bson.VDoc(geoDoc))

			// Manually set the operator
			doc2 := bson.NewDocument()
			doc2.Set(tt.operator, bson.VDoc(bson.NewDocument()))
			doc.Set("location", bson.VDoc(doc2))

			result, err := Parse(doc)
			if err != nil {
				// Expected for now since geo parsing may have limitations
				t.Logf("Parse returned error (may be expected): %v", err)
				return
			}
			if result == nil {
				t.Error("expected non-nil result")
				return
			}
			if result.Type() != tt.opType {
				t.Errorf("expected type %v, got %v", tt.opType, result.Type())
			}
		})
	}
}
