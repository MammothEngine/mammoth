package aggregation

import (
	"context"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestUnwindStage_Name(t *testing.T) {
	stage, err := newUnwindStage("$tags")
	if err != nil {
		t.Fatalf("newUnwindStage: %v", err)
	}

	if name := stage.Name(); name != "$unwind" {
		t.Errorf("expected Name() = '$unwind', got '%s'", name)
	}
}

func TestNewLookupStage(t *testing.T) {
	tests := []struct {
		name    string
		spec    interface{}
		wantErr bool
	}{
		{
			name: "valid lookup",
			spec: map[string]interface{}{
				"from":         "orders",
				"localField":   "customerId",
				"foreignField": "_id",
				"as":           "customerOrders",
			},
			wantErr: false,
		},
		{
			name:    "missing from",
			spec:    map[string]interface{}{"localField": "a", "foreignField": "b", "as": "c"},
			wantErr: true,
		},
		{
			name:    "missing localField",
			spec:    map[string]interface{}{"from": "a", "foreignField": "b", "as": "c"},
			wantErr: true,
		},
		{
			name:    "missing foreignField",
			spec:    map[string]interface{}{"from": "a", "localField": "b", "as": "c"},
			wantErr: true,
		},
		{
			name:    "missing as",
			spec:    map[string]interface{}{"from": "a", "localField": "b", "foreignField": "c"},
			wantErr: true,
		},
		{
			name:    "non-map value",
			spec:    "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stage, err := newLookupStage(tt.spec)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if stage == nil {
				t.Error("expected stage, got nil")
				return
			}
			if stage.Name() != "$lookup" {
				t.Errorf("expected Name() = '$lookup', got '%s'", stage.Name())
			}
		})
	}
}

func TestLookupStage_Process(t *testing.T) {
	stage, _ := newLookupStage(map[string]interface{}{
		"from":         "orders",
		"localField":   "customerId",
		"foreignField": "_id",
		"as":           "customerOrders",
	})

	docs := []*bson.Document{
		createDoc(map[string]interface{}{"_id": "1", "customerId": "cust1"}),
		createDoc(map[string]interface{}{"_id": "2", "customerId": "cust2"}),
	}

	ctx := context.Background()
	input := newSliceIterator(docs)

	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Lookup is currently a placeholder - returns input unchanged
	doc, _ := output.Next()
	if doc == nil {
		t.Error("expected document from output")
	}
}

func TestEmptyIterator(t *testing.T) {
	it := &emptyIterator{}

	// Next should always return nil
	doc, err := it.Next()
	if err != nil {
		t.Errorf("Next() error: %v", err)
	}
	if doc != nil {
		t.Error("expected nil document from empty iterator")
	}

	// Close should not error
	if err := it.Close(); err != nil {
		t.Errorf("Close() error: %v", err)
	}
}

func TestTransformIterator_Close(t *testing.T) {
	docs := []*bson.Document{
		createDoc(map[string]interface{}{"name": "alice"}),
	}

	source := newSliceIterator(docs)
	transformIter := &TransformIterator{
		source: source,
		transform: func(doc *bson.Document) (*bson.Document, error) {
			return doc, nil
		},
	}

	// Close should call source.Close()
	if err := transformIter.Close(); err != nil {
		t.Errorf("Close() error: %v", err)
	}
}

func TestTransformIterator_Next(t *testing.T) {
	docs := []*bson.Document{
		createDoc(map[string]interface{}{"name": "alice", "value": 10}),
		createDoc(map[string]interface{}{"name": "bob", "value": 20}),
	}

	source := newSliceIterator(docs)
	transformIter := &TransformIterator{
		source: source,
		transform: func(doc *bson.Document) (*bson.Document, error) {
			// Add a computed field
			newDoc := bson.NewDocument()
			for _, e := range doc.Elements() {
				newDoc.Set(e.Key, e.Value)
			}
			newDoc.Set("doubled", bson.VInt32(20))
			return newDoc, nil
		},
	}

	// First document
	doc, err := transformIter.Next()
	if err != nil {
		t.Fatalf("Next() error: %v", err)
	}
	if doc == nil {
		t.Fatal("expected document, got nil")
	}

	val, _ := doc.Get("doubled")
	if val.Int32() != 20 {
		t.Errorf("expected doubled=20, got %v", val.Int32())
	}

	// Second document
	doc, _ = transformIter.Next()
	if doc == nil {
		t.Error("expected second document")
	}

	// Exhausted
	doc, _ = transformIter.Next()
	if doc != nil {
		t.Error("expected nil after exhausted")
	}
}

func TestToMap(t *testing.T) {
	// Test with map[string]interface{}
	m := map[string]interface{}{"key": "value", "num": 42}
	result, err := toMap(m)
	if err != nil {
		t.Errorf("toMap(map) error: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("expected key='value', got %v", result["key"])
	}

	// Test with *bson.Document
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("test"))
	doc.Set("count", bson.VInt32(5))

	result, err = toMap(doc)
	if err != nil {
		t.Errorf("toMap(doc) error: %v", err)
	}
	if result["name"] != "test" {
		t.Errorf("expected name='test', got %v", result["name"])
	}

	// Test with invalid type
	_, err = toMap("invalid")
	if err == nil {
		t.Error("expected error for invalid type")
	}
}

func TestToDocument(t *testing.T) {
	// Test with *bson.Document
	orig := bson.NewDocument()
	orig.Set("key", bson.VString("value"))

	result, err := toDocument(orig)
	if err != nil {
		t.Errorf("toDocument(doc) error: %v", err)
	}
	if result != orig {
		t.Error("expected same document back")
	}

	// Test with map[string]interface{}
	m := map[string]interface{}{"name": "alice", "age": 30}
	result, err = toDocument(m)
	if err != nil {
		t.Errorf("toDocument(map) error: %v", err)
	}
	val, _ := result.Get("name")
	if val.String() != "alice" {
		t.Errorf("expected name='alice', got %v", val.String())
	}

	// Test with invalid type
	_, err = toDocument("invalid")
	if err == nil {
		t.Error("expected error for invalid type")
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected int64
		ok       bool
	}{
		{int(42), 42, true},
		{int32(42), 42, true},
		{int64(42), 42, true},
		{float64(42.9), 42, true},
		{"string", 0, false},
		{nil, 0, false},
	}

	for _, tt := range tests {
		result, ok := toInt64(tt.input)
		if ok != tt.ok {
			t.Errorf("toInt64(%v) ok=%v, want %v", tt.input, ok, tt.ok)
		}
		if ok && result != tt.expected {
			t.Errorf("toInt64(%v) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestToInterface(t *testing.T) {
	tests := []struct {
		input    bson.Value
		expected interface{}
	}{
		{bson.VNull(), nil},
		{bson.VBool(true), true},
		{bson.VInt32(42), int32(42)},
		{bson.VInt64(42), int64(42)},
		{bson.VDouble(3.14), 3.14},
		{bson.VString("hello"), "hello"},
	}

	for _, tt := range tests {
		result := toInterface(tt.input)
		if result != tt.expected {
			t.Errorf("toInterface(%v) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestStageNames_WithUnwindAndLookup(t *testing.T) {
	stages := []StageDefinition{
		{Name: "$match", Value: map[string]interface{}{"status": "active"}},
		{Name: "$unwind", Value: "$tags"},
		{Name: "$lookup", Value: map[string]interface{}{
			"from":         "orders",
			"localField":   "customerId",
			"foreignField": "_id",
			"as":           "orders",
		}},
	}

	pipeline, err := NewPipeline(stages)
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}

	names := pipeline.StageNames()
	expected := []string{"$match", "$unwind", "$lookup"}

	if len(names) != len(expected) {
		t.Errorf("expected %d stages, got %d", len(expected), len(names))
	}

	for i, exp := range expected {
		if i < len(names) && names[i] != exp {
			t.Errorf("stage %d: expected '%s', got '%s'", i, exp, names[i])
		}
	}
}

func TestUnwindStage_InvalidInput(t *testing.T) {
	// Non-string value
	_, err := newUnwindStage(123)
	if err == nil {
		t.Error("expected error for non-string unwind value")
	}

	// Array value
	_, err = newUnwindStage([]interface{}{"a", "b"})
	if err == nil {
		t.Error("expected error for array unwind value")
	}
}

// Test toFloat64 with bson.Value types
func TestToFloat64_BSONValues(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
		ok       bool
	}{
		{bson.VInt32(42), 42, true},
		{bson.VInt64(100), 100, true},
		{bson.VDouble(3.14), 3.14, true},
		{bson.VString("not a number"), 0, false},
		{bson.VNull(), 0, false},
		{bson.VBool(true), 0, false},
	}

	for _, tt := range tests {
		result := toFloat64(tt.input)
		if tt.ok && result != tt.expected {
			t.Errorf("toFloat64(%v) = %f, want %f", tt.input, result, tt.expected)
		}
	}
}

// Test toBSONValue with more types
func TestToBSONValue_MoreTypes(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected bson.BSONType
	}{
		{nil, bson.TypeNull},
		{true, bson.TypeBoolean},
		{int(42), bson.TypeInt32},
		{int32(42), bson.TypeInt32},
		{int64(100), bson.TypeInt64},
		{float64(3.14), bson.TypeDouble},
		{"hello", bson.TypeString},
		{bson.VInt32(5), bson.TypeInt32},
		{[]byte("test"), bson.TypeNull}, // unknown type returns null
	}

	for _, tt := range tests {
		result := toBSONValue(tt.input)
		if result.Type != tt.expected {
			t.Errorf("toBSONValue(%v).Type = %v, want %v", tt.input, result.Type, tt.expected)
		}
	}
}

// Test toInterface default case
func TestToInterface_DefaultCase(t *testing.T) {
	// Test with ObjectID which falls through to default case
	oid := bson.NewObjectID()
	v := bson.VObjectID(oid)
	result := toInterface(v)
	if result == nil {
		t.Error("toInterface for ObjectID should not return nil")
	}
}

// Test evaluateIDExpression with nil and $null
func TestEvaluateIDExpression_NilAndNull(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("field", bson.VString("value"))

	// nil input
	result := evaluateIDExpression(nil, doc)
	if result.Type != bson.TypeNull {
		t.Errorf("evaluateIDExpression(nil) = %v, want null", result.Type)
	}

	// $null string
	result = evaluateIDExpression("$null", doc)
	if result.Type != bson.TypeNull {
		t.Errorf("evaluateIDExpression('$null') = %v, want null", result.Type)
	}
}

// Test evaluateExpression with various types
func TestEvaluateExpression_Types(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("alice"))
	doc.Set("age", bson.VInt32(30))

	// String field reference
	result := evaluateExpression("$name", doc)
	if result.String() != "alice" {
		t.Errorf("evaluateExpression('$name') = %v, want alice", result.String())
	}

	// Missing field
	result = evaluateExpression("$missing", doc)
	if result.Type != bson.TypeNull {
		t.Errorf("evaluateExpression('$missing') = %v, want null", result.Type)
	}

	// Plain string
	result = evaluateExpression("hello", doc)
	if result.String() != "hello" {
		t.Errorf("evaluateExpression('hello') = %v, want hello", result.String())
	}

	// Int
	result = evaluateExpression(42, doc)
	if result.Int32() != 42 {
		t.Errorf("evaluateExpression(42) = %v, want 42", result.Int32())
	}

	// Int64
	result = evaluateExpression(int64(100), doc)
	if result.Int64() != 100 {
		t.Errorf("evaluateExpression(int64(100)) = %v, want 100", result.Int64())
	}

	// Float64
	result = evaluateExpression(3.14, doc)
	if result.Double() != 3.14 {
		t.Errorf("evaluateExpression(3.14) = %v, want 3.14", result.Double())
	}

	// Bool
	result = evaluateExpression(true, doc)
	if !result.Boolean() {
		t.Error("evaluateExpression(true) should return true")
	}
}

// Test evaluateExpression with operators
func TestEvaluateExpression_Operators(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(10))
	doc.Set("b", bson.VInt32(5))

	// $add
	result := evaluateExpression(map[string]interface{}{"$add": []interface{}{"$a", "$b", 5}}, doc)
	if result.Double() != 20 {
		t.Errorf("$add result = %v, want 20", result.Double())
	}

	// $multiply
	result = evaluateExpression(map[string]interface{}{"$multiply": []interface{}{"$a", "$b"}}, doc)
	if result.Double() != 50 {
		t.Errorf("$multiply result = %v, want 50", result.Double())
	}

	// $subtract
	result = evaluateExpression(map[string]interface{}{"$subtract": []interface{}{"$a", "$b"}}, doc)
	if result.Double() != 5 {
		t.Errorf("$subtract result = %v, want 5", result.Double())
	}

	// $divide
	result = evaluateExpression(map[string]interface{}{"$divide": []interface{}{"$a", "$b"}}, doc)
	if result.Double() != 2 {
		t.Errorf("$divide result = %v, want 2", result.Double())
	}

	// $divide by zero
	result = evaluateExpression(map[string]interface{}{"$divide": []interface{}{"$a", 0}}, doc)
	if result.Type != bson.TypeNull {
		t.Errorf("$divide by zero = %v, want null", result.Type)
	}

	// Unknown operator
	result = evaluateExpression(map[string]interface{}{"$unknown": []interface{}{"$a"}}, doc)
	if result.Type != bson.TypeNull {
		t.Errorf("$unknown operator = %v, want null", result.Type)
	}
}

// Test evaluateAdd with non-array
func TestEvaluateAdd_NonArray(t *testing.T) {
	doc := bson.NewDocument()
	result := evaluateAdd("not an array", doc)
	if result.Type != bson.TypeNull {
		t.Errorf("evaluateAdd(non-array) = %v, want null", result.Type)
	}
}

// Test evaluateMultiply with insufficient operands
func TestEvaluateMultiply_InsufficientOperands(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(10))

	// Less than 2 operands
	result := evaluateMultiply([]interface{}{"$a"}, doc)
	if result.Type != bson.TypeNull {
		t.Errorf("evaluateMultiply(1 operand) = %v, want null", result.Type)
	}

	// Non-array
	result = evaluateMultiply("not an array", doc)
	if result.Type != bson.TypeNull {
		t.Errorf("evaluateMultiply(non-array) = %v, want null", result.Type)
	}
}

// Test evaluateSubtract with wrong number of operands
func TestEvaluateSubtract_WrongOperands(t *testing.T) {
	doc := bson.NewDocument()

	// Non-array
	result := evaluateSubtract("not an array", doc)
	if result.Type != bson.TypeNull {
		t.Errorf("evaluateSubtract(non-array) = %v, want null", result.Type)
	}

	// Wrong number of operands
	result = evaluateSubtract([]interface{}{1, 2, 3}, doc)
	if result.Type != bson.TypeNull {
		t.Errorf("evaluateSubtract(3 operands) = %v, want null", result.Type)
	}
}

// Test evaluateDivide with wrong number of operands
func TestEvaluateDivide_WrongOperands(t *testing.T) {
	doc := bson.NewDocument()

	// Non-array
	result := evaluateDivide("not an array", doc)
	if result.Type != bson.TypeNull {
		t.Errorf("evaluateDivide(non-array) = %v, want null", result.Type)
	}

	// Wrong number of operands
	result = evaluateDivide([]interface{}{1, 2, 3}, doc)
	if result.Type != bson.TypeNull {
		t.Errorf("evaluateDivide(3 operands) = %v, want null", result.Type)
	}
}

