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
