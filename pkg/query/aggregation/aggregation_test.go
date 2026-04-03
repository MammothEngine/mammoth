package aggregation

import (
	"context"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestPipeline_NewPipeline(t *testing.T) {
	stages := []StageDefinition{
		{Name: "$match", Value: map[string]interface{}{"status": "active"}},
		{Name: "$limit", Value: int64(10)},
	}

	pipeline, err := NewPipeline(stages)
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}

	if pipeline.Count() != 2 {
		t.Errorf("expected 2 stages, got %d", pipeline.Count())
	}

	names := pipeline.StageNames()
	if names[0] != "$match" || names[1] != "$limit" {
		t.Errorf("unexpected stage names: %v", names)
	}
}

func TestPipeline_EmptyPipeline(t *testing.T) {
	_, err := NewPipeline(nil)
	if err == nil {
		t.Error("expected error for empty pipeline")
	}

	_, err = NewPipeline([]StageDefinition{})
	if err == nil {
		t.Error("expected error for empty pipeline slice")
	}
}

func TestPipeline_UnsupportedStage(t *testing.T) {
	stages := []StageDefinition{
		{Name: "$unsupported", Value: map[string]interface{}{}},
	}

	_, err := NewPipeline(stages)
	if err == nil {
		t.Error("expected error for unsupported stage")
	}
}

func TestMatchStage(t *testing.T) {
	// Create test documents
	docs := []*bson.Document{
		createDoc(map[string]interface{}{"name": "alice", "age": 30}),
		createDoc(map[string]interface{}{"name": "bob", "age": 25}),
		createDoc(map[string]interface{}{"name": "charlie", "age": 30}),
	}

	stage, err := newMatchStage(map[string]interface{}{"age": 30})
	if err != nil {
		t.Fatalf("newMatchStage: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Count results
	count := 0
	for {
		doc, err := output.Next()
		if err != nil || doc == nil {
			break
		}
		count++
		// Verify age is 30
		age, _ := doc.Get("age")
		if age.Int32() != 30 {
			t.Errorf("expected age 30, got %v", age.Int32())
		}
	}

	if count != 2 {
		t.Errorf("expected 2 matching docs, got %d", count)
	}
}

func TestProjectStage(t *testing.T) {
	docs := []*bson.Document{
		createDoc(map[string]interface{}{"name": "alice", "age": 30, "city": "NYC"}),
	}

	// Test inclusive projection
	stage, err := newProjectStage(map[string]interface{}{"name": 1, "age": 1})
	if err != nil {
		t.Fatalf("newProjectStage: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	doc, _ := output.Next()
	if doc == nil {
		t.Fatal("expected output document")
	}

	// Should have name, age, and _id
	if _, ok := doc.Get("name"); !ok {
		t.Error("expected name field")
	}
	if _, ok := doc.Get("age"); !ok {
		t.Error("expected age field")
	}
	if _, ok := doc.Get("_id"); !ok {
		t.Error("expected _id field")
	}
	// Should NOT have city
	if _, ok := doc.Get("city"); ok {
		t.Error("should not have city field")
	}
}

func TestProjectStage_ExcludeID(t *testing.T) {
	docs := []*bson.Document{
		createDoc(map[string]interface{}{"name": "alice", "age": 30}),
	}

	stage, err := newProjectStage(map[string]interface{}{"name": 1, "age": 1, "_id": 0})
	if err != nil {
		t.Fatalf("newProjectStage: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	doc, _ := output.Next()
	if doc == nil {
		t.Fatal("expected output document")
	}

	if _, ok := doc.Get("_id"); ok {
		t.Error("_id should be excluded")
	}
}

func TestLimitStage(t *testing.T) {
	docs := make([]*bson.Document, 10)
	for i := 0; i < 10; i++ {
		docs[i] = createDoc(map[string]interface{}{"n": i})
	}

	stage, err := newLimitStage(int64(5))
	if err != nil {
		t.Fatalf("newLimitStage: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	count := 0
	for {
		doc, err := output.Next()
		if err != nil || doc == nil {
			break
		}
		count++
	}

	if count != 5 {
		t.Errorf("expected 5 documents, got %d", count)
	}
}

func TestSkipStage(t *testing.T) {
	docs := make([]*bson.Document, 10)
	for i := 0; i < 10; i++ {
		docs[i] = createDoc(map[string]interface{}{"n": i})
	}

	stage, err := newSkipStage(int64(3))
	if err != nil {
		t.Fatalf("newSkipStage: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// First document should have n=3
	doc, _ := output.Next()
	if doc == nil {
		t.Fatal("expected output document")
	}

	n, _ := doc.Get("n")
	if n.Int32() != 3 {
		t.Errorf("expected first doc n=3, got %v", n.Int32())
	}

	// Count remaining
	count := 1
	for {
		doc, err := output.Next()
		if err != nil || doc == nil {
			break
		}
		count++
	}

	if count != 7 {
		t.Errorf("expected 7 documents, got %d", count)
	}
}

func TestSortStage(t *testing.T) {
	docs := []*bson.Document{
		createDoc(map[string]interface{}{"name": "charlie", "age": 35}),
		createDoc(map[string]interface{}{"name": "alice", "age": 25}),
		createDoc(map[string]interface{}{"name": "bob", "age": 30}),
	}

	stage, err := newSortStage(map[string]interface{}{"age": 1})
	if err != nil {
		t.Fatalf("newSortStage: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	expected := []string{"alice", "bob", "charlie"}
	for i, exp := range expected {
		doc, _ := output.Next()
		if doc == nil {
			t.Fatalf("expected doc %d", i)
		}
		name, _ := doc.Get("name")
		if name.String() != exp {
			t.Errorf("position %d: expected %s, got %s", i, exp, name.String())
		}
	}
}

func TestSortStage_Descending(t *testing.T) {
	docs := []*bson.Document{
		createDoc(map[string]interface{}{"name": "alice", "age": 25}),
		createDoc(map[string]interface{}{"name": "bob", "age": 30}),
	}

	stage, err := newSortStage(map[string]interface{}{"age": -1})
	if err != nil {
		t.Fatalf("newSortStage: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	doc, _ := output.Next()
	name, _ := doc.Get("name")
	if name.String() != "bob" {
		t.Errorf("expected bob first (descending), got %s", name.String())
	}
}

func TestGroupStage_Sum(t *testing.T) {
	docs := []*bson.Document{
		createDoc(map[string]interface{}{"category": "A", "amount": 10}),
		createDoc(map[string]interface{}{"category": "A", "amount": 20}),
		createDoc(map[string]interface{}{"category": "B", "amount": 15}),
	}

	stage, err := newGroupStage(map[string]interface{}{
		"_id":         "$category",
		"totalAmount": map[string]interface{}{"$sum": "$amount"},
	})
	if err != nil {
		t.Fatalf("newGroupStage: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	results := make(map[string]float64)
	for {
		doc, _ := output.Next()
		if doc == nil {
			break
		}
		id, _ := doc.Get("_id")
		total, _ := doc.Get("totalAmount")
		results[id.String()] = total.Double()
	}

	if results["A"] != 30 {
		t.Errorf("expected total A=30, got %v", results["A"])
	}
	if results["B"] != 15 {
		t.Errorf("expected total B=15, got %v", results["B"])
	}
}

func TestGroupStage_Avg(t *testing.T) {
	docs := []*bson.Document{
		createDoc(map[string]interface{}{"category": "A", "amount": 10}),
		createDoc(map[string]interface{}{"category": "A", "amount": 20}),
		createDoc(map[string]interface{}{"category": "A", "amount": 30}),
	}

	stage, err := newGroupStage(map[string]interface{}{
		"_id":       "$category",
		"avgAmount": map[string]interface{}{"$avg": "$amount"},
	})
	if err != nil {
		t.Fatalf("newGroupStage: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	doc, _ := output.Next()
	if doc == nil {
		t.Fatal("expected output")
	}

	avg, _ := doc.Get("avgAmount")
	if avg.Double() != 20 {
		t.Errorf("expected avg=20, got %v", avg.Double())
	}
}

func TestGroupStage_Count(t *testing.T) {
	docs := []*bson.Document{
		createDoc(map[string]interface{}{"status": "active"}),
		createDoc(map[string]interface{}{"status": "active"}),
		createDoc(map[string]interface{}{"status": "inactive"}),
	}

	stage, err := newGroupStage(map[string]interface{}{
		"_id":  "$status",
		"count": map[string]interface{}{"$sum": 1},
	})
	if err != nil {
		t.Fatalf("newGroupStage: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	results := make(map[string]float64)
	for {
		doc, _ := output.Next()
		if doc == nil {
			break
		}
		id, _ := doc.Get("_id")
		count, _ := doc.Get("count")
		results[id.String()] = count.Double()
	}

	if results["active"] != 2 {
		t.Errorf("expected count active=2, got %v", results["active"])
	}
	if results["inactive"] != 1 {
		t.Errorf("expected count inactive=1, got %v", results["inactive"])
	}
}

func TestUnwindStage(t *testing.T) {
	docs := []*bson.Document{
		createDoc(map[string]interface{}{
			"name":   "alice",
			"tags":   []interface{}{"a", "b", "c"},
		}),
	}

	stage, err := newUnwindStage("$tags")
	if err != nil {
		t.Fatalf("newUnwindStage: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Should produce 3 documents
	expectedTags := []string{"a", "b", "c"}
	for i, exp := range expectedTags {
		doc, _ := output.Next()
		if doc == nil {
			t.Fatalf("expected doc %d", i)
		}
		tag, _ := doc.Get("tags")
		if tag.String() != exp {
			t.Errorf("doc %d: expected tag %s, got %s", i, exp, tag.String())
		}
	}
}

func TestUnwindStage_NotArray(t *testing.T) {
	docs := []*bson.Document{
		createDoc(map[string]interface{}{
			"name": "alice",
			"tag":  "single",
		}),
	}

	stage, err := newUnwindStage("$tag")
	if err != nil {
		t.Fatalf("newUnwindStage: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := stage.Process(ctx, input)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Should include document as-is
	doc, _ := output.Next()
	if doc == nil {
		t.Fatal("expected output document")
	}

	name, _ := doc.Get("name")
	if name.String() != "alice" {
		t.Errorf("expected name=alice, got %s", name.String())
	}
}

func TestPipeline_Chaining(t *testing.T) {
	docs := []*bson.Document{
		createDoc(map[string]interface{}{"status": "active", "score": 10}),
		createDoc(map[string]interface{}{"status": "inactive", "score": 20}),
		createDoc(map[string]interface{}{"status": "active", "score": 30}),
		createDoc(map[string]interface{}{"status": "active", "score": 40}),
	}

	stages := []StageDefinition{
		{Name: "$match", Value: map[string]interface{}{"status": "active"}},
		{Name: "$sort", Value: map[string]interface{}{"score": 1}},
		{Name: "$limit", Value: int64(2)},
	}

	pipeline, err := NewPipeline(stages)
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}

	ctx := context.Background()
	input := newSliceIterator(docs)
	output, err := pipeline.Execute(ctx, input)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Should get 2 active docs with lowest scores
	doc1, _ := output.Next()
	score1, _ := doc1.Get("score")
	if score1.Int32() != 10 {
		t.Errorf("first doc score should be 10, got %v", score1.Int32())
	}

	doc2, _ := output.Next()
	score2, _ := doc2.Get("score")
	if score2.Int32() != 30 {
		t.Errorf("second doc score should be 30, got %v", score2.Int32())
	}

	// Should be no more
	doc3, _ := output.Next()
	if doc3 != nil {
		t.Error("expected no third document")
	}
}

func TestExpression_Evaluate(t *testing.T) {
	doc := createDoc(map[string]interface{}{
		"price":    100,
		"quantity": 3,
	})

	// Test field reference
	result := evaluateExpression("$price", doc)
	if result.Int32() != 100 {
		t.Errorf("expected price=100, got %v", result.Int32())
	}

	// Test literal
	result = evaluateExpression("literal", doc)
	if result.String() != "literal" {
		t.Errorf("expected literal string, got %v", result.String())
	}
}

func TestExpression_Add(t *testing.T) {
	doc := createDoc(map[string]interface{}{
		"a": 10,
		"b": 20,
	})

	expr := map[string]interface{}{
		"$add": []interface{}{"$a", "$b", 5},
	}

	result := evaluateExpression(expr, doc)
	if result.Double() != 35 {
		t.Errorf("expected 35, got %v", result.Double())
	}
}

func TestExpression_Multiply(t *testing.T) {
	doc := createDoc(map[string]interface{}{
		"price":    10,
		"quantity": 3,
	})

	expr := map[string]interface{}{
		"$multiply": []interface{}{"$price", "$quantity"},
	}

	result := evaluateExpression(expr, doc)
	if result.Double() != 30 {
		t.Errorf("expected 30, got %v", result.Double())
	}
}

// Helper function to create a BSON document from a map
func createDoc(m map[string]interface{}) *bson.Document {
	doc := bson.NewDocument()
	// Always add _id if not present
	if _, ok := m["_id"]; !ok {
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	}
	for k, v := range m {
		switch val := v.(type) {
		case string:
			doc.Set(k, bson.VString(val))
		case int:
			doc.Set(k, bson.VInt32(int32(val)))
		case int32:
			doc.Set(k, bson.VInt32(val))
		case int64:
			doc.Set(k, bson.VInt64(val))
		case float64:
			doc.Set(k, bson.VDouble(val))
		case bool:
			doc.Set(k, bson.VBool(val))
		case []interface{}:
			arr := make(bson.Array, len(val))
			for i, elem := range val {
				switch e := elem.(type) {
				case string:
					arr[i] = bson.VString(e)
				case int:
					arr[i] = bson.VInt32(int32(e))
				default:
					arr[i] = bson.VNull()
				}
			}
			doc.Set(k, bson.VArray(arr))
		default:
			doc.Set(k, bson.VNull())
		}
	}
	return doc
}

// Test Stage Name methods
func TestStageNames(t *testing.T) {
	tests := []struct {
		name     string
		stage    Stage
		expected string
	}{
		{"match", must(newMatchStage(map[string]interface{}{})), "$match"},
		{"project", must(newProjectStage(map[string]interface{}{"_id": 1})), "$project"},
		{"limit", must(newLimitStage(int64(10))), "$limit"},
		{"skip", must(newSkipStage(int64(5))), "$skip"},
		{"sort", must(newSortStage(map[string]interface{}{"name": 1})), "$sort"},
		{"group", must(newGroupStage(map[string]interface{}{"_id": "$category"})), "$group"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stage.Name(); got != tt.expected {
				t.Errorf("Name() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func must(s Stage, err error) Stage {
	if err != nil {
		panic(err)
	}
	return s
}

// Test toBSONValue with various types
func TestToBSONValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected bson.BSONType
	}{
		{"int", int(42), bson.TypeInt32},
		{"int32", int32(42), bson.TypeInt32},
		{"int64", int64(42), bson.TypeInt64},
		{"float64", float64(3.14), bson.TypeDouble},
		{"string", "hello", bson.TypeString},
		{"bool", true, bson.TypeBoolean},
		{"nil", nil, bson.TypeNull},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toBSONValue(tt.input)
			if result.Type != tt.expected {
				t.Errorf("toBSONValue(%v).Type = %v, want %v", tt.input, result.Type, tt.expected)
			}
		})
	}
}

// Test evaluateSubtract
func TestEvaluateSubtract(t *testing.T) {
	doc := createDoc(map[string]interface{}{"a": 10, "b": 3})

	expr := map[string]interface{}{
		"$subtract": []interface{}{"$a", "$b"},
	}

	result := evaluateExpression(expr, doc)
	if result.Double() != 7 {
		t.Errorf("expected 7, got %v", result.Double())
	}
}

// Test evaluateDivide
func TestEvaluateDivide(t *testing.T) {
	doc := createDoc(map[string]interface{}{"a": 30, "b": 5})

	expr := map[string]interface{}{
		"$divide": []interface{}{"$a", "$b"},
	}

	result := evaluateExpression(expr, doc)
	if result.Double() != 6 {
		t.Errorf("expected 6, got %v", result.Double())
	}
}

// Test compareValues
func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a, b     bson.Value
		expected int
	}{
		{"int32 less", bson.VInt32(5), bson.VInt32(10), -1},
		{"int32 equal", bson.VInt32(5), bson.VInt32(5), 0},
		{"int32 greater", bson.VInt32(10), bson.VInt32(5), 1},
		{"int64 less", bson.VInt64(5), bson.VInt64(10), -1},
		{"double less", bson.VDouble(3.14), bson.VDouble(6.28), -1},
		{"string equal", bson.VString("same"), bson.VString("same"), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareValues(tt.a, tt.b)
			if got != tt.expected {
				t.Errorf("compareValues(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.expected)
			}
		})
	}
}

// Test pipeline createStage for supported stages
func TestPipeline_CreateStage(t *testing.T) {

	tests := []struct {
		name    string
		stage   string
		value   interface{}
		wantErr bool
	}{
		{"match", "$match", map[string]interface{}{"status": "active"}, false},
		{"project", "$project", map[string]interface{}{"name": 1}, false},
		{"limit", "$limit", int64(10), false},
		{"skip", "$skip", int64(5), false},
		{"sort", "$sort", map[string]interface{}{"name": 1}, false},
		{"group", "$group", map[string]interface{}{"_id": "$field"}, false},
		{"unsupported", "$unsupported", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stage, err := createStage(StageDefinition{Name: tt.stage, Value: tt.value})
			if tt.wantErr {
				if err == nil {
					t.Error("expected error for unsupported stage")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if stage == nil {
				t.Error("expected stage, got nil")
			}
		})
	}
}

// Test Iterator Close methods
func TestIterator_Close(t *testing.T) {
	// Test that Close methods don't panic
	docs := []*bson.Document{
		createDoc(map[string]interface{}{"name": "alice"}),
	}

	// sliceIterator
	sliceIter := newSliceIterator(docs)
	if err := sliceIter.Close(); err != nil {
		t.Errorf("sliceIterator.Close() error: %v", err)
	}

	// TransformIterator
	matchStage, _ := newMatchStage(map[string]interface{}{})
	ctx := context.Background()
	transformIter, _ := matchStage.Process(ctx, sliceIter)
	if err := transformIter.Close(); err != nil {
		t.Errorf("TransformIterator.Close() error: %v", err)
	}

	// limitIterator
	limitIter := newLimitIterator(sliceIter, 5)
	if err := limitIter.Close(); err != nil {
		t.Errorf("limitIterator.Close() error: %v", err)
	}

	// skipIterator
	skipIter := newSkipIterator(sliceIter, 2)
	if err := skipIter.Close(); err != nil {
		t.Errorf("skipIterator.Close() error: %v", err)
	}
}
