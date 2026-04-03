package mammoth

import (
	"context"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/repl"
)

func TestChangeStream_BasicCreation(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, err := db.Collection("test_watch")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create change stream without replicator (mock mode)
	cs, err := coll.Watch(ctx, nil)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer cs.Close()

	if cs == nil {
		t.Fatal("expected non-nil change stream")
	}

	// TryNext should return false immediately in mock mode
	if cs.TryNext() {
		t.Error("expected TryNext to return false in mock mode")
	}
}

func TestChangeStream_DatabaseWatch(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create database-level change stream
	cs, err := db.Watch(ctx, nil)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer cs.Close()

	if cs == nil {
		t.Fatal("expected non-nil change stream")
	}

	// Verify namespace is just the database name
	if cs.ns != "default" {
		t.Errorf("expected ns='default', got '%s'", cs.ns)
	}
}

func TestChangeStream_WithPipeline(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("test_pipeline")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create change stream with match pipeline
	pipeline := []PipelineStage{
		Match(map[string]interface{}{
			"operationType": "insert",
		}),
	}

	cs, err := coll.Watch(ctx, pipeline)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer cs.Close()

	if len(cs.pipeline) != 1 {
		t.Errorf("expected 1 pipeline stage, got %d", len(cs.pipeline))
	}
}

func TestChangeStream_ResumeToken(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("test_resume")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cs, err := coll.Watch(ctx, nil)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer cs.Close()

	// Initial resume token should be empty
	token := cs.ResumeToken()
	if token.Data != "" {
		t.Errorf("expected empty initial token, got '%s'", token.Data)
	}
}

func TestChangeStream_ResumeAfter(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("test_resume_after")

	// Create a resume token
	resumeToken := ResumeToken{Data: "test_token_123"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := ChangeStreamOptions{
		ResumeAfter: &resumeToken,
	}

	cs, err := coll.Watch(ctx, nil, opts)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer cs.Close()

	// Resume token should be set
	if cs.resumeToken != "test_token_123" {
		t.Errorf("expected resume token 'test_token_123', got '%s'", cs.resumeToken)
	}
}

func TestChangeStream_StartAtOperationTime(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("test_start_time")

	startTime := time.Now().Add(-1 * time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := ChangeStreamOptions{
		StartAtOperationTime: &startTime,
	}

	cs, err := coll.Watch(ctx, nil, opts)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer cs.Close()

	if cs.opts.StartAtOperationTime == nil {
		t.Error("expected StartAtOperationTime to be set")
	} else if !cs.opts.StartAtOperationTime.Equal(startTime) {
		t.Error("StartAtOperationTime mismatch")
	}
}

func TestChangeStream_Close(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("test_close")

	ctx := context.Background()
	cs, err := coll.Watch(ctx, nil)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Close should succeed
	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Double close should be safe
	if err := cs.Close(); err != nil {
		t.Fatalf("Double Close: %v", err)
	}

	// TryNext should return false after close
	if cs.TryNext() {
		t.Error("expected TryNext to return false after close")
	}
}

func TestChangeStream_Err(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("test_err")

	ctx := context.Background()
	cs, err := coll.Watch(ctx, nil)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer cs.Close()

	// Initially no error
	if cs.Err() != nil {
		t.Errorf("expected no error initially, got: %v", cs.Err())
	}
}

func TestChangeStream_OperationTypeFilter(t *testing.T) {
	// Test operation type filter creation
	filter := OperationType("insert")
	if op, ok := filter["operationType"]; !ok || op != "insert" {
		t.Error("OperationType filter creation failed")
	}

	// Test multiple operation types
	filter = OperationType("insert", "update", "delete")
	if op, ok := filter["operationType"].(map[string]interface{}); !ok {
		t.Error("OperationType with multiple types should create $in filter")
	} else {
		if in, ok := op["$in"].([]string); !ok || len(in) != 3 {
			t.Error("OperationType $in filter incorrect")
		}
	}
}

func TestChangeStream_FullDocumentOptions(t *testing.T) {
	tests := []struct {
		name     string
		opt      FullDocumentOption
		expected string
	}{
		{"default", FullDocumentDefault(), "default"},
		{"off", FullDocumentOff(), "off"},
		{"updateLookup", FullDocumentUpdateLookup(), "updateLookup"},
		{"whenAvailable", FullDocumentWhenAvailable(), "whenAvailable"},
		{"required", FullDocumentRequired(), "required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.opt) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.opt)
			}
		})
	}
}

func TestChangeStream_PipelineHelpers(t *testing.T) {
	// Test Match helper
	matchStage := Match(map[string]interface{}{"operationType": "insert"})
	if matchStage.Stage != "$match" {
		t.Errorf("expected stage '$match', got '%s'", matchStage.Stage)
	}

	// Test Project helper
	projectStage := Project(map[string]interface{}{"fullDocument": 1})
	if projectStage.Stage != "$project" {
		t.Errorf("expected stage '$project', got '%s'", projectStage.Stage)
	}
}

func TestChangeStream_NextTimeout(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("test_next_timeout")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	opts := ChangeStreamOptions{
		MaxAwaitTime: 100 * time.Millisecond,
	}

	cs, err := coll.Watch(ctx, nil, opts)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer cs.Close()

	// Next should timeout quickly
	start := time.Now()
	result := cs.Next()
	elapsed := time.Since(start)

	if result {
		t.Error("expected Next to return false (no events)")
	}

	// Should have waited approximately MaxAwaitTime
	if elapsed > 500*time.Millisecond {
		t.Errorf("Next took too long: %v", elapsed)
	}
}

func TestChangeStream_ConvertOplogEntry(t *testing.T) {
	cs := &ChangeStream{
		ns:   "test.collection",
		opts: ChangeStreamOptions{FullDocument: UpdateLookup},
	}

	// Test insert entry conversion
	insertEntry := &repl.OplogEntry{
		Operation:   repl.OpInsert,
		Namespace:   "test.collection",
		Timestamp:   time.Now(),
		Hash:        123,
		TxnNumber:   1,
		SessionID:   "session-1",
	}

	// Create a BSON document for the object
	doc := bson.NewDocument()
	doc.Set("_id", bson.VString("doc1"))
	doc.Set("name", bson.VString("test"))
	insertEntry.Object = doc

	event := cs.convertOplogEntry(insertEntry)

	if event == nil {
		t.Fatal("expected non-nil event for insert")
	}

	if event.OperationType != "insert" {
		t.Errorf("expected operationType='insert', got '%s'", event.OperationType)
	}

	if event.TxnNumber == nil || *event.TxnNumber != 1 {
		t.Error("expected txnNumber to be set")
	}

	if event.LSID == nil {
		t.Error("expected LSID to be set")
	}

	// Test noop entry (should return nil)
	noopEntry := &repl.OplogEntry{
		Operation: repl.OpNoop,
	}

	event = cs.convertOplogEntry(noopEntry)
	if event != nil {
		t.Error("expected nil event for noop")
	}
}

func TestChangeStream_MatchesPipeline(t *testing.T) {
	cs := &ChangeStream{
		pipeline: []PipelineStage{
			Match(map[string]interface{}{"operationType": "insert"}),
		},
	}

	// Event matching the filter
	insertEvent := &ChangeEvent{
		OperationType: "insert",
	}

	if !cs.matchesPipeline(insertEvent) {
		t.Error("expected insert event to match pipeline")
	}

	// Event not matching
	deleteEvent := &ChangeEvent{
		OperationType: "delete",
	}

	if cs.matchesPipeline(deleteEvent) {
		t.Error("expected delete event to not match pipeline")
	}
}

func TestResumeToken_EncodeDecode(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Nanosecond)
	hash := int64(12345)

	token := encodeResumeToken(ts, hash)
	if token == "" {
		t.Fatal("expected non-empty token")
	}

	decoded := decodeResumeToken(token)

	// Allow small time difference due to precision
	diff := decoded.Sub(ts)
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Millisecond {
		t.Errorf("decoded time mismatch: expected %v, got %v (diff: %v)", ts, decoded, diff)
	}
}

func TestParseNamespace(t *testing.T) {
	tests := []struct {
		input    string
		expected Namespace
	}{
		{"db.collection", Namespace{DB: "db", Coll: "collection"}},
		{"mydb.mycoll", Namespace{DB: "mydb", Coll: "mycoll"}},
		{"db", Namespace{DB: "db", Coll: ""}},
		{"", Namespace{DB: "", Coll: ""}},
	}

	for _, tt := range tests {
		result := parseNamespace(tt.input)
		if result.DB != tt.expected.DB || result.Coll != tt.expected.Coll {
			t.Errorf("parseNamespace(%q) = %+v, expected %+v", tt.input, result, tt.expected)
		}
	}
}

func TestChangeStream_IDMethod(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("test_id_method")

	ctx := context.Background()
	cs, err := coll.Watch(ctx, nil)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer cs.Close()

	// ID() should be alias for ResumeToken()
	token1 := cs.ResumeToken()
	token2 := cs.ID()

	if token1.Data != token2.Data {
		t.Error("ID() and ResumeToken() should return the same value")
	}
}

func TestChangeStream_MatchDocument(t *testing.T) {
	cs := &ChangeStream{}

	doc := map[string]interface{}{
		"name": "alice",
		"age":  30,
	}

	// Matching filter
	filter := map[string]interface{}{
		"name": "alice",
	}

	if !cs.matchDocument(doc, filter) {
		t.Error("expected document to match filter")
	}

	// Non-matching filter
	filter = map[string]interface{}{
		"name": "bob",
	}

	if cs.matchDocument(doc, filter) {
		t.Error("expected document to not match filter")
	}

	// Filter with non-existent field
	filter = map[string]interface{}{
		"nonexistent": "value",
	}

	if cs.matchDocument(doc, filter) {
		t.Error("expected document to not match filter with non-existent field")
	}
}

func TestChangeStream_Decode(t *testing.T) {
	// Create a change stream with a buffered event
	cs := &ChangeStream{
		buffer: []*ChangeEvent{
			{
				OperationType: "insert",
				NS:            Namespace{DB: "test", Coll: "coll"},
				DocumentKey:   map[string]interface{}{"_id": "123"},
				FullDocument:  map[string]interface{}{"_id": "123", "name": "test"},
			},
		},
		position: 0,
	}

	// Decode into ChangeEvent
	var event ChangeEvent
	if err := cs.Decode(&event); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if event.OperationType != "insert" {
		t.Errorf("expected operationType='insert', got '%s'", event.OperationType)
	}

	// Decode into map
	var m map[string]interface{}
	if err := cs.Decode(&m); err != nil {
		t.Fatalf("Decode to map: %v", err)
	}


	if m["operationType"] != "insert" {
		t.Errorf("expected operationType='insert' in map, got '%v'", m["operationType"])
	}
}
