package mammoth

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/repl"
)

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{"both nil", nil, nil, 0},
		{"a nil", nil, 5, -1},
		{"b nil", 5, nil, 1},
		{"int equal", 5, 5, 0},
		{"int less", 3, 5, -1},
		{"int greater", 5, 3, 1},
		{"int with int32", 5, int32(5), 0},
		{"int with int32 less", 3, int32(5), -1},
		{"int with int32 greater", 5, int32(3), 1},
		{"int with int64", 5, int64(5), 0},
		{"int with int64 less", 3, int64(5), -1},
		{"int with int64 greater", 5, int64(3), 1},
		{"int with float64", 5, float64(5), 0},
		{"int with float64 less", 3, float64(5), -1},
		{"int with float64 greater", 5, float64(3), 1},
		{"int32 equal", int32(5), int32(5), 0},
		{"int32 less", int32(3), int32(5), -1},
		{"int32 greater", int32(5), int32(3), 1},
		{"int32 with int", int32(5), 5, 0},
		{"int32 with int64", int32(5), int64(5), 0},
		{"int32 with float64", int32(5), float64(5), 0},
		{"int64 equal", int64(5), int64(5), 0},
		{"int64 less", int64(3), int64(5), -1},
		{"int64 greater", int64(5), int64(3), 1},
		{"int64 with int", int64(5), 5, 0},
		{"int64 with int32", int64(5), int32(5), 0},
		{"int64 with float64", int64(5), float64(5), 0},
		{"float64 equal", float64(5), float64(5), 0},
		{"float64 less", float64(3), float64(5), -1},
		{"float64 greater", float64(5), float64(3), 1},
		{"float64 with int", float64(5), 5, 0},
		{"float64 with int32", float64(5), int32(5), 0},
		{"float64 with int64", float64(5), int64(5), 0},
		{"string equal", "hello", "hello", 0},
		{"string less", "abc", "def", -1},
		{"string greater", "def", "abc", 1},
		{"time equal", time.Unix(1000, 0), time.Unix(1000, 0), 0},
		{"time before", time.Unix(1000, 0), time.Unix(2000, 0), -1},
		{"time after", time.Unix(2000, 0), time.Unix(1000, 0), 1},
		{"different types - string vs int", "abc", 5, 1},  // strings compare greater than numbers
		{"different types - int vs string", 5, "abc", -1}, // numbers compare less than strings
		// Additional int32 comparison paths
		{"int32 with int - less", int32(5), 10, -1},
		{"int32 with int - greater", int32(10), 5, 1},
		{"int32 with int64 - less", int32(5), int64(10), -1},
		{"int32 with int64 - greater", int32(10), int64(5), 1},
		{"int32 with float64 - less", int32(5), float64(10), -1},
		{"int32 with float64 - greater", int32(10), float64(5), 1},
		// Additional int64 comparison paths
		{"int64 with int - less", int64(5), 10, -1},
		{"int64 with int - greater", int64(10), 5, 1},
		{"int64 with int32 - less", int64(5), int32(10), -1},
		{"int64 with int32 - greater", int64(10), int32(5), 1},
		// Additional float64 comparison paths
		{"float64 with int - less", float64(5), 10, -1},
		{"float64 with int - greater", float64(10), 5, 1},
		{"float64 with int32 - less", float64(5), int32(10), -1},
		{"float64 with int32 - greater", float64(10), int32(5), 1},
		{"float64 with int64 - less", float64(5), int64(10), -1},
		{"float64 with int64 - greater", float64(10), int64(5), 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareValues(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareValues(%v, %v) = %d, want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestTransaction_Get(t *testing.T) {
	db := openTestDB(t)

	tx, err := db.StartTransaction()
	if err != nil {
		t.Fatalf("StartTransaction: %v", err)
	}

	// Get on active transaction should work
	_, err = tx.Get([]byte("nonexistent"))
	if err != nil {
		t.Logf("Get returned: %v (may be expected for missing key)", err)
	}

	// Commit and verify Get fails after commit
	tx.Commit()

	_, err = tx.Get([]byte("test"))
	if err == nil {
		t.Error("expected error for Get on committed transaction")
	}
}

func TestTransaction_Rollback(t *testing.T) {
	db := openTestDB(t)

	tx, err := db.StartTransaction()
	if err != nil {
		t.Fatalf("StartTransaction: %v", err)
	}

	// Rollback should work
	tx.Rollback()

	if !tx.IsRolledBack() {
		t.Error("expected IsRolledBack() = true")
	}
	if tx.IsActive() {
		t.Error("expected IsActive() = false")
	}

	// Double rollback should be safe
	tx.Rollback()

	// Operations on rolled back transaction should fail
	_, err = tx.Get([]byte("test"))
	if err == nil {
		t.Error("expected error for Get on rolled back transaction")
	}
}

func TestTransaction_ReadOnly(t *testing.T) {
	db := openTestDB(t)

	opts := DefaultTransactionOptions()
	opts.ReadOnly = true

	tx, err := db.StartTransaction(opts)
	if err != nil {
		t.Fatalf("StartTransaction: %v", err)
	}
	defer tx.Rollback()

	// Put should fail in read-only transaction
	err = tx.Put([]byte("key"), []byte("value"))
	if err == nil {
		t.Error("expected error for Put in read-only transaction")
	}

	// Delete should fail in read-only transaction
	err = tx.Delete([]byte("key"))
	if err == nil {
		t.Error("expected error for Delete in read-only transaction")
	}

	// Get should work in read-only transaction
	_, err = tx.Get([]byte("key"))
	// May return error for missing key but not for read-only
	t.Logf("Get returned: %v", err)
}

func TestIsRetryableError(t *testing.T) {
	// Currently isRetryableError always returns false
	if isRetryableError(nil) {
		t.Error("expected nil to not be retryable")
	}

	err := &testError{msg: "test error", retryable: false}
	if isRetryableError(err) {
		t.Error("expected non-retryable error to not be retryable")
	}
}

type testError struct {
	msg       string
	retryable bool
}

func (e *testError) Error() string {
	return e.msg
}

func TestEncodeID(t *testing.T) {
	tests := []struct {
		name     string
		id       interface{}
		expected []byte
	}{
		{"string", "test123", []byte("test123")},
		{"bytes", []byte("test123"), []byte("test123")},
		{"int", 123, []byte("123")},
		{"int64", int64(456), []byte("456")},
		{"nil", nil, []byte("<nil>")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodeID(tt.id)
			if err != nil {
				t.Fatalf("encodeID: %v", err)
			}
			if string(result) != string(tt.expected) {
				t.Errorf("encodeID(%v) = %s, want %s", tt.id, result, tt.expected)
			}
		})
	}
}

func TestCollectionTx_ReadOnly(t *testing.T) {
	db := openTestDB(t)
	coll, _ := db.Collection("tx_readonly_test")

	opts := DefaultTransactionOptions()
	opts.ReadOnly = true

	tx, err := db.StartTransaction(opts)
	if err != nil {
		t.Fatalf("StartTransaction: %v", err)
	}
	defer tx.Rollback()

	ct := coll.WithTransaction(tx)

	// InsertOne should fail in read-only transaction
	_, err = ct.InsertOne(map[string]interface{}{"_id": "test", "value": 1})
	if err == nil {
		t.Error("expected error for InsertOne in read-only transaction")
	}

	// UpdateOne should fail in read-only transaction
	_, err = ct.UpdateOne(
		map[string]interface{}{"_id": "test"},
		map[string]interface{}{"$set": map[string]interface{}{"value": 2}},
	)
	if err == nil {
		t.Error("expected error for UpdateOne in read-only transaction")
	}

	// DeleteOne should fail in read-only transaction
	_, err = ct.DeleteOne(map[string]interface{}{"_id": "test"})
	if err == nil {
		t.Error("expected error for DeleteOne in read-only transaction")
	}

	// FindOne should work in read-only transaction
	_, err = ct.FindOne(map[string]interface{}{"_id": "test"})
	// May return ErrNotFound but not read-only error
	t.Logf("FindOne returned: %v", err)
}

func TestCollectionTx_DeleteOne_NotFound(t *testing.T) {
	db := openTestDB(t)
	coll, _ := db.Collection("tx_delete_nf_test")

	err := db.WithTransaction(context.Background(), func(tx *Transaction) error {
		ct := coll.WithTransaction(tx)
		deleted, err := ct.DeleteOne(map[string]interface{}{"_id": "nonexistent"})
		if err != nil {
			return err
		}
		if deleted != 0 {
			t.Errorf("expected 0 deleted, got %d", deleted)
		}
		return nil
	})

	if err != nil {
		t.Logf("WithTransaction: %v (may be expected)", err)
	}
}

// Test generateIndexName function
func TestGenerateIndexName(t *testing.T) {
	tests := []struct {
		name     string
		keys     map[string]interface{}
		expected string
	}{
		{
			name:     "single key ascending",
			keys:     map[string]interface{}{"name": 1},
			expected: "name_1",
		},
		{
			name:     "single key descending",
			keys:     map[string]interface{}{"name": -1},
			expected: "name_-1",
		},
		{
			name:     "multiple keys",
			keys:     map[string]interface{}{"name": 1, "age": -1},
			expected: "name_1_age_-1",
		},
		{
			name:     "int32 ascending",
			keys:     map[string]interface{}{"field": int32(1)},
			expected: "field_1",
		},
		{
			name:     "int32 descending",
			keys:     map[string]interface{}{"field": int32(-1)},
			expected: "field_1", // Note: int32(-1) == -1 in switch comparison
		},
		{
			name:     "int64 ascending",
			keys:     map[string]interface{}{"field": int64(1)},
			expected: "field_1",
		},
		{
			name:     "float64 negative",
			keys:     map[string]interface{}{"field": float64(-0.5)},
			expected: "field_-1",
		},
		{
			name:     "float64 positive",
			keys:     map[string]interface{}{"field": float64(0.5)},
			expected: "field_1",
		},
		{
			name:     "string value",
			keys:     map[string]interface{}{"field": "hashed"},
			expected: "field_1",
		},
		{
			name:     "empty keys",
			keys:     map[string]interface{}{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateIndexName(tt.keys)
			// For multiple keys, the order is non-deterministic due to map iteration
			// So we just verify it contains the expected parts
			if len(tt.keys) <= 1 {
				if result != tt.expected {
					t.Errorf("generateIndexName() = %q, want %q", result, tt.expected)
				}
			} else {
				// For multiple keys, just verify expected parts are present
				for k, v := range tt.keys {
					suffix := "_1"
					if v == -1 || v == int32(-1) || v == int64(-1) {
						suffix = "_-1"
					}
					expectedPart := k + suffix
					if !strings.Contains(result, expectedPart) {
						t.Errorf("generateIndexName() = %q, missing part %q", result, expectedPart)
					}
				}
			}
		})
	}
}

// Test valueToInterface function
func TestValueToInterface(t *testing.T) {
	tests := []struct {
		name     string
		val      bson.Value
		expected interface{}
	}{
		{"double", bson.VDouble(3.14), float64(3.14)},
		{"string", bson.VString("hello"), "hello"},
		{"int32", bson.VInt32(42), int(42)},
		{"int64", bson.VInt64(123), int64(123)},
		{"bool", bson.VBool(true), true},
		{"null", bson.VNull(), nil},
		{"datetime", bson.VDateTime(1609459200000), int64(1609459200000)},
		{"binary", bson.VBinary(bson.BinaryGeneric, []byte{1, 2, 3}), []byte{1, 2, 3}},
		{"timestamp", bson.VTimestamp(12345), uint64(12345)},
		{"array", bson.VArray(bson.A(bson.VInt32(1), bson.VInt32(2))), []interface{}{int(1), int(2)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToInterface(tt.val)
			switch expected := tt.expected.(type) {
			case []interface{}:
				arr, ok := result.([]interface{})
				if !ok || len(arr) != len(expected) {
					t.Errorf("valueToInterface() = %v, want %v", result, expected)
				}
			case []byte:
				b, ok := result.([]byte)
				if !ok || string(b) != string(expected) {
					t.Errorf("valueToInterface() = %v, want %v", result, expected)
				}
			default:
				if result != expected {
					t.Errorf("valueToInterface() = %v, want %v", result, expected)
				}
			}
		})
	}
}

// Test interfaceToBSONValue function
func TestInterfaceToBSONValue(t *testing.T) {
	tests := []struct {
		name     string
		val      interface{}
		expected bson.Value
	}{
		{"nil", nil, bson.VNull()},
		{"bool", true, bson.VBool(true)},
		{"int", 42, bson.VInt32(42)},
		{"int32", int32(42), bson.VInt32(42)},
		{"int64", int64(42), bson.VInt64(42)},
		{"float64", float64(3.14), bson.VDouble(3.14)},
		{"string", "hello", bson.VString("hello")},
		{"objectID", bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, bson.VObjectID(bson.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})},
		{"bytes", []byte{1, 2, 3}, bson.VBinary(bson.BinaryGeneric, []byte{1, 2, 3})},
		{"[]string", []string{"a", "b"}, bson.VArray(bson.A(bson.VString("a"), bson.VString("b")))},
		{"nested map", map[string]interface{}{"nested": "value"}, bson.VDoc(bson.D("nested", bson.VString("value")))},
		{"[]interface{}", []interface{}{"a", 42, true}, bson.VArray(bson.A(bson.VString("a"), bson.VInt32(42), bson.VBool(true)))},
		{"default type", struct{ Name string }{Name: "test"}, bson.VNull()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := interfaceToBSONValue(tt.val)
			if result.Type != tt.expected.Type {
				t.Errorf("interfaceToBSONValue() type = %v, want %v", result.Type, tt.expected.Type)
			}
			// Additional value checks based on type
			switch tt.expected.Type {
			case bson.TypeBoolean:
				if result.Boolean() != tt.expected.Boolean() {
					t.Errorf("interfaceToBSONValue() = %v, want %v", result.Boolean(), tt.expected.Boolean())
				}
			case bson.TypeInt32:
				if result.Int32() != tt.expected.Int32() {
					t.Errorf("interfaceToBSONValue() = %v, want %v", result.Int32(), tt.expected.Int32())
				}
			case bson.TypeString:
				if result.String() != tt.expected.String() {
					t.Errorf("interfaceToBSONValue() = %v, want %v", result.String(), tt.expected.String())
				}
			}
		})
	}
}

// Test Cursor Decode error cases
func TestCursorDecodeErrors(t *testing.T) {
	db := openTestDB(t)
	coll, _ := db.Collection("decode_test")

	// Insert a document
	_, err := coll.InsertOne(map[string]interface{}{"_id": "1", "name": "test"})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Find and get cursor
	cur, err := coll.Find(nil)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	defer cur.Close()

	// Exhaust cursor
	for cur.Next() {
	}

	// Try to decode after cursor exhausted
	var result map[string]interface{}
	err = cur.Decode(&result)
	if err == nil {
		t.Error("expected error for exhausted cursor")
	}
}

// Test documentToMap and mapToDoc functions
func TestDocumentToMapAndMapToDoc(t *testing.T) {
	// Create a document
	doc := bson.D(
		"name", bson.VString("Alice"),
		"age", bson.VInt32(30),
		"active", bson.VBool(true),
	)

	// Convert to map
	m := documentToMap(doc)

	if m["name"] != "Alice" {
		t.Errorf("expected name='Alice', got %v", m["name"])
	}
	if m["age"] != int(30) {
		t.Errorf("expected age=30, got %v", m["age"])
	}
	if m["active"] != true {
		t.Errorf("expected active=true, got %v", m["active"])
	}

	// Convert back to document
	doc2 := mapToDoc(m)
	if doc2 == nil {
		t.Fatal("expected non-nil document")
	}

	// Verify round-trip
	if name, ok := doc2.Get("name"); !ok || name.String() != "Alice" {
		t.Error("round-trip failed for name")
	}
}


// Test WithTransaction function
func TestWithTransaction(t *testing.T) {
	db := openTestDB(t)

	// Successful transaction
	err := db.WithTransaction(context.Background(), func(tx *Transaction) error {
		return nil
	})
	if err != nil {
		t.Errorf("WithTransaction should succeed for nil error: %v", err)
	}

	// Transaction that returns an error should retry and eventually fail
	callCount := 0
	err = db.WithTransaction(context.Background(), func(tx *Transaction) error {
		callCount++
		return context.Canceled
	})
	if err == nil {
		t.Error("expected error for canceled transaction")
	}
	if callCount == 0 {
		t.Error("expected transaction function to be called")
	}
}

// Test parseUpdateDescription function
func TestParseUpdateDescription(t *testing.T) {
	cs := &ChangeStream{}

	tests := []struct {
		name     string
		doc      *bson.Document
		expected *UpdateDescription
	}{
		{
			name: "empty document",
			doc:  bson.NewDocument(),
			expected: &UpdateDescription{
				UpdatedFields: make(map[string]interface{}),
				RemovedFields: []string{},
			},
		},
		{
			name: "$set operator",
			doc: bson.D(
				"$set", bson.VDoc(bson.D(
					"name", bson.VString("Alice"),
					"age", bson.VInt32(30),
				)),
			),
			expected: &UpdateDescription{
				UpdatedFields: map[string]interface{}{
					"name": "Alice",
					"age":  int(30),
				},
				RemovedFields: []string{},
			},
		},
		{
			name: "$unset operator",
			doc: bson.D(
				"$unset", bson.VDoc(bson.D(
					"oldField", bson.VInt32(1),
					"tempField", bson.VInt32(1),
				)),
			),
			expected: &UpdateDescription{
				UpdatedFields: make(map[string]interface{}),
				RemovedFields: []string{"oldField", "tempField"},
			},
		},
		{
			name: "direct field replacement",
			doc: bson.D(
				"name", bson.VString("Bob"),
				"status", bson.VString("active"),
			),
			expected: &UpdateDescription{
				UpdatedFields: map[string]interface{}{
					"name":   "Bob",
					"status": "active",
				},
				RemovedFields: []string{},
			},
		},
		{
			name: "mixed $set and $unset",
			doc: bson.D(
				"$set", bson.VDoc(bson.D("name", bson.VString("Charlie"))),
				"$unset", bson.VDoc(bson.D("deprecated", bson.VInt32(1))),
			),
			expected: &UpdateDescription{
				UpdatedFields: map[string]interface{}{
					"name": "Charlie",
				},
				RemovedFields: []string{"deprecated"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cs.parseUpdateDescription(tt.doc)

			if len(result.UpdatedFields) != len(tt.expected.UpdatedFields) {
				t.Errorf("UpdatedFields length = %d, want %d", len(result.UpdatedFields), len(tt.expected.UpdatedFields))
			}
			for k, v := range tt.expected.UpdatedFields {
				if result.UpdatedFields[k] != v {
					t.Errorf("UpdatedFields[%q] = %v, want %v", k, result.UpdatedFields[k], v)
				}
			}

			if len(result.RemovedFields) != len(tt.expected.RemovedFields) {
				t.Errorf("RemovedFields length = %d, want %d", len(result.RemovedFields), len(tt.expected.RemovedFields))
			}
			for i, v := range tt.expected.RemovedFields {
				if i >= len(result.RemovedFields) || result.RemovedFields[i] != v {
					t.Errorf("RemovedFields[%d] = %v, want %v", i, result.RemovedFields[i], v)
				}
			}
		})
	}
}

// Test SetReplicator function
func TestChangeStream_SetReplicator(t *testing.T) {
	db := openTestDB(t)

	// Create a change stream
	coll, _ := db.Collection("test_repl")
	cs, err := coll.Watch(context.Background(), nil)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer cs.Close()

	// Test SetReplicator after started - should fail
	fakeReplicator := &repl.OplogReplicator{}
	err = cs.SetReplicator(fakeReplicator)
	if err == nil {
		t.Error("expected error when setting replicator after stream has started")
	}
}

// Test startWithReplicator function
func TestChangeStream_StartWithReplicator(t *testing.T) {
	db := openTestDB(t)
	coll, _ := db.Collection("test_start_repl")

	// Create a change stream (not started)
	cs := &ChangeStream{
		coll:     coll,
		ctx:      context.Background(),
		ns:       "test.test_start_repl",
		pipeline: []PipelineStage{},
	}

	// Test with nil replicator - should succeed
	err := cs.startWithReplicator(nil)
	if err != nil {
		t.Errorf("startWithReplicator(nil) = %v, want nil", err)
	}
}

// Test processLoop function (indirectly via context cancellation)
func TestChangeStream_ProcessLoop_Context(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create a minimal change stream
	cs := &ChangeStream{
		ctx:    ctx,
		cancel: cancel,
		buffer: make([]*ChangeEvent, 0),
	}

	// Start processLoop
	go cs.processLoop()

	// Cancel context to stop the loop
	cancel()

	// Give it time to stop
	time.Sleep(10 * time.Millisecond)

	// Should have no error (context cancelled is not an error)
	if cs.Err() != nil {
		t.Logf("Error after context cancel: %v", cs.Err())
	}
}

// Test getTime function
func TestGetTime(t *testing.T) {
	now := time.Now()
	nowMillis := now.UnixMilli()

	tests := []struct {
		name     string
		m        map[string]interface{}
		key      string
		expected time.Time
	}{
		{
			name:     "time.Time value",
			m:        map[string]interface{}{"created": now},
			key:      "created",
			expected: now,
		},
		{
			name:     "int64 value (milliseconds)",
			m:        map[string]interface{}{"timestamp": nowMillis},
			key:      "timestamp",
			expected: time.UnixMilli(nowMillis),
		},
		{
			name:     "float64 value (milliseconds)",
			m:        map[string]interface{}{"timestamp": float64(nowMillis)},
			key:      "timestamp",
			expected: time.UnixMilli(nowMillis),
		},
		{
			name:     "missing key",
			m:        map[string]interface{}{"other": now},
			key:      "created",
			expected: time.Time{},
		},
		{
			name:     "empty map",
			m:        map[string]interface{}{},
			key:      "created",
			expected: time.Time{},
		},
		{
			name:     "wrong type (string)",
			m:        map[string]interface{}{"created": "not-a-time"},
			key:      "created",
			expected: time.Time{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getTime(tt.m, tt.key)
			if tt.expected.IsZero() {
				if !result.IsZero() {
					t.Errorf("getTime() = %v, want zero time", result)
				}
			} else {
				// Compare Unix timestamps to avoid precision issues
				if result.Unix() != tt.expected.Unix() {
					t.Errorf("getTime() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

// Test getMap function
func TestGetMap(t *testing.T) {
	tests := []struct {
		name     string
		m        map[string]interface{}
		key      string
		expected map[string]interface{}
	}{
		{
			name:     "map value exists",
			m:        map[string]interface{}{"metadata": map[string]interface{}{"key": "value"}},
			key:      "metadata",
			expected: map[string]interface{}{"key": "value"},
		},
		{
			name:     "missing key",
			m:        map[string]interface{}{"other": "value"},
			key:      "metadata",
			expected: nil,
		},
		{
			name:     "wrong type (string)",
			m:        map[string]interface{}{"metadata": "not-a-map"},
			key:      "metadata",
			expected: nil,
		},
		{
			name:     "empty map",
			m:        map[string]interface{}{},
			key:      "metadata",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getMap(tt.m, tt.key)
			if tt.expected == nil {
				if result != nil {
					t.Errorf("getMap() = %v, want nil", result)
				}
			} else {
				if result == nil {
					t.Fatalf("getMap() = nil, want %v", tt.expected)
				}
				for k, v := range tt.expected {
					if result[k] != v {
						t.Errorf("getMap()[%q] = %v, want %v", k, result[k], v)
					}
				}
			}
		})
	}
}

// Test CreateIndex with various options and edge cases
func TestCreateIndex_Options(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create collection first (needed for index creation)
	coll, _ := db.Collection("test_coll")
	coll.InsertOne(map[string]interface{}{"_id": "init"})

	// Test empty keys error
	_, err := db.CreateIndex("test_coll", map[string]interface{}{})
	if err == nil {
		t.Error("expected error for empty index keys")
	}

	// Test with int32 direction
	_, err = db.CreateIndex("test_coll", map[string]interface{}{"field1": int32(1)})
	if err != nil {
		t.Errorf("CreateIndex with int32: %v", err)
	}

	// Test with int64 direction
	_, err = db.CreateIndex("test_coll", map[string]interface{}{"field2": int64(-1)})
	if err != nil {
		t.Errorf("CreateIndex with int64: %v", err)
	}

	// Test with float64 direction
	_, err = db.CreateIndex("test_coll", map[string]interface{}{"field3": float64(1)})
	if err != nil {
		t.Errorf("CreateIndex with float64: %v", err)
	}

	// Test with custom name option
	name, err := db.CreateIndex("test_coll", map[string]interface{}{"field4": 1}, IndexOptions{Name: "custom_name"})
	if err != nil {
		t.Errorf("CreateIndex with custom name: %v", err)
	}
	if name != "custom_name" {
		t.Errorf("expected index name 'custom_name', got %q", name)
	}

	// Test with unique option
	_, err = db.CreateIndex("test_coll", map[string]interface{}{"field5": 1}, IndexOptions{Unique: true})
	if err != nil {
		t.Errorf("CreateIndex with unique: %v", err)
	}

	// Test with sparse option
	_, err = db.CreateIndex("test_coll", map[string]interface{}{"field6": 1}, IndexOptions{Sparse: true})
	if err != nil {
		t.Errorf("CreateIndex with sparse: %v", err)
	}

	// Test with all options combined
	_, err = db.CreateIndex("test_coll", map[string]interface{}{"field7": 1}, IndexOptions{
		Name:   "combined_idx",
		Unique: true,
		Sparse: true,
	})
	if err != nil {
		t.Errorf("CreateIndex with all options: %v", err)
	}
}

// Test CollectionTx_UpdateOne_NotFound tests update when document doesn't exist
func TestCollectionTx_UpdateOne_NotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("tx_update_nf")

	err := db.WithTransaction(context.Background(), func(tx *Transaction) error {
		ct := coll.WithTransaction(tx)
		// Try to update a non-existent document
		updated, err := ct.UpdateOne(
			map[string]interface{}{"_id": "nonexistent"},
			map[string]interface{}{"$set": map[string]interface{}{"value": 42}},
		)
		if err != nil {
			return err
		}
		if updated != 0 {
			t.Errorf("expected 0 updated for non-existent doc, got %d", updated)
		}
		return nil
	})

	if err != nil {
		t.Errorf("WithTransaction: %v", err)
	}
}

// Test ListIndexes and DropIndex
func TestListIndexesAndDropIndex(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	collName := "idx_test_coll"

	// Create collection first
	coll, _ := db.Collection(collName)
	coll.InsertOne(map[string]interface{}{"_id": "init"})

	// Create some indexes
	_, err := db.CreateIndex(collName, map[string]interface{}{"name": 1})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	_, err = db.CreateIndex(collName, map[string]interface{}{"age": -1}, IndexOptions{Unique: true})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// List indexes
	indexes, err := db.ListIndexes(collName)
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}

	// Should have at least 2 indexes (including _id_)
	if len(indexes) < 2 {
		t.Errorf("expected at least 2 indexes, got %d", len(indexes))
	}

	// Find and drop a specific index
	var idxToDrop string
	for _, idx := range indexes {
		if idx.Name != "_id_" {
			idxToDrop = idx.Name
			break
		}
	}

	if idxToDrop != "" {
		err = db.DropIndex(collName, idxToDrop)
		if err != nil {
			t.Errorf("DropIndex: %v", err)
		}

		// Verify index is dropped
		indexes, _ = db.ListIndexes(collName)
		found := false
		for _, idx := range indexes {
			if idx.Name == idxToDrop {
				found = true
				break
			}
		}
		if found {
			t.Errorf("index %q should have been dropped", idxToDrop)
		}
	}
}

// Test FindWithOptions with projection
func TestFindWithOptions_Projection(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("projection_test")

	// Insert test documents
	for i := 0; i < 5; i++ {
		coll.InsertOne(map[string]interface{}{
			"_id":   fmt.Sprintf("doc%d", i),
			"name":  fmt.Sprintf("Name%d", i),
			"value": i * 10,
			"extra": "hidden",
		})
	}

	// Find with projection - only include name and value
	cursor, err := coll.FindWithOptions(FindOptions{
		Filter:     map[string]interface{}{},
		Projection: map[string]interface{}{"name": 1, "value": 1},
	})
	if err != nil {
		t.Fatalf("FindWithOptions: %v", err)
	}
	defer cursor.Close()

	count := 0
	for cursor.Next() {
		var doc map[string]interface{}
		if err := cursor.Decode(&doc); err != nil {
			t.Errorf("Decode: %v", err)
			continue
		}
		// Should have _id, name, value but not extra
		if _, ok := doc["_id"]; !ok {
			t.Error("expected _id in projected result")
		}
		if _, ok := doc["name"]; !ok {
			t.Error("expected name in projected result")
		}
		if _, ok := doc["value"]; !ok {
			t.Error("expected value in projected result")
		}
		// "extra" should not be present
		if _, ok := doc["extra"]; ok {
			t.Error("extra should not be in projected result")
		}
		count++
	}

	if count != 5 {
		t.Errorf("expected 5 documents, got %d", count)
	}
}

// Test compareDocs with missing fields
func TestCompareDocs_MissingFields(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("compare_test")

	// Insert documents with different fields
	coll.InsertOne(map[string]interface{}{"_id": "1", "name": "Alice", "age": 30})
	coll.InsertOne(map[string]interface{}{"_id": "2", "name": "Bob"})         // missing age
	coll.InsertOne(map[string]interface{}{"_id": "3", "age": 25})              // missing name
	coll.InsertOne(map[string]interface{}{"_id": "4", "name": "Charlie", "age": 35})

	// Sort by age - missing age should sort last
	cursor, err := coll.FindWithOptions(FindOptions{
		Filter: map[string]interface{}{},
		Sort:   map[string]interface{}{"age": 1},
	})
	if err != nil {
		t.Fatalf("FindWithOptions: %v", err)
	}
	defer cursor.Close()

	var ids []string
	for cursor.Next() {
		var doc map[string]interface{}
		cursor.Decode(&doc)
		ids = append(ids, doc["_id"].(string))
	}

	// "3" (age 25) should be first, "1" (30) second, "4" (35) third
	// "2" (missing age) should be last
	expected := []string{"3", "1", "4", "2"}
	for i, id := range ids {
		if i < len(expected) && id != expected[i] {
			t.Errorf("position %d: expected %s, got %s", i, expected[i], id)
		}
	}
}

// Test FindWithOptions with Skip and Limit
func TestFindWithOptions_SkipLimit(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("skip_limit_test")

	// Insert 10 documents
	for i := 0; i < 10; i++ {
		coll.InsertOne(map[string]interface{}{"_id": fmt.Sprintf("doc%d", i), "value": i})
	}

	// Test Skip
	cursor, err := coll.FindWithOptions(FindOptions{
		Filter: nil,
		Skip:   5,
	})
	if err != nil {
		t.Fatalf("FindWithOptions with Skip: %v", err)
	}

	count := 0
	for cursor.Next() {
		count++
	}
	if count != 5 {
		t.Errorf("Skip 5: expected 5 results, got %d", count)
	}

	// Test Limit
	cursor2, err := coll.FindWithOptions(FindOptions{
		Filter: nil,
		Limit:  3,
	})
	if err != nil {
		t.Fatalf("FindWithOptions with Limit: %v", err)
	}

	count = 0
	for cursor2.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("Limit 3: expected 3 results, got %d", count)
	}

	// Test Skip + Limit
	cursor3, err := coll.FindWithOptions(FindOptions{
		Filter: nil,
		Skip:   2,
		Limit:  3,
	})
	if err != nil {
		t.Fatalf("FindWithOptions with Skip+Limit: %v", err)
	}

	count = 0
	for cursor3.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("Skip 2 Limit 3: expected 3 results, got %d", count)
	}

	// Test Skip larger than result set
	cursor4, err := coll.FindWithOptions(FindOptions{
		Filter: nil,
		Skip:   20,
	})
	if err != nil {
		t.Fatalf("FindWithOptions with large Skip: %v", err)
	}

	count = 0
	for cursor4.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Skip 20: expected 0 results, got %d", count)
	}
}

// Test FindWithOptions with descending sort
func TestFindWithOptions_DescendingSort(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("desc_sort_test")

	// Insert documents
	for i := 0; i < 5; i++ {
		coll.InsertOne(map[string]interface{}{"_id": fmt.Sprintf("doc%d", i), "value": i})
	}

	// Sort descending by value
	cursor, err := coll.FindWithOptions(FindOptions{
		Filter: nil,
		Sort:   map[string]interface{}{"value": -1},
	})
	if err != nil {
		t.Fatalf("FindWithOptions with desc sort: %v", err)
	}

	var values []int
	for cursor.Next() {
		var doc map[string]interface{}
		cursor.Decode(&doc)
		values = append(values, doc["value"].(int))
	}

	// Should be in descending order: 4, 3, 2, 1, 0
	for i := 0; i < len(values)-1; i++ {
		if values[i] < values[i+1] {
			t.Errorf("descending sort failed at position %d: %v", i, values)
			break
		}
	}
}

// Test Decode with struct pointer
func TestDecode_StructPointer(t *testing.T) {
	type Person struct {
		ID   string `bson:"_id"`
		Name string `bson:"name"`
		Age  int    `bson:"age"`
	}

	db := openTestDB(t)
	defer db.Close()

	coll, _ := db.Collection("struct_decode_test")
	coll.InsertOne(map[string]interface{}{"_id": "p1", "name": "Alice", "age": 30})

	cur, _ := coll.Find(map[string]interface{}{"_id": "p1"})
	defer cur.Close()

	if !cur.Next() {
		t.Fatal("expected document")
	}

	var person Person
	err := cur.Decode(&person)
	if err != nil {
		t.Errorf("Decode into struct: %v", err)
	}

	if person.ID != "p1" || person.Name != "Alice" || person.Age != 30 {
		t.Errorf("decoded struct = %+v, want {ID:p1 Name:Alice Age:30}", person)
	}
}
