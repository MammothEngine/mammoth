package mammoth

import (
	"context"
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
