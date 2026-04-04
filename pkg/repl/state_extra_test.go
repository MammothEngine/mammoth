package repl

import (
	"encoding/json"
	"testing"
	"time"
)

// Test Apply with config change entry (type=1)
func TestStateMachineApply_ConfigChange(t *testing.T) {
	eng := newMemEngine()
	sm := NewMammothStateMachine(eng)

	entry := LogEntry{
		Type: 1, // Config change
		Data: []byte(`{"some": "config"}`),
	}

	err := sm.Apply(entry)
	if err != nil {
		t.Errorf("Apply config change: %v", err)
	}
}

// Test Apply with invalid JSON data
func TestStateMachineApply_InvalidJSON(t *testing.T) {
	eng := newMemEngine()
	sm := NewMammothStateMachine(eng)

	entry := LogEntry{
		Type: 0,
		Data: []byte(`not valid json`),
	}

	err := sm.Apply(entry)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// Test Apply with put command
func TestStateMachineApply_Put(t *testing.T) {
	eng := newMemEngine()
	sm := NewMammothStateMachine(eng)

	cmd := Command{
		Op:    "put",
		Key:   []byte("testkey"),
		Value: []byte("testvalue"),
	}
	data, _ := json.Marshal(cmd)

	entry := LogEntry{
		Type: 0,
		Data: data,
	}

	err := sm.Apply(entry)
	if err != nil {
		t.Errorf("Apply put: %v", err)
	}

	// Verify value was stored
	val, err := eng.Get([]byte("testkey"))
	if err != nil {
		t.Fatalf("Get after put: %v", err)
	}
	if string(val) != "testvalue" {
		t.Errorf("expected value 'testvalue', got %q", string(val))
	}
}

// Test Apply with delete command
func TestStateMachineApply_Delete(t *testing.T) {
	eng := newMemEngine()
	sm := NewMammothStateMachine(eng)

	// First put a value
	_ = eng.Put([]byte("delkey"), []byte("value"))

	cmd := Command{
		Op:  "delete",
		Key: []byte("delkey"),
	}
	data, _ := json.Marshal(cmd)

	entry := LogEntry{
		Type: 0,
		Data: data,
	}

	err := sm.Apply(entry)
	if err != nil {
		t.Errorf("Apply delete: %v", err)
	}

	// Verify value was deleted
	_, err = eng.Get([]byte("delkey"))
	if err == nil {
		t.Error("expected key to be deleted")
	}
}

// Test Apply with batch command
func TestStateMachineApply_Batch(t *testing.T) {
	eng := newMemEngine()
	sm := NewMammothStateMachine(eng)

	cmd := Command{
		Op: "batch",
		Ops: []Command{
			{Op: "put", Key: []byte("key1"), Value: []byte("val1")},
			{Op: "put", Key: []byte("key2"), Value: []byte("val2")},
			{Op: "delete", Key: []byte("key1")},
		},
	}
	data, _ := json.Marshal(cmd)

	entry := LogEntry{
		Type: 0,
		Data: data,
	}

	err := sm.Apply(entry)
	if err != nil {
		t.Errorf("Apply batch: %v", err)
	}

	// Verify key1 was deleted
	_, err = eng.Get([]byte("key1"))
	if err == nil {
		t.Error("expected key1 to be deleted")
	}

	// Verify key2 exists
	val, err := eng.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2: %v", err)
	}
	if string(val) != "val2" {
		t.Errorf("expected key2='val2', got %q", string(val))
	}
}

// Test Apply with oplog command
func TestStateMachineApply_Oplog(t *testing.T) {
	eng := newMemEngine()
	sm := NewMammothStateMachine(eng)

	oplogCmd := OplogCommand{
		Op:        OpInsert,
		Timestamp: time.Now(),
		Hash:      1,
		Namespace: "test.collection",
		Object: map[string]interface{}{
			"_id":  "testdoc",
			"name": "test",
		},
	}
	oplogData, _ := json.Marshal(oplogCmd)

	cmd := Command{
		Op:    "oplog",
		Value: oplogData,
	}
	data, _ := json.Marshal(cmd)

	entry := LogEntry{
		Type: 0,
		Data: data,
	}

	err := sm.Apply(entry)
	if err != nil {
		t.Errorf("Apply oplog: %v", err)
	}
}

// Test Apply with oplog command having invalid JSON
func TestStateMachineApply_OplogInvalidJSON(t *testing.T) {
	eng := newMemEngine()
	sm := NewMammothStateMachine(eng)

	cmd := Command{
		Op:    "oplog",
		Value: []byte(`not valid json`),
	}
	data, _ := json.Marshal(cmd)

	entry := LogEntry{
		Type: 0,
		Data: data,
	}

	err := sm.Apply(entry)
	if err == nil {
		t.Error("expected error for invalid oplog JSON")
	}
}

// Test Apply with unknown command
func TestStateMachineApply_UnknownOp(t *testing.T) {
	eng := newMemEngine()
	sm := NewMammothStateMachine(eng)

	cmd := Command{
		Op:    "unknown_op",
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	data, _ := json.Marshal(cmd)

	entry := LogEntry{
		Type: 0,
		Data: data,
	}

	// Unknown op should return nil (no-op)
	err := sm.Apply(entry)
	if err != nil {
		t.Errorf("Apply unknown op: %v", err)
	}
}
