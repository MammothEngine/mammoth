package repl

import (
	"encoding/json"
	"testing"
)

func TestStateMachineApply(t *testing.T) {
	eng := newMemEngine()
	sm := NewMammothStateMachine(eng)

	// Apply a put command
	putCmd := Command{Op: "put", Key: []byte("k1"), Value: []byte("v1")}
	data, _ := json.Marshal(putCmd)
	err := sm.Apply(LogEntry{Index: 1, Term: 1, Data: data})
	if err != nil {
		t.Fatalf("Apply put: %v", err)
	}

	// Verify
	val, err := eng.Get([]byte("k1"))
	if err != nil || string(val) != "v1" {
		t.Fatalf("expected v1, got %s, err=%v", val, err)
	}
}

func TestStateMachineApplyDelete(t *testing.T) {
	eng := newMemEngine()
	eng.Put([]byte("k1"), []byte("v1"))
	sm := NewMammothStateMachine(eng)

	delCmd := Command{Op: "delete", Key: []byte("k1")}
	data, _ := json.Marshal(delCmd)
	err := sm.Apply(LogEntry{Index: 1, Term: 1, Data: data})
	if err != nil {
		t.Fatalf("Apply delete: %v", err)
	}

	_, err = eng.Get([]byte("k1"))
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestStateMachineApplyBatch(t *testing.T) {
	eng := newMemEngine()
	sm := NewMammothStateMachine(eng)

	batchCmd := Command{
		Op: "batch",
		Ops: []Command{
			{Op: "put", Key: []byte("a"), Value: []byte("1")},
			{Op: "put", Key: []byte("b"), Value: []byte("2")},
			{Op: "put", Key: []byte("c"), Value: []byte("3")},
		},
	}
	data, _ := json.Marshal(batchCmd)
	err := sm.Apply(LogEntry{Index: 1, Term: 1, Data: data})
	if err != nil {
		t.Fatalf("Apply batch: %v", err)
	}

	for _, tc := range []struct{ key, want string }{
		{"a", "1"}, {"b", "2"}, {"c", "3"},
	} {
		val, err := eng.Get([]byte(tc.key))
		if err != nil {
			t.Fatalf("Get(%s): %v", tc.key, err)
		}
		if string(val) != tc.want {
			t.Fatalf("expected %s=%s, got %s", tc.key, tc.want, val)
		}
	}
}

func TestStateMachineConfigChangeNoop(t *testing.T) {
	eng := newMemEngine()
	sm := NewMammothStateMachine(eng)

	// Config change entries (Type=1) should be no-op
	err := sm.Apply(LogEntry{Index: 1, Term: 1, Type: 1, Data: []byte("ignored")})
	if err != nil {
		t.Fatalf("Apply config change: %v", err)
	}
}

func TestSnapshotAndRestore(t *testing.T) {
	eng := newMemEngine()
	sm := NewMammothStateMachine(eng)

	// Put some data
	eng.Put([]byte("k1"), []byte("v1"))
	eng.Put([]byte("k2"), []byte("v2"))
	eng.Put([]byte("k3"), []byte("v3"))

	// Take snapshot
	snapData, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Modify engine
	eng.Put([]byte("k4"), []byte("v4"))
	eng.Delete([]byte("k1"))

	// Restore from snapshot
	err = sm.Restore(snapData)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Verify restored state
	val, err := eng.Get([]byte("k1"))
	if err != nil || string(val) != "v1" {
		t.Fatalf("expected k1=v1 after restore, got %s", val)
	}
	val, err = eng.Get([]byte("k3"))
	if err != nil || string(val) != "v3" {
		t.Fatalf("expected k3=v3 after restore, got %s", val)
	}
	// k4 should be gone (was added after snapshot)
	_, err = eng.Get([]byte("k4"))
	if err == nil {
		t.Fatal("expected k4 to be gone after restore")
	}
}

func TestSnapshotChunking(t *testing.T) {
	snap := SnapshotData{
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Data:              make([]byte, 2500),
	}

	chunks := ChunkSnapshot(snap, 1000)
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	// First chunk: offset 0, not done
	if chunks[0].Offset != 0 || chunks[0].Done {
		t.Fatal("first chunk should have offset=0, done=false")
	}
	// Last chunk: done
	if !chunks[2].Done {
		t.Fatal("last chunk should be done=true")
	}

	// Reassemble
	builder := NewSnapshotBuilder()
	for _, c := range chunks {
		done := builder.ApplyChunk(c)
		if c.Done && !done {
			t.Fatal("expected done=true on last chunk")
		}
	}
	result := builder.Build()
	if result.LastIncludedIndex != 10 {
		t.Fatalf("expected lastIndex=10, got %d", result.LastIncludedIndex)
	}
	if len(result.Data) != 2500 {
		t.Fatalf("expected 2500 bytes, got %d", len(result.Data))
	}
}

func TestSnapshotEncodeDecode(t *testing.T) {
	snap := SnapshotData{
		LastIncludedIndex: 42,
		LastIncludedTerm:  7,
		Data:              []byte("hello snapshot"),
	}

	encoded, err := EncodeSnapshotData(snap)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := DecodeSnapshotData(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.LastIncludedIndex != 42 || decoded.LastIncludedTerm != 7 {
		t.Fatalf("metadata mismatch: %+v", decoded)
	}
	if string(decoded.Data) != "hello snapshot" {
		t.Fatalf("data mismatch: %s", decoded.Data)
	}
}
