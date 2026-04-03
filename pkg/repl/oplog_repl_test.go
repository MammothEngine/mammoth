package repl

import (
	"context"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func TestOplogReplicator_StartStop(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)
	rs, rsm := setupTestReplicaSet(t, eng)

	replicator := NewOplogReplicator(oplog, rs, rsm)

	if err := replicator.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Should not be able to start twice
	if err := replicator.Start(); err == nil {
		t.Error("expected error on double start")
	}

	replicator.Stop()

	// Should be able to restart after stop
	if err := replicator.Start(); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	replicator.Stop()
}

func TestOplogReplicator_LogOperation_NotLeader(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)
	rs, rsm := setupTestReplicaSet(t, eng)

	replicator := NewOplogReplicator(oplog, rs, rsm)
	replicator.Start()
	defer replicator.Stop()

	// Don't start the replica set, so it won't be leader
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("test"))

	_, err = replicator.LogOperation(OpInsert, "test.items", doc, nil)
	if err == nil {
		t.Error("expected error when not leader")
	}
}

func TestOplogReplicator_Tail(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)
	rs, rsm := setupTestReplicaSet(t, eng)

	replicator := NewOplogReplicator(oplog, rs, rsm)
	replicator.Start()
	defer replicator.Stop()

	// Create tail client
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	since := time.Now().UTC()
	client, err := replicator.Tail(ctx, since, nil)
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}

	// Stop tail when done
	defer replicator.StopTail(client.ID)

	// Verify client was created
	if client.ID == "" {
		t.Error("expected non-empty client ID")
	}
	if client.Ch == nil {
		t.Error("expected non-nil channel")
	}
}

func TestOplogReplicator_matchesFilter(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)
	rs, rsm := setupTestReplicaSet(t, eng)

	replicator := NewOplogReplicator(oplog, rs, rsm)

	entry := &OplogEntry{
		Operation: OpInsert,
		Namespace: "test.collection",
	}

	// Nil filter matches all
	if !replicator.matchesFilter(entry, nil) {
		t.Error("nil filter should match all")
	}

	// Namespace filter
	filter := map[string]interface{}{"ns": "test.collection"}
	if !replicator.matchesFilter(entry, filter) {
		t.Error("should match correct namespace")
	}

	filter = map[string]interface{}{"ns": "other.collection"}
	if replicator.matchesFilter(entry, filter) {
		t.Error("should not match wrong namespace")
	}

	// Operation filter
	filter = map[string]interface{}{"op": "i"}
	if !replicator.matchesFilter(entry, filter) {
		t.Error("should match correct op")
	}

	filter = map[string]interface{}{"op": "d"}
	if replicator.matchesFilter(entry, filter) {
		t.Error("should not match wrong op")
	}
}

func TestOplogReplicator_GetReplicationStatus(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)
	rs, rsm := setupTestReplicaSet(t, eng)

	replicator := NewOplogReplicator(oplog, rs, rsm)
	replicator.Start()
	defer replicator.Stop()

	status := replicator.GetReplicationStatus()

	// Since replica set is not started, we shouldn't be leader
	if status.IsPrimary {
		t.Error("expected not to be primary when RS not started")
	}

	if status.LastAppliedHash != 0 {
		t.Error("expected zero initial hash")
	}
}

func TestOplogReplicator_ApplyOplogEntry(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)
	rs, rsm := setupTestReplicaSet(t, eng)

	replicator := NewOplogReplicator(oplog, rs, rsm)

	// Apply an insert command
	cmd := OplogCommand{
		Op:        OpInsert,
		Namespace: "test.users",
		Timestamp: time.Now().UTC(),
		Term:      1,
		Hash:      1,
		Object: map[string]interface{}{
			"_id":  "user1",
			"name": "alice",
		},
	}

	if err := replicator.ApplyOplogEntry(cmd); err != nil {
		t.Fatalf("ApplyOplogEntry: %v", err)
	}

	// Verify document was inserted
	key := []byte("test.users.user1")
	data, err := eng.Get(key)
	if err != nil {
		t.Fatalf("Get after insert: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected data after insert")
	}
}

func TestOplogCommand_Conversion(t *testing.T) {
	// Test docToMap and mapToDoc conversion
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("test"))
	doc.Set("count", bson.VInt32(42))
	doc.Set("active", bson.VBool(true))

	m := docToMap(doc)
	if m["name"] != "test" {
		t.Errorf("expected name=test, got %v", m["name"])
	}
	if m["count"] != int32(42) {
		t.Errorf("expected count=42, got %v", m["count"])
	}
	if m["active"] != true {
		t.Errorf("expected active=true, got %v", m["active"])
	}

	// Convert back
	doc2 := mapToDoc(m)
	if doc2 == nil {
		t.Fatal("expected non-nil doc")
	}

	name, ok := doc2.Get("name")
	if !ok || name.String() != "test" {
		t.Error("name mismatch after round-trip")
	}
}

func TestBSONValueConversion(t *testing.T) {
	tests := []struct {
		name     string
		value    bson.Value
		expected interface{}
	}{
		{"string", bson.VString("hello"), "hello"},
		{"int32", bson.VInt32(42), int32(42)},
		{"int64", bson.VInt64(100), int64(100)},
		{"double", bson.VDouble(3.14), 3.14},
		{"bool", bson.VBool(true), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bsonValueToGo(tt.value)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestGoValueConversion(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected bson.BSONType
	}{
		{"string", "hello", bson.TypeString},
		{"int", int(42), bson.TypeInt64},
		{"int32", int32(42), bson.TypeInt32},
		{"int64", int64(100), bson.TypeInt64},
		{"double", float64(3.14), bson.TypeDouble},
		{"bool", true, bson.TypeBoolean},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := goValueToBSON(tt.value)
			if result.Type != tt.expected {
				t.Errorf("expected type %v, got %v", tt.expected, result.Type)
			}
		})
	}
}

// Helper function to set up a test replica set
func setupTestReplicaSet(t *testing.T, eng *engine.Engine) (*ReplicaSet, *ReplicaSetManager) {
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:1", Voter: true},
		},
		Version: 1,
	}

	rs := NewReplicaSet(ReplicaSetConfig{
		ID:     1,
		Config: cfg,
		Engine: eng,
	})

	rsm := NewReplicaSetManager(rs)

	return rs, rsm
}
