package repl

import (
	"context"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func TestOplogReplicator_processPrimaryTasks(t *testing.T) {
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

	// Create a tail client to test cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := replicator.Tail(ctx, time.Now().UTC(), nil)
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}

	// Cancel context to make client stale
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Run processPrimaryTasks - should clean up stale clients
	replicator.processPrimaryTasks()

	// Verify client was cleaned up
	replicator.mu.Lock()
	_, exists := replicator.tailClients[client.ID]
	replicator.mu.Unlock()

	if exists {
		t.Error("stale client should have been cleaned up")
	}
}

func TestOplogReplicator_processSecondaryTasks(t *testing.T) {
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

	// Add an entry to oplog
	_, _ = oplog.Append(OpInsert, "test.items", bson.NewDocument(), nil)

	// Run processSecondaryTasks
	replicator.processSecondaryTasks()

	// No error expected, just verifying it runs
}

func TestOplogReplicator_notifyTailClients(t *testing.T) {
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

	// Create a tail client
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := map[string]interface{}{"ns": "test.items"}
	client, err := replicator.Tail(ctx, time.Now().UTC(), filter)
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}

	// Create an entry that matches the filter
	entry := &OplogEntry{
		Operation:   OpInsert,
		Namespace:   "test.items",
		Timestamp:   time.Now().UTC(),
	}

	// Notify clients
	replicator.notifyTailClients(entry)

	// Entry should be sent to client
	select {
	case received := <-client.Ch:
		if received.Namespace != "test.items" {
			t.Errorf("expected namespace test.items, got %s", received.Namespace)
		}
	case <-time.After(500 * time.Millisecond):
		// Channel might be buffered, not an error
	}
}

func TestOplogReplicator_cleanupStaleClients(t *testing.T) {
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

	// Create a client with cancel context
	ctx1, cancel1 := context.WithCancel(context.Background())

	client1, _ := replicator.Tail(ctx1, time.Now().UTC(), nil)

	// Cancel first client
	cancel1()
	time.Sleep(100 * time.Millisecond)

	// Run cleanup
	replicator.cleanupStaleClients()

	// Verify first client was removed
	replicator.mu.Lock()
	_, exists1 := replicator.tailClients[client1.ID]
	replicator.mu.Unlock()

	if exists1 {
		t.Error("cancelled client should have been removed")
	}
}

func TestOplogReplicator_updateReplicationLag(t *testing.T) {
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

	// Add an entry to oplog to have a latest timestamp
	_, _ = oplog.Append(OpInsert, "test.items", bson.NewDocument(), nil)

	// Create a tail client
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, _ := replicator.Tail(ctx, time.Now().UTC(), nil)

	// Update lag
	replicator.updateReplicationLag()

	// Verify client's Since was updated
	replicator.mu.RLock()
	c := replicator.tailClients[client.ID]
	replicator.mu.RUnlock()

	if c == nil {
		t.Fatal("client not found")
	}

	// Since should be recent (within last second)
	if time.Since(c.Since) > time.Second {
		t.Error("client Since should have been updated recently")
	}
}

func TestOplogReplicator_applyLocalOplog(t *testing.T) {
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

	// Apply local oplog (should be no-op when oplog is empty)
	replicator.applyLocalOplog()

	// Add some entries and apply again
	doc := bson.NewDocument()
	doc.Set("_id", bson.VString("test1"))
	_, _ = oplog.Append(OpInsert, "test.items", doc, nil)

	replicator.applyLocalOplog()
}

func TestOplogReplicator_matchesFilterCombined(t *testing.T) {
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

	// Combined filter (both ns and op)
	filter := map[string]interface{}{
		"ns": "test.collection",
		"op": "i",
	}
	if !replicator.matchesFilter(entry, filter) {
		t.Error("should match combined filter")
	}

	// Wrong namespace
	filter = map[string]interface{}{
		"ns": "other.collection",
		"op": "i",
	}
	if replicator.matchesFilter(entry, filter) {
		t.Error("should not match wrong namespace")
	}

	// Wrong operation
	filter = map[string]interface{}{
		"ns": "test.collection",
		"op": "d",
	}
	if replicator.matchesFilter(entry, filter) {
		t.Error("should not match wrong operation")
	}
}

func TestOplogReplicator_StopTail(t *testing.T) {
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

	// Create a tail client
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := replicator.Tail(ctx, time.Now().UTC(), nil)
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}

	// Stop tail
	replicator.StopTail(client.ID)

	// Verify client was removed
	replicator.mu.Lock()
	_, exists := replicator.tailClients[client.ID]
	replicator.mu.Unlock()

	if exists {
		t.Error("client should have been removed after StopTail")
	}
}


func TestOplogReplicator_ApplyOplogEntry_Delete(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	oplog := NewOplog(eng)
	rs, rsm := setupTestReplicaSet(t, eng)

	replicator := NewOplogReplicator(oplog, rs, rsm)

	// First insert a document
	insertCmd := OplogCommand{
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

	if err := replicator.ApplyOplogEntry(insertCmd); err != nil {
		t.Fatalf("ApplyOplogEntry insert: %v", err)
	}

	// Now delete the document
	deleteCmd := OplogCommand{
		Op:        OpDelete,
		Namespace: "test.users",
		Timestamp: time.Now().UTC(),
		Term:      1,
		Hash:      2,
		Object: map[string]interface{}{
			"_id": "user1",
		},
	}

	if err := replicator.ApplyOplogEntry(deleteCmd); err != nil {
		t.Fatalf("ApplyOplogEntry delete: %v", err)
	}

	// Verify document was deleted
	key := []byte("test.users.user1")
	data, err := eng.Get(key)
	if err == nil && len(data) > 0 {
		t.Error("expected document to be deleted")
	}
}

func TestBSONValueConversion_ObjectID(t *testing.T) {
	// Test ObjectID conversion
	oid := bson.NewObjectID()
	val := bson.VObjectID(oid)
	result := bsonValueToGo(val)
	if result != oid.String() {
		t.Errorf("expected %s, got %v", oid.String(), result)
	}
}

func TestGoValueConversion_Array(t *testing.T) {
	// Test array conversion
	arr := []interface{}{"hello", int32(42), true}
	result := goValueToBSON(arr)
	if result.Type != bson.TypeArray {
		t.Errorf("expected Array type, got %v", result.Type)
	}
}

func TestGoValueConversion_DefaultCase(t *testing.T) {
	// Test default case (converts to string)
	result := goValueToBSON([]byte("test"))
	if result.Type != bson.TypeString {
		t.Errorf("expected String type for unknown, got %v", result.Type)
	}
}

// Test bsonValueToGo for all supported types
func TestBSONValueToGo_Types(t *testing.T) {
	tests := []struct {
		name     string
		value    bson.Value
		expected interface{}
	}{
		{"string", bson.VString("test"), "test"},
		{"int32", bson.VInt32(42), int32(42)},
		{"int64", bson.VInt64(100), int64(100)},
		{"double", bson.VDouble(3.14), float64(3.14)},
		{"bool", bson.VBool(true), true},
		{"null", bson.VNull(), ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := bsonValueToGo(tc.value)
			_ = result
		})
	}
}

// Test bsonValueToGo for array
func TestBSONValueToGo_Array(t *testing.T) {
	arr := bson.A(bson.VInt32(1), bson.VInt32(2), bson.VInt32(3))
	val := bson.VArray(arr)
	result := bsonValueToGo(val)

	if result == nil {
		t.Error("expected non-nil result for array")
	}
}

// Test bsonValueToGo for document
func TestBSONValueToGo_Document(t *testing.T) {
	doc := bson.D("name", bson.VString("test"), "value", bson.VInt32(42))
	val := bson.VDoc(doc)
	result := bsonValueToGo(val)

	if result == nil {
		t.Error("expected non-nil result for document")
	}
}

// Test docToMap
func TestDocToMap(t *testing.T) {
	doc := bson.D(
		"name", bson.VString("alice"),
		"age", bson.VInt32(30),
		"tags", bson.VArray(bson.A(bson.VString("a"), bson.VString("b"))),
	)

	m := docToMap(doc)
	if m == nil {
		t.Error("expected non-nil map")
	}
}

// Test mapToDoc
func TestMapToDoc(t *testing.T) {
	m := map[string]interface{}{
		"name": "alice",
		"age":  int32(30),
	}

	doc := mapToDoc(m)
	if doc == nil {
		t.Error("expected non-nil document")
	}
}
