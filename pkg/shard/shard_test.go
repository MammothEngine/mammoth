package shard

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupShardTest(t *testing.T) (*Config, *Router, func()) {
	t.Helper()

	cfg := NewConfig()

	// Add test shards
	cfg.AddShard(&Shard{ID: "shard1", Host: "localhost:27018", State: "active"})
	cfg.AddShard(&Shard{ID: "shard2", Host: "localhost:27019", State: "active"})
	cfg.AddShard(&Shard{ID: "shard3", Host: "localhost:27020", State: "active"})

	router := NewRouter(cfg)

	// Create temp engines for each shard
	engines := make([]*engine.Engine, 0, 3)
	for _, s := range cfg.ListShards() {
		dir := t.TempDir()
		eng, err := engine.Open(engine.DefaultOptions(dir))
		if err != nil {
			t.Fatalf("Failed to create engine for shard %s: %v", s.ID, err)
		}
		router.ConnectShard(s.ID, eng)
		engines = append(engines, eng)
	}

	cleanup := func() {
		for _, eng := range engines {
			eng.Close()
		}
	}

	return cfg, router, cleanup
}

func TestConfig_AddShard(t *testing.T) {
	cfg := NewConfig()

	s1 := &Shard{ID: "shard1", Host: "localhost:27018"}
	if err := cfg.AddShard(s1); err != nil {
		t.Fatalf("AddShard failed: %v", err)
	}

	// Duplicate should fail
	if err := cfg.AddShard(s1); err == nil {
		t.Error("Expected error for duplicate shard")
	}

	shards := cfg.ListShards()
	if len(shards) != 1 {
		t.Errorf("Expected 1 shard, got %d", len(shards))
	}
}

func TestConfig_ShardKey(t *testing.T) {
	cfg := NewConfig()

	sk := &ShardKey{
		Fields: []string{"user_id"},
		Ns:     "testdb.users",
	}
	cfg.SetShardKey(sk)

	retrieved, ok := cfg.GetShardKey("testdb.users")
	if !ok {
		t.Fatal("GetShardKey failed")
	}
	if len(retrieved.Fields) != 1 || retrieved.Fields[0] != "user_id" {
		t.Errorf("Unexpected fields: %v", retrieved.Fields)
	}
}

func TestExtractShardKey(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("user_id", bson.VInt64(42))
	doc.Set("name", bson.VString("test"))

	sk := &ShardKey{Fields: []string{"user_id"}, Ns: "testdb.users"}
	val, err := ExtractShardKey(doc, sk)
	if err != nil {
		t.Fatalf("ExtractShardKey failed: %v", err)
	}
	if val != int64(42) {
		t.Errorf("Expected 42, got %v", val)
	}
}

func TestExtractShardKey_Compound(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("country", bson.VString("TR"))
	doc.Set("city", bson.VString("Istanbul"))

	sk := &ShardKey{Fields: []string{"country", "city"}, Ns: "testdb.locations"}
	val, err := ExtractShardKey(doc, sk)
	if err != nil {
		t.Fatalf("ExtractShardKey failed: %v", err)
	}

	arr, ok := val.([]interface{})
	if !ok || len(arr) != 2 {
		t.Fatalf("Expected array of 2, got %v", val)
	}
	if arr[0] != "TR" || arr[1] != "Istanbul" {
		t.Errorf("Unexpected values: %v", arr)
	}
}

func TestRouter_RouteForWrite(t *testing.T) {
	cfg, router, cleanup := setupShardTest(t)
	defer cleanup()

	// Configure sharding
	cfg.SetShardKey(&ShardKey{
		Fields: []string{"user_id"},
		Ns:     "testdb.users",
	})

	doc := bson.NewDocument()
	doc.Set("user_id", bson.VInt64(12345))
	doc.Set("name", bson.VString("Alice"))

	shardID, err := router.RouteForWrite("testdb.users", doc)
	if err != nil {
		t.Fatalf("RouteForWrite failed: %v", err)
	}

	// Should return one of the shards
	found := false
	for _, s := range cfg.ListShards() {
		if s.ID == shardID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Invalid shard ID: %s", shardID)
	}
}

func TestRouter_RouteForRead(t *testing.T) {
	cfg, router, cleanup := setupShardTest(t)
	defer cleanup()

	// Configure sharding
	cfg.SetShardKey(&ShardKey{
		Fields: []string{"user_id"},
		Ns:     "testdb.users",
	})

	// Create a chunk for user_id=12345
	chunk := &Chunk{
		ID:    "chunk_1",
		Ns:    "testdb.users",
		Min:   ChunkRange{Min: int64(0), Max: int64(100000)},
		Max:   ChunkRange{Min: int64(0), Max: int64(100000)},
		Shard: "shard1",
	}
	cfg.AddChunk(chunk)

	filter := bson.NewDocument()
	filter.Set("user_id", bson.VInt64(12345))

	shards, err := router.RouteForRead("testdb.users", filter)
	if err != nil {
		t.Fatalf("RouteForRead failed: %v", err)
	}

	// Should route to specific shard when shard key is in filter and chunk exists
	if len(shards) != 1 || shards[0] != "shard1" {
		t.Errorf("Expected [shard1], got %v", shards)
	}
}

func TestRouter_RouteForRead_ScatterGather(t *testing.T) {
	cfg, router, cleanup := setupShardTest(t)
	defer cleanup()

	// Configure sharding
	cfg.SetShardKey(&ShardKey{
		Fields: []string{"user_id"},
		Ns:     "testdb.users",
	})

	// Filter without shard key - should scatter to all shards
	filter := bson.NewDocument()
	filter.Set("name", bson.VString("Alice"))

	shards, err := router.RouteForRead("testdb.users", filter)
	if err != nil {
		t.Fatalf("RouteForRead failed: %v", err)
	}

	if len(shards) != 3 {
		t.Errorf("Expected 3 shards for scatter-gather, got %d", len(shards))
	}
}

func TestRouter_IsSharded(t *testing.T) {
	cfg, router, cleanup := setupShardTest(t)
	defer cleanup()

	if router.IsSharded("testdb.users") {
		t.Error("Expected unsharded")
	}

	cfg.SetShardKey(&ShardKey{Fields: []string{"id"}, Ns: "testdb.users"})

	if !router.IsSharded("testdb.users") {
		t.Error("Expected sharded")
	}
}

func TestHashShardKey(t *testing.T) {
	// Same value should hash to same result
	h1 := hashShardKey("test_user_123")
	h2 := hashShardKey("test_user_123")
	if h1 != h2 {
		t.Error("Hash should be deterministic")
	}

	// Different values should likely differ
	h3 := hashShardKey("different_user")
	if h1 == h3 {
		t.Log("Warning: Hash collision (unlikely but possible)")
	}

	// Should distribute across range
	if h1 == 0 {
		t.Error("Hash should not be zero")
	}
}

func TestCompareShardKey(t *testing.T) {
	// String comparison
	if compareShardKey("a", "b") >= 0 {
		t.Error("'a' should be less than 'b'")
	}
	if compareShardKey("b", "a") <= 0 {
		t.Error("'b' should be greater than 'a'")
	}
	if compareShardKey("a", "a") != 0 {
		t.Error("Equal strings should compare equal")
	}

	// Int comparison
	if compareShardKey(int64(5), int64(10)) >= 0 {
		t.Error("5 should be less than 10")
	}

	// Compound key comparison
	arr1 := []interface{}{"TR", int64(5)}
	arr2 := []interface{}{"TR", int64(10)}
	if compareShardKey(arr1, arr2) >= 0 {
		t.Error("Compound [TR,5] should be less than [TR,10]")
	}
}

func TestBalancer_StartStop(t *testing.T) {
	cfg, router, cleanup := setupShardTest(t)
	defer cleanup()
	balancer := NewBalancer(cfg, router)

	if balancer.IsRunning() {
		t.Error("Should not be running initially")
	}

	ctx, cancel := context.WithCancel(context.Background())
	go balancer.Start(ctx)

	// Give it time to start
	time.Sleep(100 * time.Millisecond)
	if !balancer.IsRunning() {
		t.Error("Should be running after Start")
	}

	cancel()
	balancer.Stop()

	if balancer.IsRunning() {
		t.Error("Should not be running after Stop")
	}
}

func TestChunkRange_Contains(t *testing.T) {
	cr := ChunkRange{
		Min: int64(0),
		Max: int64(100),
	}

	if !cr.Contains(int64(50)) {
		t.Error("50 should be in range [0, 100)")
	}
	if !cr.Contains(int64(0)) {
		t.Error("0 should be in range [0, 100)")
	}
	if cr.Contains(int64(100)) {
		t.Error("100 should NOT be in range [0, 100)")
	}
	if cr.Contains(int64(-1)) {
		t.Error("-1 should NOT be in range [0, 100)")
	}
}

func TestScatterGather(t *testing.T) {
	cfg, router, cleanup := setupShardTest(t)
	defer cleanup()

	// Insert documents on different shards
	for i := int64(0); i < 9; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VInt64(i))
		doc.Set("value", bson.VString("test"))

		// Calculate which shard
		shardID := cfg.ListShards()[int(i)%len(cfg.ListShards())].ID
		eng, _ := router.GetEngine(shardID)
		key := []byte(fmt.Sprintf("testdb.docs.%d", i))
		eng.Put(key, bson.Encode(doc))
	}

	// Query all shards
	ctx := context.Background()
	filter := bson.NewDocument()
	docs, err := router.ScatterGather(ctx, "testdb.docs", filter)
	if err != nil {
		t.Fatalf("ScatterGather failed: %v", err)
	}

	if len(docs) != 9 {
		t.Errorf("Expected 9 documents, got %d", len(docs))
	}
}

func TestBalancerState(t *testing.T) {
	cfg, router, cleanup := setupShardTest(t)
	defer cleanup()
	balancer := NewBalancer(cfg, router)

	state := balancer.State()
	if !state.Enabled {
		t.Error("Should be enabled by default")
	}
	if state.Running {
		t.Error("Should not be running initially")
	}

	// Stats should be empty initially
	stats := balancer.GetStats()
	if stats.RoundsCompleted != 0 {
		t.Error("Stats should be empty initially")
	}
}

func TestRouter_HashedSharding(t *testing.T) {
	cfg, router, cleanup := setupShardTest(t)
	defer cleanup()

	// Configure hashed sharding
	cfg.SetShardKey(&ShardKey{
		Fields: []string{"email"},
		Hashed: true,
		Ns:     "testdb.users",
	})

	doc := bson.NewDocument()
	doc.Set("email", bson.VString("user@example.com"))

	shardID, err := router.RouteForWrite("testdb.users", doc)
	if err != nil {
		t.Fatalf("RouteForWrite failed: %v", err)
	}

	// Should be deterministic
	shardID2, _ := router.RouteForWrite("testdb.users", doc)
	if shardID != shardID2 {
		t.Error("Hashed routing should be deterministic")
	}

	// Range queries should scatter
	min, max := uint64(0), uint64(^uint64(0))
	shards, _ := router.RouteForRange("testdb.users", min, max)
	if len(shards) != len(cfg.ListShards()) {
		t.Error("Hashed sharding should scatter for range queries")
	}
}
