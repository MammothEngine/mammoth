package shard

import (
	"context"
	"fmt"
	"sync"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// Router handles request routing to appropriate shards.
type Router struct {
	config  *Config
	engines map[ShardID]*engine.Engine
	mu      sync.RWMutex
}

// NewRouter creates a new sharding router.
func NewRouter(cfg *Config) *Router {
	return &Router{
		config:  cfg,
		engines: make(map[ShardID]*engine.Engine),
	}
}

// ConnectShard establishes connection to a shard's engine.
func (r *Router) ConnectShard(id ShardID, eng *engine.Engine) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.engines[id] = eng
	return nil
}

// GetEngine returns the engine for a specific shard.
func (r *Router) GetEngine(id ShardID) (*engine.Engine, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	eng, ok := r.engines[id]
	if !ok {
		return nil, fmt.Errorf("shard %s not connected", id)
	}
	return eng, nil
}

// RouteForWrite determines which shard should handle a write operation.
func (r *Router) RouteForWrite(ns string, doc *bson.Document) (ShardID, error) {
	// Check if collection is sharded
	sk, ok := r.config.GetShardKey(ns)
	if !ok {
		// Not sharded - use default shard
		shards := r.config.ListShards()
		if len(shards) == 0 {
			return "", fmt.Errorf("no shards available")
		}
		return shards[0].ID, nil
	}

	// Extract shard key value
	keyVal, err := ExtractShardKey(doc, sk)
	if err != nil {
		return "", fmt.Errorf("cannot extract shard key: %w", err)
	}

	// For hashed sharding, compute hash
	if sk.Hashed {
		keyVal = hashShardKey(keyVal)
	}

	// Find chunk for this key
	chunk := r.config.FindChunkForKey(ns, keyVal)
	if chunk == nil {
		// No chunk found - might need to create initial chunk
		return r.assignToLeastLoadedShard(ns)
	}

	return chunk.Shard, nil
}

// RouteForRead determines which shards to query for a read operation.
func (r *Router) RouteForRead(ns string, filter *bson.Document) ([]ShardID, error) {
	// Check if collection is sharded
	sk, ok := r.config.GetShardKey(ns)
	if !ok {
		// Not sharded - query all shards (should be just one)
		return r.allShardIDs(), nil
	}

	// Try to extract shard key from filter
	keyVal, hasKey := ExtractShardKeyFromFilter(filter, sk)
	if !hasKey {
		// Scatter-gather: query all shards
		return r.allShardIDs(), nil
	}

	// For hashed sharding, compute hash
	if sk.Hashed {
		keyVal = hashShardKey(keyVal)
	}

	// Find specific chunk
	chunk := r.config.FindChunkForKey(ns, keyVal)
	if chunk == nil {
		// No chunk found - query all shards as fallback
		return r.allShardIDs(), nil
	}

	return []ShardID{chunk.Shard}, nil
}

// RouteForRange determines shards for a range query.
func (r *Router) RouteForRange(ns string, min, max interface{}) ([]ShardID, error) {
	// Check if collection is sharded
	sk, ok := r.config.GetShardKey(ns)
	if !ok {
		return r.allShardIDs(), nil
	}

	if sk.Hashed {
		// Hashed sharding doesn't support range queries efficiently
		return r.allShardIDs(), nil
	}

	// Find all chunks overlapping the range
	chunks := r.config.GetChunksForNamespace(ns)
	shardSet := make(map[ShardID]bool)

	for _, chunk := range chunks {
		if rangesOverlap(min, max, chunk.Min, chunk.Max) {
			shardSet[chunk.Shard] = true
		}
	}

	if len(shardSet) == 0 {
		return r.allShardIDs(), nil
	}

	result := make([]ShardID, 0, len(shardSet))
	for id := range shardSet {
		result = append(result, id)
	}
	return result, nil
}

// allShardIDs returns IDs of all known shards.
func (r *Router) allShardIDs() []ShardID {
	shards := r.config.ListShards()
	result := make([]ShardID, len(shards))
	for i, s := range shards {
		result[i] = s.ID
	}
	return result
}

// assignToLeastLoadedShard assigns to the shard with fewest chunks.
func (r *Router) assignToLeastLoadedShard(ns string) (ShardID, error) {
	shards := r.config.ListShards()
	if len(shards) == 0 {
		return "", fmt.Errorf("no shards available")
	}

	// Count chunks per shard
	chunkCounts := make(map[ShardID]int)
	for _, chunk := range r.config.GetChunksForNamespace(ns) {
		chunkCounts[chunk.Shard]++
	}

	// Find least loaded
	var bestShard ShardID
	minCount := int(^uint(0) >> 1) // MaxInt
	for _, shard := range shards {
		count := chunkCounts[shard.ID]
		if count < minCount {
			minCount = count
			bestShard = shard.ID
		}
	}

	return bestShard, nil
}

// rangesOverlap checks if two ranges overlap.
func rangesOverlap(qMin, qMax interface{}, cMin, cMax interface{}) bool {
	// Query range [qMin, qMax) overlaps with chunk [cMin, cMax)
	// Simplified: check if ranges intersect
	return compareShardKey(qMax, cMin) > 0 && compareShardKey(qMin, cMax) < 0
}

// hashShardKey computes a hash for the given shard key value.
// Uses FNV-1a 64-bit hash for good distribution.
func hashShardKey(val interface{}) uint64 {
	h := uint64(14695981039346656037) // FNV offset basis

	switch v := val.(type) {
	case string:
		for _, b := range []byte(v) {
			h ^= uint64(b)
			h *= 1099511628211 // FNV prime
		}
	case int64:
		for i := 0; i < 8; i++ {
			h ^= uint64(v >> (i * 8)) & 0xFF
			h *= 1099511628211
		}
	case float64:
		// Convert to int64 bits for hashing
		bits := int64(v)
		for i := 0; i < 8; i++ {
			h ^= uint64(bits >> (i * 8)) & 0xFF
			h *= 1099511628211
		}
	case []interface{}:
		for _, elem := range v {
			hash := hashShardKey(elem)
			h ^= hash
			h *= 1099511628211
		}
	}

	return h
}

// ShardClient provides a client interface to a specific shard.
type ShardClient struct {
	ID     ShardID
	Engine *engine.Engine
}

// QueryResult holds results from a shard query.
type QueryResult struct {
	ShardID ShardID
	Docs    []*bson.Document
	Error   error
}

// ScatterGather executes a query across multiple shards and aggregates results.
func (r *Router) ScatterGather(ctx context.Context, ns string, filter *bson.Document) ([]*bson.Document, error) {
	shards, err := r.RouteForRead(ns, filter)
	if err != nil {
		return nil, err
	}

	if len(shards) == 1 {
		// Single shard - direct query
		return r.queryShard(shards[0], ns, filter)
	}

	// Multiple shards - scatter-gather
	var wg sync.WaitGroup
	results := make(chan QueryResult, len(shards))

	for _, shardID := range shards {
		wg.Add(1)
		go func(id ShardID) {
			defer wg.Done()
			docs, err := r.queryShard(id, ns, filter)
			results <- QueryResult{ShardID: id, Docs: docs, Error: err}
		}(shardID)
	}

	// Wait and collect
	go func() {
		wg.Wait()
		close(results)
	}()

	// Aggregate results
	var allDocs []*bson.Document
	var firstError error
	for res := range results {
		if res.Error != nil && firstError == nil {
			firstError = res.Error
		}
		allDocs = append(allDocs, res.Docs...)
	}

	if firstError != nil {
		return allDocs, firstError // Return partial results with error
	}
	return allDocs, nil
}

// queryShard executes a query on a specific shard.
func (r *Router) queryShard(shardID ShardID, ns string, filter *bson.Document) ([]*bson.Document, error) {
	eng, err := r.GetEngine(shardID)
	if err != nil {
		return nil, err
	}

	// Parse namespace
	db, coll := parseNamespace(ns)

	// Use prefix scan for collection
	prefix := []byte(db + "." + coll + ".")

	var docs []*bson.Document
	eng.Scan(prefix, func(key, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true // skip corrupt
		}

		// Apply filter matching
		if filter == nil || filter.Len() == 0 {
			docs = append(docs, doc)
		} else {
			// Simple field matching
			match := true
			for _, elem := range filter.Elements() {
				if v, ok := doc.Get(elem.Key); !ok || !valuesEqual(v, elem.Value) {
					match = false
					break
				}
			}
			if match {
				docs = append(docs, doc)
			}
		}
		return true
	})

	return docs, nil
}

// parseNamespace splits "db.collection" into components.
func parseNamespace(ns string) (string, string) {
	for i := 0; i < len(ns); i++ {
		if ns[i] == '.' {
			return ns[:i], ns[i+1:]
		}
	}
	return ns, ""
}

// valuesEqual compares two BSON values for equality.
func valuesEqual(a, b bson.Value) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case bson.TypeString:
		return a.String() == b.String()
	case bson.TypeInt32:
		return a.Int32() == b.Int32()
	case bson.TypeInt64:
		return a.Int64() == b.Int64()
	case bson.TypeDouble:
		return a.Double() == b.Double()
	case bson.TypeObjectID:
		return a.ObjectID() == b.ObjectID()
	default:
		return a.String() == b.String()
	}
}

// IsSharded returns true if the namespace is sharded.
func (r *Router) IsSharded(ns string) bool {
	_, ok := r.config.GetShardKey(ns)
	return ok
}
