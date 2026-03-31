// Package shard implements server-side horizontal sharding.
//
// Architecture:
//   - Config Server: Manages shard topology, chunk ranges, metadata
//   - Query Router: Routes requests to appropriate shard based on shard key
//   - Chunk: Contiguous range of shard key values stored on one shard
//   - Balancer: Monitors chunk distribution and migrates chunks as needed
//
// Sharding is configured per collection using shard keys.
// The router extracts the shard key from documents and queries,
// then forwards requests to the appropriate shard.
package shard

import (
	"fmt"
	"sync"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// ShardID uniquely identifies a shard.
type ShardID string

// ChunkRange defines the boundaries of a chunk.
type ChunkRange struct {
	Min interface{} // inclusive lower bound
	Max interface{} // exclusive upper bound
}

// Contains returns true if value falls within this chunk's range.
func (cr ChunkRange) Contains(val interface{}) bool {
	return compareShardKey(val, cr.Min) >= 0 && compareShardKey(val, cr.Max) < 0
}

// Chunk represents a data partition assigned to a shard.
type Chunk struct {
	ID       string      `json:"_id"`
	Ns       string      `json:"ns"`       // namespace: db.collection
	Min      interface{} `json:"min"`      // inclusive lower bound
	Max      interface{} `json:"max"`      // exclusive upper bound
	Shard    ShardID     `json:"shard"`    // assigned shard
	Jumbo    bool        `json:"jumbo"`    // too large to split
	Size     int64       `json:"size"`     // estimated size in bytes
	DocCount int64       `json:"docCount"` // estimated document count
}

// ContainsKey returns true if the given key falls within this chunk's range.
func (c *Chunk) ContainsKey(key interface{}) bool {
	return compareShardKey(key, c.Min) >= 0 && compareShardKey(key, c.Max) < 0
}

// Shard represents a single shard in the cluster.
type Shard struct {
	ID       ShardID `json:"_id"`
	Host     string  `json:"host"`     // connection string
	State    string  `json:"state"`    // "active", "inactive", "draining"
	Version  int64   `json:"version"`  // config version
	MaxSize  int64   `json:"maxSize"`  // max storage in bytes
	CurrSize int64   `json:"currSize"` // current storage
}

// Config holds sharding configuration for the cluster.
type Config struct {
	mu       sync.RWMutex
	shards   map[ShardID]*Shard
	chunks   map[string][]*Chunk // namespace -> chunks
	versions map[string]int64    // namespace -> version
	keyCache map[string]*ShardKey // namespace -> shard key
}

// ShardKey defines how a collection is sharded.
type ShardKey struct {
	Fields    []string `json:"fields"`    // compound shard key fields
	Hashed    bool     `json:"hashed"`    // use hash-based sharding
	Unique    bool     `json:"unique"`    // enforce unique shard key
	Ns        string   `json:"ns"`        // namespace
}

// NewConfig creates an empty sharding configuration.
func NewConfig() *Config {
	return &Config{
		shards:   make(map[ShardID]*Shard),
		chunks:   make(map[string][]*Chunk),
		versions: make(map[string]int64),
		keyCache: make(map[string]*ShardKey),
	}
}

// AddShard registers a new shard in the cluster.
func (c *Config) AddShard(s *Shard) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.shards[s.ID]; exists {
		return fmt.Errorf("shard %s already exists", s.ID)
	}
	c.shards[s.ID] = s
	return nil
}

// RemoveShard removes a shard from the cluster.
func (c *Config) RemoveShard(id ShardID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.shards[id]; !exists {
		return fmt.Errorf("shard %s not found", id)
	}
	delete(c.shards, id)
	return nil
}

// GetShard returns shard by ID.
func (c *Config) GetShard(id ShardID) (*Shard, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.shards[id]
	return s, ok
}

// ListShards returns all shards.
func (c *Config) ListShards() []*Shard {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*Shard, 0, len(c.shards))
	for _, s := range c.shards {
		result = append(result, s)
	}
	return result
}

// SetShardKey configures sharding for a collection.
func (c *Config) SetShardKey(sk *ShardKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keyCache[sk.Ns] = sk
}

// GetShardKey returns the shard key for a namespace.
func (c *Config) GetShardKey(ns string) (*ShardKey, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sk, ok := c.keyCache[ns]
	return sk, ok
}

// AddChunk adds a chunk to the configuration.
func (c *Config) AddChunk(chunk *Chunk) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.chunks[chunk.Ns] = append(c.chunks[chunk.Ns], chunk)
	c.versions[chunk.Ns]++
}

// FindChunkForKey locates the chunk containing the given shard key value.
func (c *Config) FindChunkForKey(ns string, keyVal interface{}) *Chunk {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, chunk := range c.chunks[ns] {
		if chunk.ContainsKey(keyVal) {
			return chunk
		}
	}
	return nil
}

// GetChunksForNamespace returns all chunks for a namespace.
func (c *Config) GetChunksForNamespace(ns string) []*Chunk {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.chunks[ns]
}

// UpdateChunkShard moves a chunk to a different shard.
func (c *Config) UpdateChunkShard(chunkID string, newShard ShardID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for ns, chunks := range c.chunks {
		for _, ch := range chunks {
			if ch.ID == chunkID {
				ch.Shard = newShard
				c.versions[ns]++
				return nil
			}
		}
	}
	return fmt.Errorf("chunk %s not found", chunkID)
}

// ExtractShardKey extracts the shard key value from a document.
func ExtractShardKey(doc *bson.Document, sk *ShardKey) (interface{}, error) {
	if len(sk.Fields) == 0 {
		return nil, fmt.Errorf("empty shard key")
	}

	// For compound keys, return array of values
	if len(sk.Fields) > 1 {
		values := make([]interface{}, len(sk.Fields))
		for i, field := range sk.Fields {
			v, ok := doc.Get(field)
			if !ok {
				return nil, fmt.Errorf("missing shard key field: %s", field)
			}
			values[i] = bsonValueToComparable(v)
		}
		return values, nil
	}

	// Single field
	v, ok := doc.Get(sk.Fields[0])
	if !ok {
		return nil, fmt.Errorf("missing shard key field: %s", sk.Fields[0])
	}
	return bsonValueToComparable(v), nil
}

// ExtractShardKeyFromFilter extracts shard key from query filter.
func ExtractShardKeyFromFilter(filter *bson.Document, sk *ShardKey) (interface{}, bool) {
	for _, field := range sk.Fields {
		if v, ok := filter.Get(field); ok {
			return bsonValueToComparable(v), true
		}
		// Check for equality operators
		if v, ok := filter.Get(field); ok && v.Type == bson.TypeDocument {
			doc := v.DocumentValue()
			if eqVal, ok := doc.Get("$eq"); ok {
				return bsonValueToComparable(eqVal), true
			}
		}
	}
	return nil, false
}

// bsonValueToComparable converts a BSON value to a Go comparable.
func bsonValueToComparable(v bson.Value) interface{} {
	switch v.Type {
	case bson.TypeString:
		return v.String()
	case bson.TypeInt32:
		return int64(v.Int32())
	case bson.TypeInt64:
		return v.Int64()
	case bson.TypeDouble:
		return v.Double()
	case bson.TypeObjectID:
		return v.ObjectID().String()
	case bson.TypeBinary:
		return v.Binary()
	default:
		return v.String() // fallback
	}
}

// compareShardKey compares two shard key values.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func compareShardKey(a, b interface{}) int {
	// Handle compound keys (arrays)
	aArr, aIsArr := a.([]interface{})
	bArr, bIsArr := b.([]interface{})
	if aIsArr && bIsArr {
		for i := 0; i < len(aArr) && i < len(bArr); i++ {
			cmp := compareShardKey(aArr[i], bArr[i])
			if cmp != 0 {
				return cmp
			}
		}
		return len(aArr) - len(bArr)
	}

	// Single value comparison
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// DefaultChunkSize is the target size for chunks (64MB).
const DefaultChunkSize = 64 * 1024 * 1024

// MinChunkSize is the minimum size to consider for splitting (25% of target).
const MinChunkSize = DefaultChunkSize / 4

// MaxChunksPerShard is the maximum imbalance before rebalancing.
const MaxChunksPerShard = 2
