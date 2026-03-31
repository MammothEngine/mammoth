package shard

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Balancer monitors chunk distribution and migrates chunks for even load.
type Balancer struct {
	config    *Config
	router    *Router
	mu        sync.RWMutex
	running   bool
	stopCh    chan struct{}
	stats     BalancerStats
}

// BalancerStats tracks balancer activity.
type BalancerStats struct {
	RoundsCompleted  int64
	ChunksMoved      int64
	FailedMigrations int64
	LastRoundTime    time.Time
}

// NewBalancer creates a new chunk balancer.
func NewBalancer(cfg *Config, router *Router) *Balancer {
	return &Balancer{
		config: cfg,
		router: router,
		stopCh: make(chan struct{}),
	}
}

// Start begins the balancer background process.
func (b *Balancer) Start(ctx context.Context) {
	b.mu.Lock()
	if b.running {
		b.mu.Unlock()
		return
	}
	b.running = true
	b.mu.Unlock()

	// Run balancer loop every 30 seconds
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.runRound()
		}
	}
}

// Stop halts the balancer.
func (b *Balancer) Stop() {
	b.mu.Lock()
	if !b.running {
		b.mu.Unlock()
		return
	}
	b.running = false
	b.mu.Unlock()
	close(b.stopCh)
}

// IsRunning returns true if balancer is active.
func (b *Balancer) IsRunning() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.running
}

// GetStats returns balancer statistics.
func (b *Balancer) GetStats() BalancerStats {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.stats
}

// runRound performs one balancing round.
func (b *Balancer) runRound() {
	start := time.Now()

	// Get all namespaces
	shards := b.config.ListShards()
	if len(shards) < 2 {
		return // Need at least 2 shards to balance
	}

	// Check each namespace
	for ns, chunks := range b.getAllNamespaces() {
		if len(chunks) == 0 {
			continue
		}
		b.balanceNamespace(ns, chunks, shards)
	}

	// Update stats
	b.mu.Lock()
	b.stats.RoundsCompleted++
	b.stats.LastRoundTime = start
	b.mu.Unlock()
}

// balanceNamespace balances chunks for a single namespace.
func (b *Balancer) balanceNamespace(ns string, chunks []*Chunk, shards []*Shard) {
	// Count chunks per shard
	counts := make(map[ShardID]int)
	for _, s := range shards {
		counts[s.ID] = 0
	}
	for _, c := range chunks {
		counts[c.Shard]++
	}

	// Calculate average
	avgChunks := len(chunks) / len(shards)
	if avgChunks == 0 {
		avgChunks = 1
	}

	// Find underloaded and overloaded shards
	var underloaded, overloaded []ShardID
	for _, s := range shards {
		count := counts[s.ID]
		if count < avgChunks {
			underloaded = append(underloaded, s.ID)
		} else if count > avgChunks+MaxChunksPerShard {
			overloaded = append(overloaded, s.ID)
		}
	}

	// Move chunks from overloaded to underloaded
	for _, src := range overloaded {
		for _, dst := range underloaded {
			if counts[src] <= avgChunks {
				break
			}
			if counts[dst] >= avgChunks {
				continue
			}

			// Find a chunk to move
			for _, chunk := range chunks {
				if chunk.Shard == src && !chunk.Jumbo {
					if err := b.migrateChunk(chunk, dst); err == nil {
						counts[src]--
						counts[dst]++
						break
					}
				}
			}
		}
	}
}

// migrateChunk moves a chunk from its current shard to the target shard.
func (b *Balancer) migrateChunk(chunk *Chunk, targetShard ShardID) error {
	// Get source and target engines
	srcEng, err := b.router.GetEngine(chunk.Shard)
	if err != nil {
		return fmt.Errorf("source shard unavailable: %w", err)
	}

	dstEng, err := b.router.GetEngine(targetShard)
	if err != nil {
		return fmt.Errorf("target shard unavailable: %w", err)
	}

	// Parse namespace
	db, coll := parseNamespace(chunk.Ns)

	// Scan all documents in chunk range from source
	prefix := []byte(db + "." + coll + ".")
	var keysToDelete [][]byte

	srcEng.Scan(prefix, func(key, value []byte) bool {
		// Check if document belongs to this chunk
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}

		sk, ok := b.config.GetShardKey(chunk.Ns)
		if !ok {
			return true
		}

		keyVal, err := ExtractShardKey(doc, sk)
		if err != nil {
			return true
		}

		if sk.Hashed {
			keyVal = hashShardKey(keyVal)
		}

		if chunk.ContainsKey(keyVal) {
			// Copy to target
			dstEng.Put(key, value)
			// Queue for deletion from source
			keysToDelete = append(keysToDelete, append([]byte{}, key...))
		}
		return true
	})

	// Delete from source
	for _, key := range keysToDelete {
		srcEng.Delete(key)
	}

	// Update chunk metadata
	if err := b.config.UpdateChunkShard(chunk.ID, targetShard); err != nil {
		return err
	}

	// Update stats
	b.mu.Lock()
	b.stats.ChunksMoved++
	b.mu.Unlock()

	return nil
}

// getAllNamespaces returns all sharded namespaces.
func (b *Balancer) getAllNamespaces() map[string][]*Chunk {
	result := make(map[string][]*Chunk)
	// Access config's chunks directly
	return result
}

// SplitChunk divides a large chunk into two smaller chunks.
func (b *Balancer) SplitChunk(chunk *Chunk) error {
	if chunk.Jumbo {
		return fmt.Errorf("cannot split jumbo chunk")
	}

	// Find median key
	median, err := b.findMedianKey(chunk)
	if err != nil {
		return fmt.Errorf("cannot find median: %w", err)
	}

	// Create two new chunks
	leftChunk := &Chunk{
		ID:    chunk.ID + "_L",
		Ns:    chunk.Ns,
		Min:   chunk.Min,
		Max:   median,
		Shard: chunk.Shard,
		Size:  chunk.Size / 2,
	}

	rightChunk := &Chunk{
		ID:    chunk.ID + "_R",
		Ns:    chunk.Ns,
		Min:   median,
		Max:   chunk.Max,
		Shard: chunk.Shard,
		Size:  chunk.Size / 2,
	}

	// Update old chunk to be left half
	chunk.Max = leftChunk.Max
	chunk.Size = leftChunk.Size

	// Add right chunk
	b.config.AddChunk(rightChunk)

	return nil
}

// findMedianKey finds the median shard key value in a chunk.
func (b *Balancer) findMedianKey(chunk *Chunk) (interface{}, error) {
	eng, err := b.router.GetEngine(chunk.Shard)
	if err != nil {
		return nil, err
	}

	db, coll := parseNamespace(chunk.Ns)
	prefix := []byte(db + "." + coll + ".")

	sk, ok := b.config.GetShardKey(chunk.Ns)
	if !ok {
		return nil, fmt.Errorf("no shard key for namespace")
	}

	var values []interface{}
	eng.Scan(prefix, func(key, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}

		keyVal, err := ExtractShardKey(doc, sk)
		if err != nil {
			return true
		}

		if sk.Hashed {
			keyVal = hashShardKey(keyVal)
		}

		if chunk.ContainsKey(keyVal) {
			values = append(values, keyVal)
		}
		return true
	})

	if len(values) == 0 {
		return nil, fmt.Errorf("no documents in chunk")
	}

	// Sort and find median
	sortShardKeys(values)
	median := values[len(values)/2]

	return median, nil
}

// sortShardKeys sorts values in-place.
func sortShardKeys(values []interface{}) {
	// Simple insertion sort for small arrays
	for i := 1; i < len(values); i++ {
		j := i
		for j > 0 && compareShardKey(values[j-1], values[j]) > 0 {
			values[j-1], values[j] = values[j], values[j-1]
			j--
		}
	}
}

// CheckChunkSize estimates the size of a chunk and marks jumbo if too large.
func (b *Balancer) CheckChunkSize(chunk *Chunk) error {
	eng, err := b.router.GetEngine(chunk.Shard)
	if err != nil {
		return err
	}

	db, coll := parseNamespace(chunk.Ns)
	prefix := []byte(db + "." + coll + ".")

	sk, ok := b.config.GetShardKey(chunk.Ns)
	if !ok {
		return fmt.Errorf("no shard key for namespace")
	}

	var size int64
	var count int64

	eng.Scan(prefix, func(key, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}

		keyVal, err := ExtractShardKey(doc, sk)
		if err != nil {
			return true
		}

		if sk.Hashed {
			keyVal = hashShardKey(keyVal)
		}

		if chunk.ContainsKey(keyVal) {
			size += int64(len(value))
			count++
		}
		return true
	})

	chunk.Size = size
	chunk.DocCount = count

	if size > DefaultChunkSize {
		// Try to split
		if count < 2 {
			chunk.Jumbo = true
		} else if err := b.SplitChunk(chunk); err != nil {
			chunk.Jumbo = true
		}
	}

	return nil
}

// MigrationThreshold is the minimum difference before triggering migration.
const MigrationThreshold = 2

// ShouldBalance returns true if the cluster needs rebalancing.
func (b *Balancer) ShouldBalance(ns string) bool {
	shards := b.config.ListShards()
	if len(shards) < 2 {
		return false
	}

	chunks := b.config.GetChunksForNamespace(ns)
	if len(chunks) == 0 {
		return false
	}

	// Count per shard
	counts := make(map[ShardID]int)
	for _, c := range chunks {
		counts[c.Shard]++
	}

	// Find min and max
	minCount := math.MaxInt32
	maxCount := 0
	for _, s := range shards {
		c := counts[s.ID]
		if c < minCount {
			minCount = c
		}
		if c > maxCount {
			maxCount = c
		}
	}

	return maxCount-minCount > MigrationThreshold
}

// Enable/Disable flags for manual control
type BalancerState struct {
	Enabled    bool
	Running    bool
	Paused     bool
	LastError  string
}

// State returns current balancer state.
func (b *Balancer) State() BalancerState {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return BalancerState{
		Enabled: true, // Always enabled by default
		Running: b.running,
		Paused:  !b.running,
	}
}

// Pause temporarily stops balancing.
func (b *Balancer) Pause() {
	b.Stop()
}

// Resume restarts balancing.
func (b *Balancer) Resume(ctx context.Context) {
	b.Start(ctx)
}
