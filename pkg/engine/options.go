package engine

import (
	"time"

	"github.com/mammothengine/mammoth/pkg/engine/cache"
	"github.com/mammothengine/mammoth/pkg/engine/compression"
	"github.com/mammothengine/mammoth/pkg/engine/wal"
)

// Options configures the storage engine.
type Options struct {
	// Directory for all data
	Dir string

	// WAL options
	WALSyncMode      wal.SyncMode
	WALMaxSegmentSize int64

	// Memtable options
	MemtableSize int // Max memtable size in bytes before rotation (default: 4MB)

	// SSTable options
	BlockSize      int
	Compression    compression.CompressionType
	BlockCacheSize int // Block cache capacity (default: 1000)

	// Compaction options
	L0CompactionTrigger int // Number of L0 files to trigger compaction (default: 4)

	// Background sync
	SyncInterval time.Duration
}

// DefaultOptions returns sensible defaults.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:                 dir,
		WALSyncMode:         wal.SyncFull,
		WALMaxSegmentSize:   64 * 1024 * 1024,
		MemtableSize:        4 * 1024 * 1024,
		BlockSize:           4096,
		Compression:         compression.CompressionSnappy,
		BlockCacheSize:      1000,
		L0CompactionTrigger: 4,
	}
}

func (o Options) cache() cache.Cache {
	cap := o.BlockCacheSize
	if cap <= 0 {
		cap = 1000
	}
	return cache.NewCache(cap)
}
