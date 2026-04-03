package engine

import (
	"fmt"
	"os"
	"testing"

	"github.com/mammothengine/mammoth/pkg/engine/wal"
)

func setupBenchmarkEngine(b *testing.B) (*Engine, func()) {
	dir, err := os.MkdirTemp("", "mammoth-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	opts := DefaultOptions(dir)

	eng, err := Open(opts)
	if err != nil {
		os.RemoveAll(dir)
		b.Fatal(err)
	}

	cleanup := func() {
		eng.Close()
		os.RemoveAll(dir)
	}

	return eng, cleanup
}

func BenchmarkEngine_Put(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b)
	defer cleanup()

	value := []byte("benchmark value data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key_%d", i)
		err := eng.Put(key, value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEngine_Get(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b)
	defer cleanup()

	// Pre-populate with data
	value := []byte("benchmark value data")
	for i := 0; i < 1000; i++ {
		key := fmt.Appendf(nil, "key_%d", i)
		eng.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key_%d", i%1000)
		_, _ = eng.Get(key)
	}
}

func BenchmarkEngine_PutGet(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b)
	defer cleanup()

	value := []byte("benchmark value data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key_%d", i)
		eng.Put(key, value)
		_, _ = eng.Get(key)
	}
}

func BenchmarkEngine_Delete(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b)
	defer cleanup()

	value := []byte("benchmark value data")

	// Pre-populate
	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key_%d", i)
		eng.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key_%d", i)
		eng.Delete(key)
	}
}

func BenchmarkEngine_BatchPut(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b)
	defer cleanup()

	batchSizes := []int{10, 100}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			value := []byte("benchmark value data")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := eng.NewBatch()
				for j := 0; j < size; j++ {
					key := fmt.Appendf(nil, "key_%d_%d", i, j)
					batch.Put(key, value)
				}
				err := batch.Commit()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEngine_Scan(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b)
	defer cleanup()

	// Pre-populate with data
	value := []byte("benchmark value data")
	for i := 0; i < 10000; i++ {
		key := fmt.Appendf(nil, "key_%010d", i)
		eng.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		eng.Scan([]byte("key_"), func(_, _ []byte) bool {
			count++
			return count < 1000 // Limit scan to avoid growing test time
		})
	}
}

func BenchmarkWAL_WriteAsync(b *testing.B) {
	dir, err := os.MkdirTemp("", "mammoth-wal-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions(dir)
	opts.WALSyncMode = wal.SyncNone // Async mode for performance

	eng, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	value := []byte("wal benchmark value data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "wal_key_%d", i)
		eng.Put(key, value)
	}
}

func BenchmarkWAL_WriteSync(b *testing.B) {
	dir, err := os.MkdirTemp("", "mammoth-wal-sync-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions(dir)
	opts.WALSyncMode = wal.SyncFull // Sync mode for durability

	eng, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	value := []byte("wal benchmark value data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "wal_key_%d", i)
		eng.Put(key, value)
	}
}
