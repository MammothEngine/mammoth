package sstable

import (
	"fmt"
	"os"
	"testing"

	"github.com/mammothengine/mammoth/pkg/engine/memtable"
)

func setupBenchmarkSSTable(b *testing.B, numEntries int) (*Reader, func()) {
	dir, err := os.MkdirTemp("", "sstable-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	tablePath := dir + "/test.sst"

	// Build table
	writer, err := NewWriter(WriterOptions{
		Path:        tablePath,
		BlockSize:   4096,
		Compression: 0, // No compression for benchmark consistency
	})
	if err != nil {
		os.RemoveAll(dir)
		b.Fatal(err)
	}

	for i := range numEntries {
		key := fmt.Appendf(nil, "key_%08d", i)
		value := fmt.Appendf(nil, "value_%08d", i)
		if err := writer.Add(key, value); err != nil {
			writer.Close()
			os.RemoveAll(dir)
			b.Fatal(err)
		}
	}

	if _, err := writer.Finish(); err != nil {
		writer.Close()
		os.RemoveAll(dir)
		b.Fatal(err)
	}
	writer.Close()

	// Open reader
	reader, err := NewReader(tablePath, ReaderOptions{})
	if err != nil {
		os.RemoveAll(dir)
		b.Fatal(err)
	}

	cleanup := func() {
		reader.Close()
		os.RemoveAll(dir)
	}

	return reader, cleanup
}

func BenchmarkSSTable_Get_Small(b *testing.B) {
	reader, cleanup := setupBenchmarkSSTable(b, 1000)
	defer cleanup()

	b.ResetTimer()
	for i := range b.N {
		key := fmt.Appendf(nil, "key_%08d", i%1000)
		_, _ = reader.Get(key)
	}
}

func BenchmarkSSTable_Get_Medium(b *testing.B) {
	reader, cleanup := setupBenchmarkSSTable(b, 10000)
	defer cleanup()

	b.ResetTimer()
	for i := range b.N {
		key := fmt.Appendf(nil, "key_%08d", i%10000)
		_, _ = reader.Get(key)
	}
}

func BenchmarkSSTable_Get_Large(b *testing.B) {
	reader, cleanup := setupBenchmarkSSTable(b, 100000)
	defer cleanup()

	b.ResetTimer()
	for i := range b.N {
		key := fmt.Appendf(nil, "key_%08d", i%100000)
		_, _ = reader.Get(key)
	}
}

func BenchmarkSSTable_Iterator(b *testing.B) {
	reader, cleanup := setupBenchmarkSSTable(b, 10000)
	defer cleanup()

	b.ResetTimer()
	for range b.N {
		count := 0
		err := reader.Iter(func(key, value []byte) bool {
			count++
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
		if count != 10000 {
			b.Fatalf("expected 10000 items, got %d", count)
		}
	}
}

func BenchmarkSSTable_Build(b *testing.B) {
	dir, err := os.MkdirTemp("", "sstable-build-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	b.ResetTimer()
	for i := range b.N {
		tablePath := fmt.Sprintf("%s/test_%d.sst", dir, i)
		writer, err := NewWriter(WriterOptions{
			Path:        tablePath,
			BlockSize:   4096,
			Compression: 0,
		})
		if err != nil {
			b.Fatal(err)
		}

		for j := range 10000 {
			key := fmt.Appendf(nil, "key_%08d", j)
			value := fmt.Appendf(nil, "value_%08d", j)
			if err := writer.Add(key, value); err != nil {
				writer.Close()
				b.Fatal(err)
			}
		}

		if _, err := writer.Finish(); err != nil {
			writer.Close()
			b.Fatal(err)
		}
		writer.Close()
	}
}

func BenchmarkSSTable_BuildFromMemtable(b *testing.B) {
	dir, err := os.MkdirTemp("", "sstable-mem-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Pre-populate memtable
	mt := memtable.NewMemtable(16 * 1024 * 1024)
	for i := range 10000 {
		key := fmt.Appendf(nil, "key_%08d", i)
		value := fmt.Appendf(nil, "value_%08d", i)
		mt.Put(key, value, uint64(i))
	}

	b.ResetTimer()
	for i := range b.N {
		tablePath := fmt.Sprintf("%s/test_%d.sst", dir, i)
		writer, err := NewWriter(WriterOptions{
			Path:        tablePath,
			BlockSize:   4096,
			Compression: 0,
		})
		if err != nil {
			b.Fatal(err)
		}

		it := mt.NewIterator()
		for it.SeekToFirst(); it.Valid(); it.Next() {
			if err := writer.Add(it.Key(), it.Value()); err != nil {
				writer.Close()
				b.Fatal(err)
			}
		}

		if _, err := writer.Finish(); err != nil {
			writer.Close()
			b.Fatal(err)
		}
		writer.Close()
	}
}

func BenchmarkSSTable_MayContain(b *testing.B) {
	reader, cleanup := setupBenchmarkSSTable(b, 10000)
	defer cleanup()

	b.ResetTimer()
	for i := range b.N {
		key := fmt.Appendf(nil, "key_%08d", i%10000)
		_ = reader.MayContain(key)
	}
}
