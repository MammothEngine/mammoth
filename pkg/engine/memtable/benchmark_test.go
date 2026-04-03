package memtable

import (
	"fmt"
	"testing"
)

// largeArena is used for benchmarks that need more space
const largeArena = 64 * 1024 * 1024 // 64MB

// BenchmarkSkipList_Put measures put performance
func BenchmarkSkipList_Put(b *testing.B) {
	sl := NewSkipList(largeArena)
	value := []byte("value")

	b.ResetTimer()
	for i := range b.N {
		// Cycle through keys to stay within arena limits
		keyID := i % 500000
		k := fmt.Appendf(nil, "key_%d", keyID)
		sl.Put(k, value)
	}
}

// BenchmarkSkipList_Get measures get performance
func BenchmarkSkipList_Get(b *testing.B) {
	sl := NewSkipList(largeArena)

	// Pre-populate
	for i := range 10000 {
		k := fmt.Appendf(nil, "key_%d", i)
		v := fmt.Appendf(nil, "value_%d", i)
		sl.Put(k, v)
	}

	b.ResetTimer()
	for i := range b.N {
		k := fmt.Appendf(nil, "key_%d", i%10000)
		sl.Get(k)
	}
}

// BenchmarkSkipList_Delete measures delete performance
func BenchmarkSkipList_Delete(b *testing.B) {
	sl := NewSkipList(largeArena)

	// Pre-populate
	for i := range 10000 {
		k := fmt.Appendf(nil, "key_%d", i)
		v := fmt.Appendf(nil, "value_%d", i)
		sl.Put(k, v)
	}

	b.ResetTimer()
	for i := range b.N {
		k := fmt.Appendf(nil, "key_%d", i%10000)
		sl.Delete(k)
	}
}

// BenchmarkMemtable_Put measures memtable put performance
func BenchmarkMemtable_Put(b *testing.B) {
	mt := NewMemtable(largeArena)
	value := []byte("value")

	b.ResetTimer()
	for i := range b.N {
		keyID := i % 500000
		k := fmt.Appendf(nil, "key_%d", keyID)
		mt.Put(k, value, uint64(i))
	}
}

// BenchmarkMemtable_Get measures memtable get performance
func BenchmarkMemtable_Get(b *testing.B) {
	mt := NewMemtable(largeArena)

	// Pre-populate
	for i := range 10000 {
		k := fmt.Appendf(nil, "key_%d", i)
		v := fmt.Appendf(nil, "value_%d", i)
		mt.Put(k, v, uint64(i))
	}

	b.ResetTimer()
	for i := range b.N {
		k := fmt.Appendf(nil, "key_%d", i%10000)
		mt.Get(k)
	}
}

// BenchmarkMemtable_NewIterator measures iterator creation
func BenchmarkMemtable_NewIterator(b *testing.B) {
	mt := NewMemtable(largeArena)

	// Pre-populate
	for i := range 1000 {
		k := fmt.Appendf(nil, "key_%d", i)
		v := fmt.Appendf(nil, "value_%d", i)
		mt.Put(k, v, uint64(i))
	}

	b.ResetTimer()
	for range b.N {
		_ = mt.NewIterator()
	}
}

// BenchmarkMemtable_IterateAll measures full iteration
func BenchmarkMemtable_IterateAll(b *testing.B) {
	mt := NewMemtable(largeArena)

	// Pre-populate
	for i := range 1000 {
		k := fmt.Appendf(nil, "key_%d", i)
		v := fmt.Appendf(nil, "value_%d", i)
		mt.Put(k, v, uint64(i))
	}

	b.ResetTimer()
	for range b.N {
		count := 0
		it := mt.NewIterator()
		for it.SeekToFirst(); it.Valid(); it.Next() {
			count++
		}
		if count != 1000 {
			b.Fatalf("expected 1000 items, got %d", count)
		}
	}
}
