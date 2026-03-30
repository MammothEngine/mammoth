package memtable

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
)

func TestSkipListBasicPutGet(t *testing.T) {
	sl := NewSkipList(1 << 20)

	sl.Put([]byte("apple"), []byte("red"))
	sl.Put([]byte("banana"), []byte("yellow"))
	sl.Put([]byte("cherry"), []byte("red"))

	val, ok := sl.Get([]byte("apple"))
	if !ok || !bytes.Equal(val, []byte("red")) {
		t.Fatalf("expected (red, true), got (%q, %v)", val, ok)
	}

	val, ok = sl.Get([]byte("banana"))
	if !ok || !bytes.Equal(val, []byte("yellow")) {
		t.Fatalf("expected (yellow, true), got (%q, %v)", val, ok)
	}

	val, ok = sl.Get([]byte("cherry"))
	if !ok || !bytes.Equal(val, []byte("red")) {
		t.Fatalf("expected (red, true), got (%q, %v)", val, ok)
	}

	_, ok = sl.Get([]byte("durian"))
	if ok {
		t.Fatal("expected key 'durian' to not exist")
	}
}

func TestSkipListUpdate(t *testing.T) {
	sl := NewSkipList(1 << 20)

	sl.Put([]byte("key"), []byte("v1"))
	val, ok := sl.Get([]byte("key"))
	if !ok || !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("expected v1, got %q", val)
	}

	sl.Put([]byte("key"), []byte("v2"))
	val, ok = sl.Get([]byte("key"))
	if !ok || !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("expected v2, got %q", val)
	}
}

func TestSkipListDelete(t *testing.T) {
	sl := NewSkipList(1 << 20)

	sl.Put([]byte("key"), []byte("value"))
	_, ok := sl.Get([]byte("key"))
	if !ok {
		t.Fatal("expected key to exist")
	}

	sl.Delete([]byte("key"))
	_, ok = sl.Get([]byte("key"))
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestSkipListEmptyKeyAndValue(t *testing.T) {
	sl := NewSkipList(1 << 20)

	// Empty key should work.
	sl.Put([]byte{}, []byte("val"))
	val, ok := sl.Get([]byte{})
	if !ok || !bytes.Equal(val, []byte("val")) {
		t.Fatalf("expected (val, true), got (%q, %v)", val, ok)
	}

	// Empty value should work.
	sl.Put([]byte("key2"), []byte{})
	val, ok = sl.Get([]byte("key2"))
	if !ok || !bytes.Equal(val, []byte{}) {
		t.Fatalf("expected empty value, got %q", val)
	}
}

func TestSkipList100KInsertGet(t *testing.T) {
	sl := NewSkipList(64 << 20) // 64 MB arena

	const n = 100_000
	keys := make([][]byte, n)
	vals := make([][]byte, n)

	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key_%08d", i))
		vals[i] = []byte(fmt.Sprintf("val_%08d", i))
	}

	// Shuffle for insert order.
	shuffled := make([]int, n)
	for i := range shuffled {
		shuffled[i] = i
	}
	rand.Shuffle(n, func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	for _, idx := range shuffled {
		sl.Put(keys[idx], vals[idx])
	}

	// Verify all keys.
	for i := 0; i < n; i++ {
		val, ok := sl.Get(keys[i])
		if !ok {
			t.Fatalf("key %q not found", keys[i])
		}
		if !bytes.Equal(val, vals[i]) {
			t.Fatalf("key %q: expected %q, got %q", keys[i], vals[i], val)
		}
	}

	t.Logf("Skip list height after 100K inserts: %d", sl.Height())
	t.Logf("Approximate size: %d bytes", sl.ApproximateSize())
}

func TestSkipListSortedIteration(t *testing.T) {
	sl := NewSkipList(1 << 20)

	keys := []string{"delta", "alpha", "charlie", "bravo", "echo"}
	for _, k := range keys {
		sl.Put([]byte(k), []byte("val_"+k))
	}

	// Collect all keys via forward iteration.
	var collected []string
	it := sl.NewIterator()
	it.SeekToFirst()
	for it.Valid() {
		collected = append(collected, string(it.Key()))
		it.Next()
	}

	sorted := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	if len(collected) != len(sorted) {
		t.Fatalf("expected %d keys, got %d", len(sorted), len(collected))
	}
	for i, k := range sorted {
		if collected[i] != k {
			t.Fatalf("at position %d: expected %q, got %q", i, k, collected[i])
		}
	}
}

func TestSkipListConcurrentAccess(t *testing.T) {
	sl := NewSkipList(64 << 20)

	const numGoroutines = 8
	const keysPerGoroutine = 10_000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			prefix := fmt.Sprintf("g%d_", goroutineID)
			for i := 0; i < keysPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("%skey_%06d", prefix, i))
				val := []byte(fmt.Sprintf("%sval_%06d", prefix, i))
				sl.Put(key, val)
			}
		}(g)
	}

	wg.Wait()

	// Verify all goroutines' keys.
	var totalFound int
	for g := 0; g < numGoroutines; g++ {
		prefix := fmt.Sprintf("g%d_", g)
		for i := 0; i < keysPerGoroutine; i++ {
			key := []byte(fmt.Sprintf("%skey_%06d", prefix, i))
			expected := []byte(fmt.Sprintf("%sval_%06d", prefix, i))
			val, ok := sl.Get(key)
			if !ok {
				t.Fatalf("key %q not found", key)
			}
			if !bytes.Equal(val, expected) {
				t.Fatalf("key %q: expected %q, got %q", key, expected, val)
			}
			totalFound++
		}
	}

	expectedTotal := numGoroutines * keysPerGoroutine
	if totalFound != expectedTotal {
		t.Fatalf("expected %d keys, found %d", expectedTotal, totalFound)
	}

	t.Logf("Concurrent test: %d keys from %d goroutines", totalFound, numGoroutines)
}

func TestSkipListConcurrentReadsDuringWrites(t *testing.T) {
	sl := NewSkipList(64 << 20)

	// Pre-populate.
	const n = 10_000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key_%06d", i))
		val := []byte(fmt.Sprintf("val_%06d", i))
		sl.Put(key, val)
	}

	var wg sync.WaitGroup
	const readers = 4
	const writers = 2

	// Writers update keys.
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				key := []byte(fmt.Sprintf("key_%06d", i%n))
				val := []byte(fmt.Sprintf("w%d_val_%06d", wid, i))
				sl.Put(key, val)
			}
		}(w)
	}

	// Readers read keys.
	wg.Add(readers)
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				key := []byte(fmt.Sprintf("key_%06d", i%n))
				sl.Get(key) // Just ensure no crash/panic.
			}
		}()
	}

	wg.Wait()
}

func TestSkipListHeight(t *testing.T) {
	sl := NewSkipList(1 << 20)
	if sl.Height() < 1 {
		t.Fatal("height should start at 1")
	}

	// Insert enough keys to grow the height.
	for i := 0; i < 1000; i++ {
		sl.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("v"))
	}

	if sl.Height() < 1 {
		t.Fatal("height should be at least 1 after inserts")
	}
}

func TestSkipListLexicographicOrdering(t *testing.T) {
	sl := NewSkipList(1 << 20)

	keys := []string{"z", "a", "m", "b", "y", "c"}
	for _, k := range keys {
		sl.Put([]byte(k), []byte(k))
	}

	// Collect in order.
	var result []string
	it := sl.NewIterator()
	it.SeekToFirst()
	for it.Valid() {
		result = append(result, string(it.Key()))
		it.Next()
	}

	sort.Strings(keys)
	for i, k := range keys {
		if result[i] != k {
			t.Fatalf("position %d: expected %q, got %q", i, k, result[i])
		}
	}
}
