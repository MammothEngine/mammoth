package cache

import (
	"sync"
	"testing"
)

func TestCacheBasicGetSet(t *testing.T) {
	c := NewCache(100)
	c.Set(1, "one")
	v, ok := c.Get(1)
	if !ok || v.(string) != "one" {
		t.Fatalf("expected one, got %v, ok=%v", v, ok)
	}
}

func TestCacheMissingKey(t *testing.T) {
	c := NewCache(100)
	_, ok := c.Get(999)
	if ok {
		t.Fatal("expected missing")
	}
}

func TestCacheUpdate(t *testing.T) {
	c := NewCache(100)
	c.Set(1, "old")
	c.Set(1, "new")
	v, ok := c.Get(1)
	if !ok || v.(string) != "new" {
		t.Fatalf("expected new, got %v", v)
	}
}

func TestCacheEviction(t *testing.T) {
	c := NewCache(32)

	// Fill cache beyond capacity
	for i := 0; i < 64; i++ {
		c.Set(uint64(i), i)
	}

	if c.Len() > 32 {
		t.Fatalf("cache should not exceed capacity: %d", c.Len())
	}

	// First items should be evicted
	_, ok := c.Get(0)
	if ok {
		t.Fatal("key 0 should be evicted")
	}

	// Recent items should exist
	v, ok := c.Get(63)
	if !ok || v.(int) != 63 {
		t.Fatal("key 63 should exist")
	}
}

func TestCacheDelete(t *testing.T) {
	c := NewCache(100)
	c.Set(1, "one")
	c.Delete(1)
	_, ok := c.Get(1)
	if ok {
		t.Fatal("key should be deleted")
	}
}

func TestCacheDeleteNonExistent(t *testing.T) {
	c := NewCache(100)
	c.Delete(999) // Should not panic
}

func TestCacheClear(t *testing.T) {
	c := NewCache(100)
	for i := 0; i < 50; i++ {
		c.Set(uint64(i), i)
	}
	c.Clear()
	if c.Len() != 0 {
		t.Fatalf("expected 0, got %d", c.Len())
	}
}

func TestCacheCapacity(t *testing.T) {
	c := NewCache(256)
	if c.Capacity() != 256 {
		t.Fatalf("expected 256, got %d", c.Capacity())
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	c := NewCache(1000)
	var wg sync.WaitGroup

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				key := uint64(base*1000 + i)
				c.Set(key, key)
				c.Get(key)
			}
		}(g)
	}
	wg.Wait()
}

func TestCacheLRUOrder(t *testing.T) {
	// Use keys that map to the same shard (key % 16)
	// Keys 0, 16, 32 all map to shard 0
	c := NewCache(48) // 3 per shard

	c.Set(0, "a")
	c.Set(16, "b")
	c.Set(32, "c")

	// Access 0 to make it recent
	c.Get(0)

	// Insert 48 (same shard), should evict 16 (least recently used in shard)
	c.Set(48, "d")

	_, ok := c.Get(16)
	if ok {
		t.Fatal("key 16 should be evicted")
	}

	_, ok = c.Get(0)
	if !ok {
		t.Fatal("key 0 should still exist")
	}
}
