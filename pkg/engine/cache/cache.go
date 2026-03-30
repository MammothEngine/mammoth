package cache

// Cache is a generic cache interface.
type Cache interface {
	Get(key uint64) (value interface{}, ok bool)
	Set(key uint64, value interface{})
	Delete(key uint64)
	Len() int
	Clear()
	Capacity() int
}

// NewCache creates a new sharded LRU cache with the given capacity.
func NewCache(capacity int) Cache {
	return newShardedLRU(capacity)
}
