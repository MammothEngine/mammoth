package cache

import "sync"

const numShards = 16

type lruNode struct {
	key   uint64
	value interface{}
	prev  *lruNode
	next  *lruNode
}

type lruShard struct {
	mu       sync.RWMutex
	capacity int
	items    map[uint64]*lruNode
	head     *lruNode // most recent
	tail     *lruNode // least recent
}

type shardedLRU struct {
	shards [numShards]lruShard
	cap    int
}

func newShardedLRU(capacity int) *shardedLRU {
	c := &shardedLRU{cap: capacity}
	perShard := capacity / numShards
	if perShard < 1 {
		perShard = 1
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.capacity = perShard
		s.items = make(map[uint64]*lruNode)
		s.head = &lruNode{}
		s.tail = &lruNode{}
		s.head.next = s.tail
		s.tail.prev = s.head
	}
	return c
}

func (c *shardedLRU) getShard(key uint64) *lruShard {
	return &c.shards[key%numShards]
}

func (c *shardedLRU) Get(key uint64) (interface{}, bool) {
	s := c.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.items[key]
	if !ok {
		return nil, false
	}
	s.moveToFront(node)
	return node.value, true
}

func (c *shardedLRU) Set(key uint64, value interface{}) {
	s := c.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if node, ok := s.items[key]; ok {
		node.value = value
		s.moveToFront(node)
		return
	}

	node := &lruNode{key: key, value: value}
	s.items[key] = node
	s.pushFront(node)

	if len(s.items) > s.capacity {
		s.evict()
	}
}

func (c *shardedLRU) Delete(key uint64) {
	s := c.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if node, ok := s.items[key]; ok {
		s.removeNode(node)
		delete(s.items, key)
	}
}

func (c *shardedLRU) Len() int {
	total := 0
	for i := range c.shards {
		c.shards[i].mu.RLock()
		total += len(c.shards[i].items)
		c.shards[i].mu.RUnlock()
	}
	return total
}

func (c *shardedLRU) Clear() {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.items = make(map[uint64]*lruNode)
		s.head.next = s.tail
		s.tail.prev = s.head
		s.mu.Unlock()
	}
}

func (c *shardedLRU) Capacity() int {
	return c.cap
}

func (s *lruShard) moveToFront(node *lruNode) {
	s.removeNode(node)
	s.pushFront(node)
}

func (s *lruShard) pushFront(node *lruNode) {
	node.prev = s.head
	node.next = s.head.next
	s.head.next.prev = node
	s.head.next = node
}

func (s *lruShard) removeNode(node *lruNode) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (s *lruShard) evict() {
	tail := s.tail.prev
	if tail == s.head {
		return
	}
	s.removeNode(tail)
	delete(s.items, tail.key)
}
