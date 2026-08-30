package shieldcache

import (
	"sync"
	"time"
)

// Item represents a cached item with TTL tracking
type Item struct {
	value      interface{}
	expiration time.Time
	ttl        time.Duration
}

// Value returns the cached value
func (i *Item) Value() interface{} {
	return i.value
}

// Expired returns true if the item has expired
func (i *Item) Expired() bool {
	return time.Now().After(i.expiration)
}

// TTL returns the remaining time to live
// Returns negative value if expired
func (i *Item) TTL() time.Duration {
	return time.Until(i.expiration)
}

// Cache is a simple thread-safe LRU cache with TTL support
type LRUCache struct {
	maxSize      int64
	itemsToPrune uint32
	mu           sync.RWMutex
	items        map[string]*cacheNode
	head         *cacheNode
	tail         *cacheNode
	size         int64
}

type cacheNode struct {
	key  string
	item *Item
	prev *cacheNode
	next *cacheNode
}

// NewLRUCache creates a new LRU cache with the given configuration
func NewLRUCache(maxSize int64, itemsToPrune uint32) *LRUCache {
	if maxSize < 0 {
		maxSize = 0
	}
	if itemsToPrune == 0 {
		itemsToPrune = 1
	}
	return &LRUCache{
		maxSize:      maxSize,
		itemsToPrune: itemsToPrune,
		items:        make(map[string]*cacheNode),
		head:         nil,
		tail:         nil,
		size:         0,
	}
}

// Get retrieves an item from the cache
func (c *LRUCache) Get(key string) *Item {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.items[key]
	if !ok {
		return nil
	}

	// Move to front (most recently used)
	c.moveToFront(node)

	return node.item
}

// Set stores an item in the cache with the given TTL
func (c *LRUCache) Set(key string, value interface{}, ttl time.Duration) {
	if c.setNoPrune(key, value, ttl) {
		c.pruneOverCapacity()
	}
}

func (c *LRUCache) setNoPrune(key string, value interface{}, ttl time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	item := &Item{
		value:      value,
		expiration: now.Add(ttl),
		ttl:        ttl,
	}

	// If key already exists, update it
	if node, ok := c.items[key]; ok {
		node.item = item
		c.moveToFront(node)
		return false
	}

	// Add new item
	node := &cacheNode{
		key:  key,
		item: item,
	}

	c.items[key] = node
	c.addToFront(node)
	c.size++

	return true
}

func (c *LRUCache) pruneOverCapacity() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.size > c.maxSize {
		return c.pruneLocked()
	}
	return 0
}

// Delete removes an item from the cache
func (c *LRUCache) Delete(key string) {
	c.delete(key)
}

func (c *LRUCache) delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.items[key]
	if !ok {
		return false
	}

	c.removeNode(node)
	delete(c.items, key)
	c.size--
	return true
}

// Size returns the current number of items in the cache.
func (c *LRUCache) Size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.size
}

// moveToFront moves a node to the front of the list (most recently used)
func (c *LRUCache) moveToFront(node *cacheNode) {
	if c.head == node {
		return
	}
	c.removeNode(node)
	c.addToFront(node)
}

// addToFront adds a node to the front of the list
func (c *LRUCache) addToFront(node *cacheNode) {
	node.prev = nil
	node.next = c.head

	if c.head != nil {
		c.head.prev = node
	}
	c.head = node

	if c.tail == nil {
		c.tail = node
	}
}

// removeNode removes a node from the list
func (c *LRUCache) removeNode(node *cacheNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		c.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		c.tail = node.prev
	}
}

// prune removes the least recently used items
func (c *LRUCache) prune() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pruneLocked()
}

func (c *LRUCache) pruneLocked() int64 {
	count := 0
	node := c.tail
	for node != nil && count < int(c.itemsToPrune) {
		prevNode := node.prev
		c.removeNode(node)
		delete(c.items, node.key)
		c.size--
		node = prevNode
		count++
	}
	return int64(count)
}
