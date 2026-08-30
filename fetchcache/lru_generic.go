package shieldcache

import (
	"sync"
	"time"
)

// GenericItem[T] represents a cached item with TTL tracking and type safety
type GenericItem[T any] struct {
	value      T
	expiration time.Time
	ttl        time.Duration
}

// Value returns the cached value
func (i *GenericItem[T]) Value() T {
	return i.value
}

// Expired returns true if the item has expired
func (i *GenericItem[T]) Expired() bool {
	return time.Now().After(i.expiration)
}

// TTL returns the remaining time to live
// Returns negative value if expired
func (i *GenericItem[T]) TTL() time.Duration {
	return time.Until(i.expiration)
}

// GenericLRUCache[K, V] is a thread-safe generic LRU cache with TTL support
// K is the key type, V is the value type
type GenericLRUCache[K comparable, V any] struct {
	maxSize      int64
	itemsToPrune uint32
	mu           sync.RWMutex
	items        map[K]*genericCacheNode[K, V]
	head         *genericCacheNode[K, V]
	tail         *genericCacheNode[K, V]
	size         int64
}

type genericCacheNode[K comparable, V any] struct {
	key  K
	item *GenericItem[V]
	prev *genericCacheNode[K, V]
	next *genericCacheNode[K, V]
}

// NewGenericLRUCache creates a new generic LRU cache with the given configuration
func NewGenericLRUCache[K comparable, V any](maxSize int64, itemsToPrune uint32) *GenericLRUCache[K, V] {
	if maxSize < 0 {
		maxSize = 0
	}
	if itemsToPrune == 0 {
		itemsToPrune = 1
	}
	return &GenericLRUCache[K, V]{
		maxSize:      maxSize,
		itemsToPrune: itemsToPrune,
		items:        make(map[K]*genericCacheNode[K, V]),
		head:         nil,
		tail:         nil,
		size:         0,
	}
}

// Get retrieves an item from the cache
func (c *GenericLRUCache[K, V]) Get(key K) *GenericItem[V] {
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

// GetValue retrieves just the value from the cache, returning zero value if not found
func (c *GenericLRUCache[K, V]) GetValue(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}

	// Move to front (most recently used)
	c.moveToFront(node)

	return node.item.value, true
}

// Set stores an item in the cache with the given TTL
func (c *GenericLRUCache[K, V]) Set(key K, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	item := &GenericItem[V]{
		value:      value,
		expiration: now.Add(ttl),
		ttl:        ttl,
	}

	// If key already exists, update it
	if node, ok := c.items[key]; ok {
		node.item = item
		c.moveToFront(node)
		return
	}

	// Add new item
	node := &genericCacheNode[K, V]{
		key:  key,
		item: item,
	}

	c.items[key] = node
	c.addToFront(node)
	c.size++

	// Prune if necessary
	if c.size > c.maxSize {
		c.prune()
	}
}

// Delete removes an item from the cache
func (c *GenericLRUCache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.items[key]
	if !ok {
		return
	}

	c.removeNode(node)
	delete(c.items, key)
	c.size--
}

// Clear removes all items from the cache
func (c *GenericLRUCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[K]*genericCacheNode[K, V])
	c.head = nil
	c.tail = nil
	c.size = 0
}

// Size returns the current number of items in the cache
func (c *GenericLRUCache[K, V]) Size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.size
}

// moveToFront moves a node to the front of the list (most recently used)
func (c *GenericLRUCache[K, V]) moveToFront(node *genericCacheNode[K, V]) {
	if c.head == node {
		return
	}
	c.removeNode(node)
	c.addToFront(node)
}

// addToFront adds a node to the front of the list
func (c *GenericLRUCache[K, V]) addToFront(node *genericCacheNode[K, V]) {
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
func (c *GenericLRUCache[K, V]) removeNode(node *genericCacheNode[K, V]) {
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
func (c *GenericLRUCache[K, V]) prune() {
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
}
