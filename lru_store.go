package shieldcache

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

type lruStore interface {
	Get(key string) *Item
	Set(key string, value interface{}, ttl time.Duration)
	Delete(key string)
	Size() int64
}

func newLRUStore(maxSize int64, itemsToPrune uint32, shards int) lruStore {
	if shards <= 1 {
		return NewLRUCache(maxSize, itemsToPrune)
	}
	return newShardedLRUStore(maxSize, itemsToPrune, shards)
}

type shardedLRUStore struct {
	mu      sync.Mutex
	shards  []*LRUCache
	seed    maphash.Seed
	maxSize int64
	total   int64
}

func newShardedLRUStore(maxSize int64, itemsToPrune uint32, shardCount int) *shardedLRUStore {
	if maxSize < 0 {
		maxSize = 0
	}
	if itemsToPrune == 0 {
		itemsToPrune = 1
	}

	store := &shardedLRUStore{
		shards:  make([]*LRUCache, shardCount),
		seed:    maphash.MakeSeed(),
		maxSize: maxSize,
	}
	for i := range store.shards {
		store.shards[i] = NewLRUCache(maxSize, itemsToPrune)
	}
	return store
}

func (s *shardedLRUStore) Get(key string) *Item {
	return s.shard(key).Get(key)
}

func (s *shardedLRUStore) Set(key string, value interface{}, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	shard := s.shard(key)
	if shard.setNoPrune(key, value, ttl) {
		atomic.AddInt64(&s.total, 1)
		s.pruneOverCapacity(shard)
	}
}

func (s *shardedLRUStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.shard(key).delete(key) {
		atomic.AddInt64(&s.total, -1)
	}
}

func (s *shardedLRUStore) Size() int64 {
	return atomic.LoadInt64(&s.total)
}

func (s *shardedLRUStore) shard(key string) *LRUCache {
	var hash maphash.Hash
	hash.SetSeed(s.seed)
	_, _ = hash.WriteString(key)
	return s.shards[hash.Sum64()%uint64(len(s.shards))]
}

func (s *shardedLRUStore) pruneOverCapacity(first *LRUCache) {
	for atomic.LoadInt64(&s.total) > s.maxSize {
		removed := first.prune()
		if removed == 0 {
			removed = s.pruneOtherShards(first)
		}
		if removed == 0 {
			return
		}
		atomic.AddInt64(&s.total, -removed)
	}
}

func (s *shardedLRUStore) pruneOtherShards(skip *LRUCache) int64 {
	for _, shard := range s.shards {
		if shard == skip {
			continue
		}
		if removed := shard.prune(); removed > 0 {
			return removed
		}
	}
	return 0
}
