package shieldcache

import (
	"fmt"
	"testing"
	"time"
)

func TestShardedLRUStoreUsesTotalCapacity(t *testing.T) {
	store := newShardedLRUStore(2, 1, 32)
	keys := keysForSameShard(t, store, 3)

	store.Set(keys[0], "one", time.Minute)
	store.Set(keys[1], "two", time.Minute)

	if store.Size() != 2 {
		t.Fatalf("expected total size 2, got %d", store.Size())
	}
	if store.Get(keys[0]) == nil || store.Get(keys[1]) == nil {
		t.Fatalf("expected one shard to hold two entries")
	}

	store.Set(keys[2], "three", time.Minute)

	if store.Size() > 2 {
		t.Fatalf("expected total size <= 2, got %d", store.Size())
	}
	if store.Get(keys[0]) != nil {
		t.Fatalf("expected least recently used item to be evicted")
	}
	if store.Get(keys[1]) == nil || store.Get(keys[2]) == nil {
		t.Fatalf("expected newer items to remain cached")
	}
}

func keysForSameShard(t *testing.T, store *shardedLRUStore, count int) []string {
	t.Helper()

	target := store.shard("key-0")
	keys := make([]string, 0, count)
	for i := 0; i < 10000 && len(keys) < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		if store.shard(key) == target {
			keys = append(keys, key)
		}
	}
	if len(keys) != count {
		t.Fatalf("found %d keys for same shard, want %d", len(keys), count)
	}
	return keys
}
