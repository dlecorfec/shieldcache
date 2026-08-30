package shieldcache

import (
	"testing"
	"time"
)

// BenchmarkLRUGet benchmarks Get operations on the new LRU cache
func BenchmarkLRUGet(b *testing.B) {
	cache := NewLRUCache(5000, 100)
	cache.Set("test_key", "test_value", 60*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get("test_key")
	}
}

// BenchmarkLRUSet benchmarks Set operations on the new LRU cache
func BenchmarkLRUSet(b *testing.B) {
	cache := NewLRUCache(5000, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set("key", "value", 60*time.Second)
	}
}

// BenchmarkLRUSetGet benchmarks mixed Set/Get operations on the new LRU cache
func BenchmarkLRUSetGet(b *testing.B) {
	cache := NewLRUCache(5000, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set("key", "value", 60*time.Second)
		cache.Get("key")
	}
}

// BenchmarkLRUHighContention benchmarks the new LRU cache under high concurrent load
func BenchmarkLRUHighContention(b *testing.B) {
	cache := NewLRUCache(5000, 100)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key_" + string(rune(i%100))
			if i%2 == 0 {
				cache.Set(key, i, 60*time.Second)
			} else {
				cache.Get(key)
			}
			i++
		}
	})
}
