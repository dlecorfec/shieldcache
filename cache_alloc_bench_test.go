package shieldcache

import (
	"context"
	"testing"
	"time"
)

func cacheAllocFetcher() (interface{}, bool, error) {
	return "value", true, nil
}

func cacheAllocContextFetcher(context.Context) (interface{}, bool, error) {
	return "value", true, nil
}

func BenchmarkCacheHitAlloc(b *testing.B) {
	b.Run("FetchInline", func(b *testing.B) {
		cache := newPreloadedBenchmarkCache(b)
		defer cache.Close()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := cache.Fetch("key", func() (interface{}, bool, error) {
				return "value", true, nil
			})
			if err != nil {
				b.Fatalf("fetch failed: %v", err)
			}
		}
	})

	b.Run("FetchPackageFunc", func(b *testing.B) {
		cache := newPreloadedBenchmarkCache(b)
		defer cache.Close()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := cache.Fetch("key", cacheAllocFetcher)
			if err != nil {
				b.Fatalf("fetch failed: %v", err)
			}
		}
	})

	b.Run("FetchContextPackageFunc", func(b *testing.B) {
		cache := newPreloadedBenchmarkCache(b)
		defer cache.Close()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := cache.FetchContext(context.Background(), "key", cacheAllocContextFetcher)
			if err != nil {
				b.Fatalf("fetch failed: %v", err)
			}
		}
	})

	b.Run("LRUGet", func(b *testing.B) {
		cache := NewLRUCache(1, 1)
		cache.Set("key", "value", time.Hour)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if cache.Get("key") == nil {
				b.Fatal("expected cache hit")
			}
		}
	})
}

func newPreloadedBenchmarkCache(b *testing.B) *Cache {
	b.Helper()

	cache, err := New(WithTTL(time.Hour))
	if err != nil {
		b.Fatalf("failed creating cache: %v", err)
	}
	_, err = cache.Fetch("key", cacheAllocFetcher)
	if err != nil {
		b.Fatalf("failed preloading cache: %v", err)
	}
	return cache
}
