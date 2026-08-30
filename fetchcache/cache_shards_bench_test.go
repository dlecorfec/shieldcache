package shieldcache

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkCacheHighConcurrencyShards(b *testing.B) {
	const keyCount = 4096
	shardCounts := []int{1, 2, 4, 8, 16, 32, 64, 128}
	parallelismLevels := []int{16, 64, 256}

	keys := make([]string, keyCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	for _, parallelism := range parallelismLevels {
		b.Run(fmt.Sprintf("parallelism=%d", parallelism), func(b *testing.B) {
			for _, shardCount := range shardCounts {
				b.Run(fmt.Sprintf("shards=%d", shardCount), func(b *testing.B) {
					benchmarkCacheHitsParallel(b, keys, shardCount, parallelism)
				})
			}
		})
	}
}

func benchmarkCacheHitsParallel(b *testing.B, keys []string, shardCount, parallelism int) {
	cache, err := New(
		WithSize(int32(len(keys)*2)),
		WithTTL(time.Hour),
		WithShards(shardCount),
	)
	if err != nil {
		b.Fatalf("failed creating cache: %v", err)
	}
	defer cache.Close()

	for i, key := range keys {
		value := i
		_, err := cache.Fetch(key, func() (interface{}, bool, error) {
			return value, true, nil
		})
		if err != nil {
			b.Fatalf("failed preloading cache: %v", err)
		}
	}

	fetchesBefore := cache.NewFetches()
	b.ReportAllocs()
	b.SetParallelism(parallelism)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := keys[i&(len(keys)-1)]
			value, err := cache.Fetch(key, cacheAllocFetcher)
			if err != nil {
				b.Errorf("fetch failed: %v", err)
			}
			if value == nil {
				b.Errorf("got nil cached value")
			}
			i++
		}
	})

	b.StopTimer()
	if fetches := cache.NewFetches() - fetchesBefore; fetches != 0 {
		b.Fatalf("got %d cache misses during benchmark", fetches)
	}
}
