package shieldcache

import (
	"testing"
	"time"
)

// TestGenericLRUCache tests the generic LRU cache implementation
func TestGenericLRUCache(t *testing.T) {
	cache := NewGenericLRUCache[string, string](100, 10)

	// Test Set and Get
	cache.Set("key1", "value1", 1*time.Second)
	item := cache.Get("key1")
	if item == nil {
		t.Fatal("Expected item, got nil")
	}
	if item.Value() != "value1" {
		t.Fatalf("Expected value1, got %v", item.Value())
	}

	// Test GetValue convenience method
	value, found := cache.GetValue("key1")
	if !found {
		t.Fatal("Expected to find key1")
	}
	if value != "value1" {
		t.Fatalf("Expected value1, got %v", value)
	}

	// Test non-existent key
	_, found = cache.GetValue("nonexistent")
	if found {
		t.Fatal("Expected key not found")
	}

	// Test Delete
	cache.Delete("key1")
	_, found = cache.GetValue("key1")
	if found {
		t.Fatal("Expected key to be deleted")
	}

	// Test Clear
	cache.Set("key2", "value2", 1*time.Second)
	cache.Set("key3", "value3", 1*time.Second)
	cache.Clear()
	if cache.Size() != 0 {
		t.Fatalf("Expected empty cache, got size %d", cache.Size())
	}
}

// TestGenericLRUCacheWithIntegers tests generic cache with integer values
func TestGenericLRUCacheWithIntegers(t *testing.T) {
	cache := NewGenericLRUCache[string, int](100, 10)

	cache.Set("count", 42, 1*time.Second)
	value, found := cache.GetValue("count")
	if !found {
		t.Fatal("Expected to find count")
	}
	if value != 42 {
		t.Fatalf("Expected 42, got %d", value)
	}

	// Type safety: this would be a compile error if we tried to store a string
	// cache.Set("count", "not an int", 1*time.Second) // compile error!
}

// TestGenericLRUCacheWithStructs tests generic cache with custom types
func TestGenericLRUCacheWithStructs(t *testing.T) {
	type User struct {
		ID   int
		Name string
	}

	cache := NewGenericLRUCache[int, User](100, 10)

	user := User{ID: 1, Name: "Alice"}
	cache.Set(1, user, 1*time.Second)

	retrieved, found := cache.GetValue(1)
	if !found {
		t.Fatal("Expected to find user")
	}
	if retrieved.Name != "Alice" {
		t.Fatalf("Expected Alice, got %s", retrieved.Name)
	}

	// Type safety: the compiler ensures we use the correct types
	// cache.Set(1, "wrong type", 1*time.Second) // compile error!
}

// TestGenericLRUCacheExpiration tests TTL expiration
func TestGenericLRUCacheExpiration(t *testing.T) {
	cache := NewGenericLRUCache[string, string](100, 10)

	cache.Set("temp", "value", 100*time.Millisecond)
	item := cache.Get("temp")
	if item == nil || item.Expired() {
		t.Fatal("Expected item to be valid")
	}

	time.Sleep(150 * time.Millisecond)
	item = cache.Get("temp")
	if item == nil || !item.Expired() {
		t.Fatal("Expected item to be expired")
	}
}

// TestGenericLRUCacheLRUEviction tests LRU eviction
func TestGenericLRUCacheLRUEviction(t *testing.T) {
	cache := NewGenericLRUCache[int, string](3, 1)

	// Add 3 items
	cache.Set(1, "one", 10*time.Second)
	cache.Set(2, "two", 10*time.Second)
	cache.Set(3, "three", 10*time.Second)

	// Add 4th item, should evict least recently used (item 1)
	cache.Set(4, "four", 10*time.Second)

	if cache.Size() > 3 {
		t.Fatalf("Expected cache size <= 3, got %d", cache.Size())
	}

	// Item 1 should be evicted
	_, found := cache.GetValue(1)
	if found {
		t.Fatal("Expected item 1 to be evicted")
	}
}

func TestLRUCachePrunesAtLeastOneItem(t *testing.T) {
	cache := NewLRUCache(1, 0)

	cache.Set("one", "one", 10*time.Second)
	cache.Set("two", "two", 10*time.Second)

	if len(cache.items) > 1 {
		t.Fatalf("Expected cache size <= 1, got %d", len(cache.items))
	}
	if cache.Get("one") != nil {
		t.Fatal("Expected item one to be evicted")
	}
	if item := cache.Get("two"); item == nil || item.Value() != "two" {
		t.Fatal("Expected item two to remain cached")
	}
}

func TestGenericLRUCachePrunesAtLeastOneItem(t *testing.T) {
	cache := NewGenericLRUCache[string, string](1, 0)

	cache.Set("one", "one", 10*time.Second)
	cache.Set("two", "two", 10*time.Second)

	if cache.Size() > 1 {
		t.Fatalf("Expected cache size <= 1, got %d", cache.Size())
	}
	if _, found := cache.GetValue("one"); found {
		t.Fatal("Expected item one to be evicted")
	}
	if value, found := cache.GetValue("two"); !found || value != "two" {
		t.Fatal("Expected item two to remain cached")
	}
}

func TestLRUCacheNormalizesNegativeMaxSize(t *testing.T) {
	cache := NewLRUCache(-1, 0)

	cache.Set("key", "value", 10*time.Second)

	if len(cache.items) != 0 {
		t.Fatalf("Expected empty cache, got size %d", len(cache.items))
	}
}

func TestGenericLRUCacheNormalizesNegativeMaxSize(t *testing.T) {
	cache := NewGenericLRUCache[string, string](-1, 0)

	cache.Set("key", "value", 10*time.Second)

	if cache.Size() != 0 {
		t.Fatalf("Expected empty cache, got size %d", cache.Size())
	}
}

// BenchmarkGenericLRUGetString benchmarks Get with string values
func BenchmarkGenericLRUGetString(b *testing.B) {
	cache := NewGenericLRUCache[string, string](5000, 100)
	cache.Set("key", "value", 60*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get("key")
	}
}

// BenchmarkGenericLRUGetInt benchmarks Get with int values (no unboxing needed)
func BenchmarkGenericLRUGetInt(b *testing.B) {
	cache := NewGenericLRUCache[string, int](5000, 100)
	cache.Set("count", 42, 60*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get("count")
	}
}

// BenchmarkGenericLRUSetString benchmarks Set with string values
func BenchmarkGenericLRUSetString(b *testing.B) {
	cache := NewGenericLRUCache[string, string](5000, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set("key", "value", 60*time.Second)
	}
}

// BenchmarkGenericLRUSetInt benchmarks Set with int values
func BenchmarkGenericLRUSetInt(b *testing.B) {
	cache := NewGenericLRUCache[string, int](5000, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set("key", i, 60*time.Second)
	}
}

// BenchmarkGenericLRUGetValueString benchmarks the convenience GetValue method
func BenchmarkGenericLRUGetValueString(b *testing.B) {
	cache := NewGenericLRUCache[string, string](5000, 100)
	cache.Set("key", "value", 60*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.GetValue("key")
	}
}

// BenchmarkGenericLRUHighContention benchmarks generic cache under parallel load
func BenchmarkGenericLRUHighContention(b *testing.B) {
	cache := NewGenericLRUCache[int, string](5000, 100)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % 100
			if i%2 == 0 {
				cache.Set(key, "value", 60*time.Second)
			} else {
				cache.Get(key)
			}
			i++
		}
	})
}

// BenchmarkGenericVsInterface compares generic vs interface{} performance
// Run with: go test -bench=BenchmarkGenericVsInterface -benchmem
func BenchmarkGenericVsInterface(b *testing.B) {
	b.Run("Generic/Get", func(b *testing.B) {
		cache := NewGenericLRUCache[string, string](5000, 100)
		cache.Set("key", "value", 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Get("key")
		}
	})

	b.Run("Interface/Get", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		cache.Set("key", "value", 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Get("key")
		}
	})

	b.Run("Generic/Set", func(b *testing.B) {
		cache := NewGenericLRUCache[string, string](5000, 100)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Set("key", "value", 60*time.Second)
		}
	})

	b.Run("Interface/Set", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Set("key", "value", 60*time.Second)
		}
	})
}
