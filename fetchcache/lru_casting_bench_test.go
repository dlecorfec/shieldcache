package shieldcache

import (
	"testing"
	"time"
)

// BenchmarkGenericVsInterfaceWithCasting compares generic vs interface{}
// including the cost of type casting/assertions
func BenchmarkGenericVsInterfaceWithCasting(b *testing.B) {
	// ===== STRING VALUES =====

	b.Run("Generic/Get/String", func(b *testing.B) {
		cache := NewGenericLRUCache[string, string](5000, 100)
		cache.Set("key", "value", 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("key")
			_ = item.Value() // Get value (no casting needed)
		}
	})

	b.Run("Interface/Get/String/WithoutCasting", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		cache.Set("key", "value", 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("key")
			_ = item.Value() // Get value (without casting)
		}
	})

	b.Run("Interface/Get/String/WithCasting", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		cache.Set("key", "value", 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("key")
			_ = item.Value().(string) // Type assertion (realistic usage)
		}
	})

	// ===== INT VALUES =====

	b.Run("Generic/Get/Int", func(b *testing.B) {
		cache := NewGenericLRUCache[string, int](5000, 100)
		cache.Set("count", 42, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("count")
			_ = item.Value() // Get value (no casting needed)
		}
	})

	b.Run("Interface/Get/Int/WithoutCasting", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		cache.Set("count", 42, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("count")
			_ = item.Value() // Get value (without casting)
		}
	})

	b.Run("Interface/Get/Int/WithCasting", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		cache.Set("count", 42, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("count")
			_ = item.Value().(int) // Type assertion (realistic usage)
		}
	})

	// ===== STRUCT VALUES =====

	b.Run("Generic/Get/Struct", func(b *testing.B) {
		type User struct {
			ID   int
			Name string
		}
		cache := NewGenericLRUCache[int, User](5000, 100)
		cache.Set(123, User{ID: 123, Name: "Alice"}, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get(123)
			_ = item.Value() // Get value (no casting needed)
		}
	})

	b.Run("Interface/Get/Struct/WithoutCasting", func(b *testing.B) {
		type User struct {
			ID   int
			Name string
		}
		cache := NewLRUCache(5000, 100)
		cache.Set("123", User{ID: 123, Name: "Alice"}, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("123")
			_ = item.Value() // Get value (without casting)
		}
	})

	b.Run("Interface/Get/Struct/WithCasting", func(b *testing.B) {
		type User struct {
			ID   int
			Name string
		}
		cache := NewLRUCache(5000, 100)
		cache.Set("123", User{ID: 123, Name: "Alice"}, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("123")
			_ = item.Value().(User) // Type assertion (realistic usage)
		}
	})

	// ===== SET OPERATIONS =====

	b.Run("Generic/Set/String", func(b *testing.B) {
		cache := NewGenericLRUCache[string, string](5000, 100)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Set("key", "value", 60*time.Second)
		}
	})

	b.Run("Interface/Set/String", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Set("key", "value", 60*time.Second)
		}
	})

	// ===== GETVALUE CONVENIENCE METHOD =====

	b.Run("Generic/GetValue/String", func(b *testing.B) {
		cache := NewGenericLRUCache[string, string](5000, 100)
		cache.Set("key", "value", 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = cache.GetValue("key") // Convenience method
		}
	})
}

// BenchmarkRealWorldScenarios benchmarks realistic usage patterns
func BenchmarkRealWorldScenarios(b *testing.B) {
	type UserProfile struct {
		ID    int
		Name  string
		Email string
		Age   int
	}

	// Scenario 1: Generic cache - typical usage
	b.Run("Generic/RealWorld/Get", func(b *testing.B) {
		cache := NewGenericLRUCache[int, UserProfile](5000, 100)
		cache.Set(123, UserProfile{
			ID:    123,
			Name:  "Alice",
			Email: "alice@example.com",
			Age:   30,
		}, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			user, found := cache.GetValue(123)
			if found {
				_ = user.Name // Use the value
			}
		}
	})

	// Scenario 2: Interface{} - typical usage with type assertion
	b.Run("Interface/RealWorld/Get", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		cache.Set("123", UserProfile{
			ID:    123,
			Name:  "Alice",
			Email: "alice@example.com",
			Age:   30,
		}, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("123")
			if item != nil {
				user := item.Value().(UserProfile) // Type assertion required
				_ = user.Name
			}
		}
	})

	// Scenario 3: Generic - set and get
	b.Run("Generic/RealWorld/SetGet", func(b *testing.B) {
		cache := NewGenericLRUCache[int, UserProfile](5000, 100)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := i % 100
			user := UserProfile{
				ID:    id,
				Name:  "User",
				Email: "user@example.com",
				Age:   25,
			}
			cache.Set(id, user, 60*time.Second)
			retrieved, _ := cache.GetValue(id)
			_ = retrieved.Name
		}
	})

	// Scenario 4: Interface{} - set and get with type assertion
	b.Run("Interface/RealWorld/SetGet", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := i % 100
			user := UserProfile{
				ID:    id,
				Name:  "User",
				Email: "user@example.com",
				Age:   25,
			}
			cache.Set("id_"+string(rune(id)), user, 60*time.Second)
			item := cache.Get("id_" + string(rune(id)))
			if item != nil {
				retrieved := item.Value().(UserProfile) // Type assertion
				_ = retrieved.Name
			}
		}
	})
}
