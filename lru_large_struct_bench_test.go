package shieldcache

import (
	"fmt"
	"testing"
	"time"
)

// LargeStruct64B - ~64 bytes
type LargeStruct64B struct {
	ID    int64
	Name  string
	Email string
	Age   int32
}

// LargeStruct256B - ~256 bytes
type LargeStruct256B struct {
	ID       int64
	Name     string
	Email    string
	Phone    string
	Address  string
	City     string
	State    string
	Zip      string
	Country  string
	Age      int32
	Status   string
	Type     string
	Metadata string
}

// LargeStruct1KB - ~1KB
type LargeStruct1KB struct {
	ID          int64
	Name        string
	Email       string
	Phone       string
	Address     string
	City        string
	State       string
	Zip         string
	Country     string
	Age         int32
	Status      string
	Type        string
	Metadata    string
	Description string
	Tags        [10]string
	Data1       [64]byte
	Data2       [64]byte
	Data3       [64]byte
	Data4       [64]byte
	Data5       [64]byte
}

// LargeStruct10KB - ~10KB
type LargeStruct10KB struct {
	ID       int64
	Name     string
	Email    string
	Phone    string
	Address  string
	City     string
	State    string
	Zip      string
	Country  string
	Age      int32
	Status   string
	Type     string
	Metadata string
	Desc     string
	Tags     [20]string
	Data     [10][128]byte
}

// BenchmarkLargeStructs benchmarks generic vs interface{} with large structs
func BenchmarkLargeStructs(b *testing.B) {
	// ===== 64 BYTE STRUCT =====

	b.Run("Generic/64B/Set", func(b *testing.B) {
		cache := NewGenericLRUCache[int, LargeStruct64B](5000, 100)
		s := LargeStruct64B{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 30}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Set(i%100, s, 60*time.Second)
		}
	})

	b.Run("Interface/64B/Set", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		s := LargeStruct64B{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 30}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Set(fmt.Sprintf("%d", i%100), s, 60*time.Second)
		}
	})

	b.Run("Generic/64B/Get", func(b *testing.B) {
		cache := NewGenericLRUCache[int, LargeStruct64B](5000, 100)
		s := LargeStruct64B{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 30}
		cache.Set(1, s, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = cache.GetValue(1)
		}
	})

	b.Run("Interface/64B/Get", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		s := LargeStruct64B{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 30}
		cache.Set("1", s, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("1")
			_ = item.Value().(LargeStruct64B)
		}
	})

	b.Run("Generic/64B/SetGet", func(b *testing.B) {
		cache := NewGenericLRUCache[int, LargeStruct64B](5000, 100)
		s := LargeStruct64B{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 30}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := i % 100
			cache.Set(key, s, 60*time.Second)
			_, _ = cache.GetValue(key)
		}
	})

	b.Run("Interface/64B/SetGet", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		s := LargeStruct64B{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 30}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("%d", i%100)
			cache.Set(key, s, 60*time.Second)
			item := cache.Get(key)
			_ = item.Value().(LargeStruct64B)
		}
	})

	// ===== 256 BYTE STRUCT =====

	b.Run("Generic/256B/Set", func(b *testing.B) {
		cache := NewGenericLRUCache[int, LargeStruct256B](5000, 100)
		s := LargeStruct256B{ID: 1, Name: "Alice", Email: "alice@example.com"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Set(i%100, s, 60*time.Second)
		}
	})

	b.Run("Interface/256B/Set", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		s := LargeStruct256B{ID: 1, Name: "Alice", Email: "alice@example.com"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Set(fmt.Sprintf("%d", i%100), s, 60*time.Second)
		}
	})

	b.Run("Generic/256B/Get", func(b *testing.B) {
		cache := NewGenericLRUCache[int, LargeStruct256B](5000, 100)
		s := LargeStruct256B{ID: 1, Name: "Alice", Email: "alice@example.com"}
		cache.Set(1, s, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = cache.GetValue(1)
		}
	})

	b.Run("Interface/256B/Get", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		s := LargeStruct256B{ID: 1, Name: "Alice", Email: "alice@example.com"}
		cache.Set("1", s, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("1")
			_ = item.Value().(LargeStruct256B)
		}
	})

	b.Run("Generic/256B/SetGet", func(b *testing.B) {
		cache := NewGenericLRUCache[int, LargeStruct256B](5000, 100)
		s := LargeStruct256B{ID: 1, Name: "Alice", Email: "alice@example.com"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := i % 100
			cache.Set(key, s, 60*time.Second)
			_, _ = cache.GetValue(key)
		}
	})

	b.Run("Interface/256B/SetGet", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		s := LargeStruct256B{ID: 1, Name: "Alice", Email: "alice@example.com"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("%d", i%100)
			cache.Set(key, s, 60*time.Second)
			item := cache.Get(key)
			_ = item.Value().(LargeStruct256B)
		}
	})

	// ===== 1KB STRUCT =====

	b.Run("Generic/1KB/Set", func(b *testing.B) {
		cache := NewGenericLRUCache[int, LargeStruct1KB](5000, 100)
		s := LargeStruct1KB{ID: 1, Name: "Alice", Email: "alice@example.com"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Set(i%100, s, 60*time.Second)
		}
	})

	b.Run("Interface/1KB/Set", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		s := LargeStruct1KB{ID: 1, Name: "Alice", Email: "alice@example.com"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Set(fmt.Sprintf("%d", i%100), s, 60*time.Second)
		}
	})

	b.Run("Generic/1KB/Get", func(b *testing.B) {
		cache := NewGenericLRUCache[int, LargeStruct1KB](5000, 100)
		s := LargeStruct1KB{ID: 1, Name: "Alice", Email: "alice@example.com"}
		cache.Set(1, s, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = cache.GetValue(1)
		}
	})

	b.Run("Interface/1KB/Get", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		s := LargeStruct1KB{ID: 1, Name: "Alice", Email: "alice@example.com"}
		cache.Set("1", s, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("1")
			_ = item.Value().(LargeStruct1KB)
		}
	})

	b.Run("Generic/1KB/SetGet", func(b *testing.B) {
		cache := NewGenericLRUCache[int, LargeStruct1KB](5000, 100)
		s := LargeStruct1KB{ID: 1, Name: "Alice", Email: "alice@example.com"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := i % 100
			cache.Set(key, s, 60*time.Second)
			_, _ = cache.GetValue(key)
		}
	})

	b.Run("Interface/1KB/SetGet", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		s := LargeStruct1KB{ID: 1, Name: "Alice", Email: "alice@example.com"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("%d", i%100)
			cache.Set(key, s, 60*time.Second)
			item := cache.Get(key)
			_ = item.Value().(LargeStruct1KB)
		}
	})

	// ===== 10KB STRUCT =====

	b.Run("Generic/10KB/Set", func(b *testing.B) {
		cache := NewGenericLRUCache[int, LargeStruct10KB](5000, 100)
		s := LargeStruct10KB{ID: 1, Name: "Alice", Email: "alice@example.com"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Set(i%100, s, 60*time.Second)
		}
	})

	b.Run("Interface/10KB/Set", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		s := LargeStruct10KB{ID: 1, Name: "Alice", Email: "alice@example.com"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Set(fmt.Sprintf("%d", i%100), s, 60*time.Second)
		}
	})

	b.Run("Generic/10KB/Get", func(b *testing.B) {
		cache := NewGenericLRUCache[int, LargeStruct10KB](5000, 100)
		s := LargeStruct10KB{ID: 1, Name: "Alice", Email: "alice@example.com"}
		cache.Set(1, s, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = cache.GetValue(1)
		}
	})

	b.Run("Interface/10KB/Get", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		s := LargeStruct10KB{ID: 1, Name: "Alice", Email: "alice@example.com"}
		cache.Set("1", s, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("1")
			_ = item.Value().(LargeStruct10KB)
		}
	})

	b.Run("Generic/10KB/SetGet", func(b *testing.B) {
		cache := NewGenericLRUCache[int, LargeStruct10KB](5000, 100)
		s := LargeStruct10KB{ID: 1, Name: "Alice", Email: "alice@example.com"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := i % 100
			cache.Set(key, s, 60*time.Second)
			_, _ = cache.GetValue(key)
		}
	})

	b.Run("Interface/10KB/SetGet", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		s := LargeStruct10KB{ID: 1, Name: "Alice", Email: "alice@example.com"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("%d", i%100)
			cache.Set(key, s, 60*time.Second)
			item := cache.Get(key)
			_ = item.Value().(LargeStruct10KB)
		}
	})
}

func BenchmarkLargeStructPointers(b *testing.B) {
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("%d", i)
	}
	value := &LargeStruct10KB{ID: 1, Name: "Alice", Email: "alice@example.com"}

	b.Run("Generic/10KBPointer/SetGet", func(b *testing.B) {
		cache := NewGenericLRUCache[string, *LargeStruct10KB](5000, 100)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := keys[i%len(keys)]
			cache.Set(key, value, 60*time.Second)
			got, _ := cache.GetValue(key)
			_ = got.ID
		}
	})

	b.Run("Interface/10KBPointer/SetGet", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := keys[i%len(keys)]
			cache.Set(key, value, 60*time.Second)
			item := cache.Get(key)
			got := item.Value().(*LargeStruct10KB)
			_ = got.ID
		}
	})

	b.Run("Generic/10KBPointer/Get", func(b *testing.B) {
		cache := NewGenericLRUCache[string, *LargeStruct10KB](5000, 100)
		cache.Set("1", value, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			got, _ := cache.GetValue("1")
			_ = got.ID
		}
	})

	b.Run("Interface/10KBPointer/Get", func(b *testing.B) {
		cache := NewLRUCache(5000, 100)
		cache.Set("1", value, 60*time.Second)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := cache.Get("1")
			got := item.Value().(*LargeStruct10KB)
			_ = got.ID
		}
	})
}
