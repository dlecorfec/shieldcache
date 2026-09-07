package shieldcache

import (
	"context"
	"fmt"
	"reflect"
)

// Fetch retrieves a typed value from cache, fetching it when it is missing or stale.
// It is the typed equivalent of (*Cache).Fetch.
func Fetch[T any](cache *Cache, key string, f func() (T, bool, error)) (T, error) {
	item, err := cache.Fetch(key, func() (interface{}, bool, error) {
		value, valid, err := f()
		return value, valid, err
	})
	return typedValue[T](item, err)
}

// FetchContext retrieves a typed value from cache, honoring ctx while waiting
// for another fetch, waiting for the fetch limiter, and running f.
// It is the typed equivalent of (*Cache).FetchContext.
func FetchContext[T any](ctx context.Context, cache *Cache, key string, f func(context.Context) (T, bool, error)) (T, error) {
	item, err := cache.FetchContext(ctx, key, func(ctx context.Context) (interface{}, bool, error) {
		value, valid, err := f(ctx)
		return value, valid, err
	})
	return typedValue[T](item, err)
}

func typedValue[T any](item interface{}, err error) (T, error) {
	var zero T
	if item == nil {
		if err != nil {
			return zero, err
		}
		want := reflect.TypeFor[T]()
		switch want.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
			return zero, nil
		default:
			return zero, fmt.Errorf("shieldcache: cached value has type <nil>, want %v", want)
		}
	}

	value, ok := item.(T)
	if !ok {
		if err != nil {
			return zero, err
		}
		return zero, fmt.Errorf("shieldcache: cached value has type %T, want %v", item, reflect.TypeFor[T]())
	}
	return value, err
}
