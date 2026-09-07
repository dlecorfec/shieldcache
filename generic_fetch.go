package shieldcache

import (
	"context"
	"fmt"
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
	if err != nil || item == nil {
		return zero, err
	}

	value, ok := item.(T)
	if !ok {
		return zero, fmt.Errorf("shieldcache: cached value has type %T, want %T", item, zero)
	}
	return value, nil
}
