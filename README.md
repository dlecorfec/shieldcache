# shieldcache

[![PkgGoDev](https://pkg.go.dev/badge/github.com/dlecorfec/shieldcache)](https://pkg.go.dev/github.com/dlecorfec/shieldcache)

`shieldcache` is a Go in-memory LRU cache built around one operation: fetch a
value by key, cache the result, and coordinate concurrent callers that need the
same value.

It is intended for process-local caches in services that call APIs, databases,
or expensive local computations. It supports TTLs, stale serving, negative
caching, fetch de-duplication, context-aware fetches, bounded fetch concurrency,
background refresh, lifecycle shutdown, and optional sharded storage.

The module has no third-party runtime dependencies.

## Install

```sh
go get github.com/dlecorfec/shieldcache
```

## Quick Start

```go
package example

import (
        "context"
        "fmt"
        "time"

        "github.com/dlecorfec/shieldcache"
)

type Foo struct {
        ID string
}

type FooClient interface {
        ByID(context.Context, string) (*Foo, error)
}

func newCache() (*shieldcache.Cache, error) {
        return shieldcache.New(
                shieldcache.WithSize(5000),
                shieldcache.WithTTL(5*time.Minute),
                shieldcache.WithFetchers(100),
                shieldcache.WithStaleFetchers(3),
        )
}

func cachedFoo(ctx context.Context, cache *shieldcache.Cache, client FooClient, id string) (*Foo, error) {
        item, err := cache.FetchContext(ctx, "foo:"+id, func(ctx context.Context) (interface{}, bool, error) {
                foo, err := client.ByID(ctx, id)
                if err != nil {
                        return nil, false, err
                }

                return foo, foo != nil, nil
        })
        if err != nil {
                return nil, err
        }

        foo, ok := item.(*Foo)
        if !ok {
                return nil, fmt.Errorf("unexpected cache value %T", item)
        }
        return foo, nil
}
```

Call `Close` when a cache is no longer used, especially in tests, reloadable
services, or short-lived cache instances:

```go
cache, err := newCache()
if err != nil {
        return err
}
defer cache.Close()
```

Use `CloseContext` when shutdown must obey a context deadline.

## Fetch Semantics

`Fetch` and `FetchContext` both receive a cache key and a fetcher closure. The
fetcher returns three values:

| Return value | Meaning |
| --- | --- |
| `item` | Value returned to the caller and stored if cacheable. |
| `valid` | Whether `item` belongs in the positive cache. |
| `err` | Backend or computation error. |

The result is handled as follows:

| Fetcher result | Cache behavior |
| --- | --- |
| `err != nil` | Store the result in the negative cache and keep any previous positive value. |
| `err == nil && valid == false` | Store the result in the negative cache and delete any previous positive value. |
| `err == nil && valid == true` | Store the result in the positive cache and delete any previous negative value. |

Use `Fetch` when the caller does not have a request context:

```go
item, err := cache.Fetch(key, func() (interface{}, bool, error) {
        return loadValue(), true, nil
})
```

Use `FetchContext` in request paths. The context can cancel waiting for another
goroutine fetching the same key, waiting for the foreground fetch limiter, and
the fetcher itself:

```go
item, err := cache.FetchContext(ctx, key, func(ctx context.Context) (interface{}, bool, error) {
        return loadValueContext(ctx)
})
```

Errors caused by the caller context being canceled or expired are not stored in
the negative cache.

For typed results, use the package-level generic helpers. They use the same
cache coordination and stale/negative-cache behavior without requiring a type
assertion:

```go
value, err := shieldcache.FetchContext[*Foo](cache, ctx, "foo:"+id,
        func(ctx context.Context) (*Foo, bool, error) {
                foo, err := client.ByID(ctx, id)
                return foo, foo != nil, err
        })
```

The generic helpers are package functions because Go methods cannot declare
their own type parameters.

## Features

### Cache Locking

Only one goroutine fetches a missing key at a time. Other goroutines requesting
the same key wait for that fetch to finish, then read the cached result.

If a cache is too small to retain the fetched value, waiting callers may fall
back to their own fetch after the first fetch completes.

### Positive Cache And Stale Values

Positive entries use `WithTTL`. When a positive entry expires, `Fetch` can return
the stale value immediately and queue a background refresh.

Stale positive entries are served by default. Use `WithStale(false)` to disable
stale serving globally, or `WithStaleValidator` to decide based on the stale age:

```go
cache, err := shieldcache.New(
        shieldcache.WithTTL(time.Minute),
        shieldcache.WithStaleValidator(func(item interface{}, staleAge time.Duration) bool {
                return staleAge < 30*time.Second
        }),
)
```

If a refresh fails, the previous positive value remains in the cache and may
continue to be served stale according to the stale policy.

### Negative Cache

The negative cache stores fetch errors and invalid values separately from valid
positive entries. This avoids repeatedly calling a backend for known failures,
missing objects, or temporarily invalid objects.

Use `WithNegSize`, `WithNegPruneSize`, and `WithNegTTL` to configure it.

Expired negative entries are treated as misses by default. Use
`WithNegStale(true)` to serve an expired negative entry once before removing it,
so the following request refetches.

### Fetch Concurrency Limits

`WithFetchers` limits concurrent foreground fetches. This protects the backend
when many different keys miss at the same time.

Background refresh concurrency is controlled separately by `WithStaleFetchers`.
The refresh queue size is controlled by `WithStaleQueueSize`; when the queue is
full, new refresh requests are dropped instead of blocking request handling.

### Sharded Storage

`WithShards` splits the positive and negative LRU stores across multiple shards
using `hash/maphash`. The default is `1`, which keeps one global LRU store.

Sharding can reduce lock contention on hot cache-hit paths. Capacity is enforced
across the total size of all shards, not as `size / shard count`, so one shard
may hold more entries than another when keys are unevenly distributed.

Keep the default unless profiling shows LRU lock contention. For high-concurrency
single-process workloads, benchmark values such as `4`, `8`, `16`, and `32`.

## Lifecycle

`New` starts background workers for asynchronous stale refreshes.

`Close` is idempotent. It starts shutdown, waits for in-flight background
refreshes to finish, and causes later `Fetch` or `FetchContext` calls to return
`ErrClosed`.

`CloseContext` is also idempotent. It returns the context error if the context is
done before workers have stopped; shutdown still continues in the background, and
a later `Close` or `CloseContext` can wait for completion.

## Stats

`Stats` returns an atomic snapshot of cache counters and current cache sizes:

```go
stats := cache.Stats()

hitRatio := float64(stats.Hits) / float64(stats.Requests)
_ = hitRatio
```

The package does not depend on Prometheus, OpenTelemetry, or another metrics
backend. Export the snapshot through the instrumentation stack your service
already uses.

| Field | Meaning |
| --- | --- |
| `Requests` | Calls to `Fetch` or `FetchContext` accepted by the cache. |
| `Hits` | Requests served from the positive or negative cache. |
| `Misses` | `Requests - Hits`. |
| `NewFetches` | Foreground fetches for cache misses. |
| `RefreshFetches` | Background refresh fetches for expired positive entries. |
| `StaleHits` | Expired positive entries served stale. |
| `NegativeHits` | Requests served from the negative cache. |
| `FetchErrors` | Foreground fetches that returned an error. |
| `RefreshErrors` | Background refreshes that returned an error. |
| `RefreshQueued` | Background refresh requests accepted into the queue. |
| `RefreshDropped` | Background refresh requests dropped because the queue was full. |
| `PositiveSize` | Current positive cache entry count. |
| `NegativeSize` | Current negative cache entry count. |

## Options

| Option | Default | Description |
| --- | --- | --- |
| `WithSize(int32)` | `5000` | Positive cache capacity. |
| `WithPruneSize(int32)` | `size/20 + 1` | Positive entries pruned when full. |
| `WithTTL(time.Duration)` | `60s` | Positive entry TTL. |
| `WithNegSize(int32)` | `500` | Negative cache capacity. |
| `WithNegPruneSize(int32)` | `negSize/20 + 1` | Negative entries pruned when full. |
| `WithNegTTL(time.Duration)` | `5s` | Negative entry TTL. |
| `WithNegStale(bool)` | `false` | Serve expired negative entries once before refetch. |
| `WithFetchers(int)` | `100` | Max concurrent foreground fetches. |
| `WithStaleFetchers(int)` | `3` | Background refresh worker count. |
| `WithStaleQueueSize(int)` | `1000` | Background refresh queue size. |
| `WithStale(bool)` | `true` | Enable stale positive value serving. |
| `WithStaleValidator(func(interface{}, time.Duration) bool)` | `nil` | Custom stale positive value policy. |
| `WithShards(int)` | `1` | Positive and negative LRU shard count. |

`New` returns an error for invalid option values, such as negative sizes,
negative TTLs, no stale fetchers, no foreground fetchers, no shards, or a
negative stale queue size.

## Generic LRU

The package also exposes a lower-level typed LRU cache:

```go
cache := shieldcache.NewGenericLRUCache[string, *Foo](5000, 100)

cache.Set("foo:123", &Foo{ID: "123"}, 5*time.Minute)
foo, ok := cache.GetValue("foo:123")
```

Use `GenericLRUCache[K, V]` when you want a simple typed LRU cache and do not
need the higher-level `Fetch` behavior.

For large structs, prefer storing pointers:

```go
// Prefer this for large values.
shieldcache.NewGenericLRUCache[string, *LargeValue](5000, 100)

// Avoid this for large values: it copies the value into and out of the cache.
shieldcache.NewGenericLRUCache[string, LargeValue](5000, 100)
```

`NewLRUCache` and `NewGenericLRUCache` normalize negative maximum sizes to zero
and a zero prune size to one item, so caches still respect their configured
capacity.

## Tuning and Observability

Start with the defaults and observe how the cache behaves in your workload. The
most important metrics are:

- **Hit ratio** (`Hits / Requests`): A low hit ratio typically means TTL is too
  short or the cache is too small. A very high ratio may indicate you're not
  testing real patterns.

- **Negative cache behavior** (`NegativeHits`): Track whether you're benefiting
  from negative caching (e.g., error responses, missing objects). If the negative
  cache stores results you don't want repeated, adjust `WithNegTTL`.

- **Refresh queue** (`RefreshQueued` vs `RefreshDropped`): High drop rates mean
  `WithStaleQueueSize` is too small or `WithStaleFetchers` is too slow for your
  workload. Dropping is intentional to avoid blocking requests, but monitor when
  it happens.

- **Stale hits** (`StaleHits`): Indicates you're serving expired entries while
  refreshing. If stale serving is unexpected, review `WithTTL`, `WithStale`,
  and `WithStaleValidator`.

Measure backend latency in your fetcher, not just cache hit latency. Use the
`Stats` snapshot to track trends over time. Export through your existing
observability stack.

TTL tuning is empirical. If your backend is slow or costly, prefer longer TTLs
and accept stale serving. If freshness is critical, shorter TTLs mean more
refreshes and backend load. The `WithStaleValidator` hook lets you adjust on a
per-entry basis.

For performance testing, model your real access patterns:

- **Spike testing**: Simulate a sudden miss on a hot key with many concurrent
  callers. Verify that fetch de-duplication prevents a cache stampede and
  `WithFetchers` doesn't become a bottleneck.

- **Refresh under load**: Test whether background refreshes keep up during
  normal traffic. If the queue fills and drops refresh requests, you'll see
  older stale values served longer.

- **Negative cache retention**: Confirm that storing errors or missing objects
  doesn't mask real backend recovery. If a backend recovers and you're still
  serving cached errors, `WithNegTTL` is too long for your use case.

Contention tuning is only necessary if cache hit latency degrades as throughput
increases, suggesting lock contention is the bottleneck. The `WithShards` option
trades memory for parallelism; benchmark with `4`, `8`, and `16` if latency
doesn't improve with sharding, move on to other bottlenecks.

## Limitations

The cache is process-local. It does not share entries across processes and does
not provide persistence.

Expired positive entries are not removed just because their TTL has passed. They
remain in memory until overwritten, evicted by LRU pruning, deleted, or cleared.
This is intentional for stale serving, but it means cache size and TTL should be
chosen with memory retention in mind.

When storing pointers, callers can mutate the cached object through the pointer.
Treat cached objects as immutable by convention, or store defensive copies if
that is not acceptable.

