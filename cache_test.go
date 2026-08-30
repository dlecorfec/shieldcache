package shieldcache

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const MaxIter = 40

func fetchFib(c *Cache, n int, b *testing.B) int {
	res, err := c.Fetch("fibtest", func() (interface{}, bool, error) {
		return fib(n), true, nil
	})
	if err != nil {
		b.Fatalf("Fetch error: %s", err.Error())
	}
	r, ok := res.(int)
	if !ok {
		b.Fatalf("Fetch result is not an int")
	}
	return r
}

func fib(n int) int {
	if n < 2 {
		return n
	}
	return fib(n-1) + fib(n-2)
}

var result int

func BenchmarkCache(b *testing.B) {
	c, err := New()
	if err != nil {
		b.Fatalf("error creating cache: %s", err)
	}
	fetchFib(c, MaxIter, b)
	var r int
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r = fetchFib(c, MaxIter, b)
	}
	result = r
}

func benchmarkAsyncPreFetch(b *testing.B) {
	c, err := New(WithTTL(1 * time.Second))
	if err != nil {
		b.Fatalf("error creating cache: %s", err)
	}
	fetchFib(c, MaxIter, b)
	time.Sleep(time.Second)
	var r int
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r = fetchFib(c, MaxIter, b)
	}
	result = r
}

func benchmarkAsyncNoPreFetch(b *testing.B) {
	c, err := New(WithTTL(1 * time.Second))
	if err != nil {
		b.Fatalf("error creating cache: %s", err)
	}
	var r int
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r = fetchFib(c, MaxIter, b)
	}
	result = r
}

func benchmarkNoCache(b *testing.B) {
	fib(MaxIter)
	var r int
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r = fib(MaxIter)
	}
	result = r
}

type FetchNum int

func (nfetch FetchNum) assertNum(t *testing.T, n int) {
	if int(nfetch) != n {
		t.Fatalf("bad number of fetch %d, should be %d", int(nfetch), n)
	}
}

func TestCache(t *testing.T) {
	var nfetch FetchNum
	var mu sync.RWMutex
	c, err := New(WithSize(10), WithTTL(1*time.Second), WithNegSize(3), WithNegTTL(1*time.Second), WithNegStale(true), WithStaleFetchers(3),
		WithStaleQueueSize(1))
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}
	fetchtest := func(key, val string) (string, error) {
		v, err := c.Fetch(key, func() (interface{}, bool, error) {
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			nfetch++
			mu.Unlock()
			if key == "err" {
				return nil, false, errors.New("test error")
			}
			return val, true, nil
		})
		if err != nil {
			return "", errors.New("fetch error: " + err.Error())
		}
		s, ok := v.(string)
		if !ok {
			return "", errors.New("value is not string")
		}
		return s, nil
	}

	// test cache locking
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			s0, err := fetchtest("key", "val")
			if err != nil {
				t.Errorf("s0: got error %v", err)
			}
			if s0 != "val" {
				t.Errorf("bad value for s0 %s", s0)
			}
			mu.RLock()
			nfetch.assertNum(t, 1)
			mu.RUnlock()
			wg.Done()
		}()
	}
	wg.Wait()
	// test stale: get stale value on first hit
	time.Sleep(1100 * time.Millisecond)
	mu.RLock()
	if int(nfetch) != 1 {
		t.Fatalf("bad number of fetch %d, should be %d", int(nfetch), 1)
	}
	mu.RUnlock()
	s3, err := fetchtest("key", "val")
	if err != nil {
		t.Fatalf("s3: got error %v", err)
	}
	if s3 != "val" {
		t.Fatalf("bad value for s3: %s", s3)
	}
	mu.RLock()
	if int(nfetch) != 1 {
		t.Fatalf("bad number of fetch %d, should be %d", int(nfetch), 1)
	}
	mu.RUnlock()

	// test stale: get refreshed value on second hit
	time.Sleep(100 * time.Millisecond)
	s4, err := fetchtest("key", "val")
	if err != nil {
		t.Fatalf("s4: got error %v", err)
	}
	if s4 != "val" {
		t.Fatalf("bad value for s4: %s", s4)
	}
	mu.RLock()
	nfetch.assertNum(t, 2)
	mu.RUnlock()

	// test negative cache (miss)
	_, err = fetchtest("err", "val")
	if err == nil {
		t.Fatalf("should get an error")
	}
	if err.Error() != "fetch error: test error" {
		t.Fatalf("error should be \"fetch error: test error\", is %v", err)
	}
	mu.RLock()
	nfetch.assertNum(t, 3)
	mu.RUnlock()

	// test negative cache (hit)
	_, err = fetchtest("err", "val")
	if err == nil {
		t.Fatalf("should get an error")
	}
	if err.Error() != "fetch error: test error" {
		t.Fatalf("error should be \"fetch error: test error\", is %v", err)
	}
	mu.RLock()
	nfetch.assertNum(t, 3)
	mu.RUnlock()
	time.Sleep(1100 * time.Millisecond)
	// test stale negative cache (hit)
	_, err = fetchtest("err", "val")
	if err == nil {
		t.Fatalf("should get an error")
	}
	if err.Error() != "fetch error: test error" {
		t.Fatalf("error should be \"fetch error: test error\", is %v", err)
	}
	mu.RLock()
	nfetch.assertNum(t, 3)
	mu.RUnlock()
	time.Sleep(100 * time.Millisecond)
	// test negative cache (hit)
	_, err = fetchtest("err", "val")
	if err == nil {
		t.Fatalf("should get an error")
	}
	if err.Error() != "fetch error: test error" {
		t.Fatalf("error should be \"fetch error: test error\", is %v", err)
	}
	mu.RLock()
	nfetch.assertNum(t, 4)
	mu.RUnlock()
}

func TestCacheWithoutNegativeStale(t *testing.T) {
	var nfetch FetchNum
	c, err := New(WithNegTTL(10 * time.Millisecond))
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}

	fetchtest := func() error {
		_, err := c.Fetch("err", func() (interface{}, bool, error) {
			nfetch++
			return nil, false, errors.New("test error")
		})
		return err
	}

	if err := fetchtest(); err == nil {
		t.Fatalf("should get an error")
	}
	nfetch.assertNum(t, 1)

	if err := fetchtest(); err == nil {
		t.Fatalf("should get an error")
	}
	nfetch.assertNum(t, 1)

	time.Sleep(20 * time.Millisecond)
	if err := fetchtest(); err == nil {
		t.Fatalf("should get an error")
	}
	nfetch.assertNum(t, 2)
}

func TestCacheStats(t *testing.T) {
	c, err := New(WithTTL(10*time.Millisecond), WithNegTTL(time.Minute), WithStaleFetchers(1))
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}
	defer c.Close()

	if _, err := c.Fetch("ok", func() (interface{}, bool, error) {
		return "value", true, nil
	}); err != nil {
		t.Fatalf("initial fetch failed: %v", err)
	}
	if _, err := c.Fetch("ok", func() (interface{}, bool, error) {
		t.Fatal("fresh cache hit should not call fetcher")
		return nil, false, nil
	}); err != nil {
		t.Fatalf("fresh hit failed: %v", err)
	}
	if _, err := c.Fetch("err", func() (interface{}, bool, error) {
		return nil, false, errors.New("test error")
	}); err == nil {
		t.Fatalf("expected fetch error")
	}
	if _, err := c.Fetch("err", func() (interface{}, bool, error) {
		t.Fatal("negative cache hit should not call fetcher")
		return nil, false, nil
	}); err == nil {
		t.Fatalf("expected cached fetch error")
	}

	refreshDone := make(chan struct{})
	time.Sleep(20 * time.Millisecond)
	if _, err := c.Fetch("ok", func() (interface{}, bool, error) {
		close(refreshDone)
		return nil, false, errors.New("refresh error")
	}); err != nil {
		t.Fatalf("stale hit failed: %v", err)
	}

	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refresh")
	}
	deadline := time.After(time.Second)
	for {
		stats := c.Stats()
		if stats.RefreshErrors == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for refresh error metric, got %d", stats.RefreshErrors)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	stats := c.Stats()
	if stats.Requests != 5 {
		t.Fatalf("expected 5 requests, got %d", stats.Requests)
	}
	if stats.Hits != 3 {
		t.Fatalf("expected 3 hits, got %d", stats.Hits)
	}
	if stats.Misses != 2 {
		t.Fatalf("expected 2 misses, got %d", stats.Misses)
	}
	if stats.NewFetches != 2 {
		t.Fatalf("expected 2 new fetches, got %d", stats.NewFetches)
	}
	if stats.StaleHits != 1 {
		t.Fatalf("expected 1 stale hit, got %d", stats.StaleHits)
	}
	if stats.NegativeHits != 1 {
		t.Fatalf("expected 1 negative hit, got %d", stats.NegativeHits)
	}
	if stats.FetchErrors != 1 {
		t.Fatalf("expected 1 fetch error, got %d", stats.FetchErrors)
	}
	if stats.RefreshFetches != 1 {
		t.Fatalf("expected 1 refresh fetch, got %d", stats.RefreshFetches)
	}
	if stats.RefreshErrors != 1 {
		t.Fatalf("expected 1 refresh error, got %d", stats.RefreshErrors)
	}
	if stats.RefreshQueued != 1 {
		t.Fatalf("expected 1 queued refresh, got %d", stats.RefreshQueued)
	}
	if stats.RefreshDropped != 0 {
		t.Fatalf("expected 0 dropped refreshes, got %d", stats.RefreshDropped)
	}
	if stats.PositiveSize != 1 {
		t.Fatalf("expected positive cache size 1, got %d", stats.PositiveSize)
	}
	if stats.NegativeSize != 2 {
		t.Fatalf("expected negative cache size 2, got %d", stats.NegativeSize)
	}
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name    string
		option  Option
		wantErr string
	}{
		{name: "negative positive size", option: WithSize(-1), wantErr: "positive cache size"},
		{name: "negative positive prune size", option: WithPruneSize(-1), wantErr: "positive cache prune size"},
		{name: "negative positive ttl", option: WithTTL(-1), wantErr: "positive cache TTL"},
		{name: "negative negative size", option: WithNegSize(-1), wantErr: "negative cache size"},
		{name: "negative negative prune size", option: WithNegPruneSize(-1), wantErr: "negative cache prune size"},
		{name: "negative negative ttl", option: WithNegTTL(-1), wantErr: "negative cache TTL"},
		{name: "zero stale fetchers", option: WithStaleFetchers(0), wantErr: "stale fetchers"},
		{name: "negative stale queue size", option: WithStaleQueueSize(-1), wantErr: "stale queue size"},
		{name: "zero fetchers", option: WithFetchers(0), wantErr: "fetchers"},
		{name: "zero shards", option: WithShards(0), wantErr: "shards"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.option)
			if err == nil {
				t.Fatalf("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestCacheWithShards(t *testing.T) {
	c, err := New(WithShards(4))
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}
	defer c.Close()

	if _, ok := c.posCache.(*shardedLRUStore); !ok {
		t.Fatalf("expected positive cache to be sharded")
	}
	if _, ok := c.negCache.(*shardedLRUStore); !ok {
		t.Fatalf("expected negative cache to be sharded")
	}
}

func TestFetchContextPassesContext(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}
	defer c.Close()

	type contextKey string
	ctx := context.WithValue(context.Background(), contextKey("key"), "value")
	item, err := c.FetchContext(ctx, "key", func(ctx context.Context) (interface{}, bool, error) {
		return ctx.Value(contextKey("key")), true, nil
	})
	if err != nil {
		t.Fatalf("FetchContext failed: %v", err)
	}
	if item != "value" {
		t.Fatalf("expected context value, got %v", item)
	}
}

func TestFetchContextCanceledBeforeStart(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.FetchContext(ctx, "key", func(context.Context) (interface{}, bool, error) {
		t.Fatal("fetcher should not be called with canceled context")
		return nil, false, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestFetchContextCancelsWaitingForSameKey(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}
	defer c.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = c.Fetch("key", func() (interface{}, bool, error) {
			close(started)
			<-release
			return "value", true, nil
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first fetch")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = c.FetchContext(ctx, "key", func(context.Context) (interface{}, bool, error) {
		t.Fatal("second fetcher should not be called while first fetch is in progress")
		return nil, false, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}

	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first fetch to finish")
	}
}

func TestFetchContextCancelsWaitingForFetchLimiter(t *testing.T) {
	c, err := New(WithFetchers(1))
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}
	defer c.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = c.Fetch("first", func() (interface{}, bool, error) {
			close(started)
			<-release
			return "value", true, nil
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first fetch")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = c.FetchContext(ctx, "second", func(context.Context) (interface{}, bool, error) {
		t.Fatal("second fetcher should not be called without a fetch limiter slot")
		return nil, false, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}

	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first fetch to finish")
	}

	item, err := c.Fetch("second", func() (interface{}, bool, error) {
		return "second", true, nil
	})
	if err != nil {
		t.Fatalf("retry fetch failed: %v", err)
	}
	if item != "second" {
		t.Fatalf("expected retry value, got %v", item)
	}
}

func TestFetchContextCancellationIsNotNegativeCached(t *testing.T) {
	c, err := New(WithNegTTL(time.Minute))
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	_, err = c.FetchContext(ctx, "key", func(ctx context.Context) (interface{}, bool, error) {
		cancel()
		return nil, false, ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	var calls int
	item, err := c.Fetch("key", func() (interface{}, bool, error) {
		calls++
		return "value", true, nil
	})
	if err != nil {
		t.Fatalf("retry fetch failed: %v", err)
	}
	if item != "value" {
		t.Fatalf("expected retry value, got %v", item)
	}
	if calls != 1 {
		t.Fatalf("expected retry fetcher to be called once, got %d", calls)
	}
}

func TestCacheClose(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}

	c.Close()
	c.Close()

	_, err = c.Fetch("key", func() (interface{}, bool, error) {
		t.Fatal("fetcher should not be called after Close")
		return nil, false, nil
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestCacheCloseWaitsForStaleFetch(t *testing.T) {
	c, err := New(WithTTL(0), WithStaleFetchers(1))
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	fetcher := func() (interface{}, bool, error) {
		if atomic.AddInt32(&calls, 1) == 2 {
			close(started)
			<-release
		}
		return "value", true, nil
	}

	if _, err := c.Fetch("key", fetcher); err != nil {
		t.Fatalf("initial fetch failed: %v", err)
	}
	if _, err := c.Fetch("key", fetcher); err != nil {
		t.Fatalf("stale fetch failed: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stale fetch to start")
	}

	closeStarted := make(chan struct{})
	closed := make(chan struct{})
	go func() {
		close(closeStarted)
		c.Close()
		close(closed)
	}()
	<-closeStarted

	select {
	case <-closed:
		t.Fatal("Close returned before stale fetch completed")
	default:
	}

	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Close")
	}
}

func TestCacheCloseContextTimesOut(t *testing.T) {
	c, err := New(WithTTL(0), WithStaleFetchers(1))
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	fetcher := func() (interface{}, bool, error) {
		if atomic.AddInt32(&calls, 1) == 2 {
			close(started)
			<-release
		}
		return "value", true, nil
	}

	if _, err := c.Fetch("key", fetcher); err != nil {
		t.Fatalf("initial fetch failed: %v", err)
	}
	if _, err := c.Fetch("key", fetcher); err != nil {
		t.Fatalf("stale fetch failed: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stale fetch to start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := c.CloseContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}

	_, err = c.Fetch("key", func() (interface{}, bool, error) {
		t.Fatal("fetcher should not be called after CloseContext")
		return nil, false, nil
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}

	close(release)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := c.CloseContext(waitCtx); err != nil {
		t.Fatalf("expected CloseContext to finish after release, got %v", err)
	}
}

func TestCacheQueueDrop(t *testing.T) {
	var nfetch FetchNum
	var mu sync.Mutex
	c, err := New(WithSize(10), WithTTL(1*time.Second), WithNegSize(3),
		WithPruneSize(1), WithNegPruneSize(1), WithNegTTL(1*time.Second), WithStaleFetchers(3),
		WithStaleQueueSize(1))
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}
	fetchtest := func(key, val string) (string, error) {
		v, err := c.Fetch(key, func() (interface{}, bool, error) {
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			nfetch++
			mu.Unlock()
			if key == "err" {
				return nil, false, errors.New("test error")
			}
			return val, true, nil
		})
		if err != nil {
			return "", errors.New("fetch error: " + err.Error())
		}
		s, ok := v.(string)
		if !ok {
			return "", errors.New("value is not string")
		}
		return s, nil
	}

	fetchtest("a", "val")
	fetchtest("b", "val")
	fetchtest("c", "val")
	fetchtest("d", "val")
	fetchtest("e", "val")
	mu.Lock()
	nfetch.assertNum(t, 5)
	mu.Unlock()
	time.Sleep(1100 * time.Millisecond)
	fetchtest("a", "val")
	fetchtest("b", "val")
	fetchtest("c", "val")
	fetchtest("d", "val")
	fetchtest("e", "val")
	time.Sleep(200 * time.Millisecond)
	// a, b, c have been refreshed by the 3 stale workers
	// d's refresh has been queued and done
	// e's refresh has been dropped because of full queue
	mu.Lock()
	nfetch.assertNum(t, 9)
	mu.Unlock()
	stats := c.Stats()
	if stats.RefreshQueued != 4 {
		t.Fatalf("expected 4 queued refreshes, got %d", stats.RefreshQueued)
	}
	if stats.RefreshDropped != 1 {
		t.Fatalf("expected 1 dropped refresh, got %d", stats.RefreshDropped)
	}
}

func TestBackendFail(t *testing.T) {
	var nfetch FetchNum
	var mu sync.RWMutex
	c, err := New(WithSize(10), WithTTL(1*time.Second), WithNegSize(3), WithNegTTL(1*time.Second), WithStaleFetchers(3),
		WithStaleQueueSize(1))
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}
	fetchtest := func(key, val string) (string, error) {
		v, err := c.Fetch(key, func() (interface{}, bool, error) {
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			nfetch++
			mu.Unlock()
			if val == "" {
				return "", false, errors.New("test")
			}
			if val == "flush" {
				return "", false, nil
			}
			return val, true, nil
		})
		if err != nil {
			return "", errors.New("fetch error: " + err.Error())
		}
		s, ok := v.(string)
		if !ok {
			return "", errors.New("value is not string")
		}
		return s, nil
	}

	var s string
	s, err = fetchtest("foo", "bar")
	if s != "bar" {
		t.Errorf("got \"%s\", expected \"bar\"", s)
	}
	time.Sleep(1100 * time.Millisecond)
	// test stale
	s, err = fetchtest("foo", "baz")
	if s != "bar" {
		t.Errorf("got \"%s\", expected \"bar\"", s)
	}
	time.Sleep(1100 * time.Millisecond)
	s, err = fetchtest("foo", "")
	if s != "baz" {
		t.Errorf("got \"%s\", expected \"baz\"", s)
	}
	time.Sleep(1100 * time.Millisecond)
	s, err = fetchtest("foo", "flush")
	if s != "baz" {
		t.Errorf("got \"%s\", expected \"baz\"", s)
	}
	time.Sleep(100 * time.Millisecond)
	s, err = fetchtest("foo", "flush")
	if s != "" {
		t.Errorf("got \"%s\", expected \"\"", s)
	}
}

func TestCacheConcurrency(t *testing.T) {
	targetConcurr := 10
	c, err := New(WithFetchers(targetConcurr))
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}
	var concurrencyCount int32
	var maxConcurr int32
	fetchtest := func(key, val string) (string, error) {
		v, err := c.Fetch(key, func() (interface{}, bool, error) {
			atomic.AddInt32(&concurrencyCount, 1)
			x := atomic.LoadInt32(&concurrencyCount)
			max := atomic.LoadInt32(&maxConcurr)
			if x > max {
				atomic.StoreInt32(&maxConcurr, x)
			}
			defer atomic.AddInt32(&concurrencyCount, -1)
			time.Sleep(10 * time.Millisecond)
			x = atomic.LoadInt32(&concurrencyCount)
			max = atomic.LoadInt32(&maxConcurr)
			if x > max {
				atomic.StoreInt32(&maxConcurr, x)
			}
			if key == "err" {
				return nil, false, errors.New("test error")
			}
			return val, true, nil
		})
		if err != nil {
			return "", errors.New("fetch error: " + err.Error())
		}
		s, ok := v.(string)
		if !ok {
			return "", errors.New("value is not string")
		}
		return s, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			s0, err := fetchtest(strconv.Itoa(rand.Intn(100000)), "val")
			if err != nil {
				t.Errorf("s0: got error %v", err)
			}
			if s0 != "val" {
				t.Errorf("bad value for s0 %s", s0)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if maxConcurr != int32(targetConcurr) {
		t.Errorf("got max concurrency %d, expected %d", maxConcurr, targetConcurr)
	}
}

func TestCacheTooSmall(t *testing.T) {
	targetConcurr := 10
	c, err := New(WithSize(0), WithFetchers(targetConcurr))
	if err != nil {
		t.Fatalf("failed creating cache: %v", err)
	}
	var concurrencyCount int32
	var maxConcurr int32
	fetchtest := func(key, val string) (string, error) {
		v, err := c.Fetch(key, func() (interface{}, bool, error) {
			atomic.AddInt32(&concurrencyCount, 1)
			x := atomic.LoadInt32(&concurrencyCount)
			max := atomic.LoadInt32(&maxConcurr)
			if x > max {
				atomic.StoreInt32(&maxConcurr, x)
			}
			defer atomic.AddInt32(&concurrencyCount, -1)
			time.Sleep(10 * time.Millisecond)
			x = atomic.LoadInt32(&concurrencyCount)
			max = atomic.LoadInt32(&maxConcurr)
			if x > max {
				atomic.StoreInt32(&maxConcurr, x)
			}
			if key == "err" {
				return nil, false, errors.New("test error")
			}
			return val, true, nil
		})
		if err != nil {
			return "", errors.New("fetch error: " + err.Error())
		}
		s, ok := v.(string)
		if !ok {
			return "", errors.New("value is not string")
		}
		return s, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			s0, err := fetchtest(strconv.Itoa(rand.Intn(100000)), "val")
			if err != nil {
				t.Errorf("s0: got error %v", err)
			}
			if s0 != "val" {
				t.Errorf("bad value for s0 %s", s0)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if maxConcurr != int32(targetConcurr) {
		t.Errorf("got max concurrency %d, expected %d", maxConcurr, targetConcurr)
	}
}
