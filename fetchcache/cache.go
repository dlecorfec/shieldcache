// Package shieldcache provides an in-memory LRU cache with extra features inspired by nginx caching.
// It uses an internal LRU implementation with TTL support.
//
// It adds cache locking (prevents 2 concurrent fetches for the same item),
// infinite serving of stale values in case of fetch errors,
// asynchronous refresh of stale values,
// concurrency-limited refresh fetchers,
// and negative cache.
//
// Its usage makes use of a single function Fetch() (no Get()/Set()), which is provided
// with a closure capturing the parameters necessary to fetch for the given key.
package shieldcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Cache is the type supporting the local caching of objects.
type Cache struct {
	posCache     lruStore      // positive cache: store valid entries
	posSize      int32         // how many max cached entries
	posPruneSize int32         // how many entries to evict on cache full
	posTTL       time.Duration // how long until a positive entry is considered stale

	negCache     lruStore      // negative cache: store currently invalid entries
	negSize      int32         // how many max neg cached entries
	negPruneSize int32         // how many entries to evict on cache full
	negTTL       time.Duration // how long until a neg entry is considered stale
	negStale     bool          // allow serving stale negative cache entries once

	fetching  map[string]struct{} // is an item being fetched?
	fetchLock sync.Mutex          // guard access to "fetching" map
	fetchCond *sync.Cond          // for waking up goroutines waiting for a fetcher

	fetchQueue  chan fetchReq       // queue for async fetches
	queued      map[string]struct{} // is an item already queued?
	queuedLock  sync.Mutex          // guard access to "queued" map
	done        chan struct{}       // signals background workers to stop
	closeOnce   sync.Once           // makes Close idempotent
	workers     sync.WaitGroup      // waits for background workers
	workersDone chan struct{}       // closed when background workers have stopped
	closed      uint32              // has Close been called?

	staleFetchers  int // number of fetcher goroutines
	staleQueueSize int // size of the chan storing async fetch request

	staleValidator func(interface{}, time.Duration) bool // do serve stale item given its stale age?
	canUseStale    bool                                  // allow serving stale content

	maxFetchers  int           // max number of concurrent fetches
	fetchLimiter chan struct{} // used as a semaphore for concurrency limit
	shards       int           // number of shards in each LRU store

	// instrumentation
	hits           uint64 // cache hit counter
	requests       uint64 // requests (hit+miss) counter
	newFetches     uint64 // fetch counter for item not yet in cache
	staleFetches   uint64 // fetch counter for refreshing expired items
	staleHits      uint64 // stale positive cache hit counter
	negativeHits   uint64 // negative cache hit counter
	fetchErrors    uint64 // foreground fetch error counter
	refreshErrors  uint64 // background refresh error counter
	refreshQueued  uint64 // queued background refresh counter
	refreshDropped uint64 // dropped background refresh counter
}

// Fetcher is the type of the closure passed to Fetch() for fetching the desired object if missing or stale.
//
// Depending on the boolean validity and error returned by the closure,
// the entry will end in the positive or the negative cache (and possibly removed from the other).
//
// If non-nil error, entry will be stored in negative cache (but positive entry will be untouched -
// use this for transient backend failures);
// else if validity is false, will be stored in negative cache and positive entry will be deleted
// (use this case when the item is not "positive" anymore - has invalid state or has been removed);
// else (nil error nil and true validity) store in positive cache and remove neg entry
type Fetcher func() (interface{}, bool, error)

// ContextFetcher is the type of the closure passed to FetchContext() for fetching
// the desired object if missing or stale.
type ContextFetcher func(context.Context) (interface{}, bool, error)

// Stats is an atomic snapshot of cache counters and current cache sizes.
type Stats struct {
	Requests       uint64
	Hits           uint64
	Misses         uint64
	NewFetches     uint64
	RefreshFetches uint64
	StaleHits      uint64
	NegativeHits   uint64
	FetchErrors    uint64
	RefreshErrors  uint64
	RefreshQueued  uint64
	RefreshDropped uint64
	PositiveSize   int64
	NegativeSize   int64
}

// fetchReq stores a fetch request for async refresh.
type fetchReq struct {
	key  string
	f    Fetcher
	fctx ContextFetcher
}

// negCacheEntry stores an invalid fetch result for negative caching
type negCacheEntry struct {
	x   interface{}
	err error
}

// Option is the type of option passed to the constructor.
type Option func(c *Cache)

// ErrClosed is returned by Fetch after Close has been called.
var ErrClosed = errors.New("shieldcache: cache is closed")

// WithSize sets the max cache size (in number of objets). When the cache is full,
// a portion of the least recently used objets will be evicted.
// Default: 5000
func WithSize(n int32) Option {
	return func(c *Cache) {
		c.posSize = n
	}
}

// WithPruneSize sets the number of entries to evict when the positive cache is full.
// Default: posSize/20
func WithPruneSize(n int32) Option {
	return func(c *Cache) {
		c.posPruneSize = n
	}
}

// WithTTL sets the duration upon which an object will be deemed expired.
// In this case, a background refresh will occur.
// Default: 60 * time.Second
func WithTTL(t time.Duration) Option {
	return func(c *Cache) {
		c.posTTL = t
	}
}

// WithNegSize sets the size of the negative cache, used for storing errors and invalid objects.
// Default: 500
func WithNegSize(n int32) Option {
	return func(c *Cache) {
		c.negSize = n
	}
}

// WithNegPruneSize sets the number of entries to evict when the negative cache is full.
// Default: negSize/20
func WithNegPruneSize(n int32) Option {
	return func(c *Cache) {
		c.negPruneSize = n
	}
}

// WithNegTTL sets the duration of a negative cache entry.
// Default: 5 * time.Second
func WithNegTTL(t time.Duration) Option {
	return func(c *Cache) {
		c.negTTL = t
	}
}

// WithNegStale allows expired negative cache entries to be served once
// before being removed. If false, expired negative entries are treated as misses.
// Default: false
func WithNegStale(useStale bool) Option {
	return func(c *Cache) {
		c.negStale = useStale
	}
}

// WithStaleFetchers sets the number of fetchers in the pool for async fetch of stale entries.
// Default: 3
func WithStaleFetchers(n int) Option {
	return func(c *Cache) {
		c.staleFetchers = n
	}
}

// WithStaleQueueSize sets the size of the chan for queuing items.
// Default: 1000
func WithStaleQueueSize(n int) Option {
	return func(c *Cache) {
		c.staleQueueSize = n
	}
}

// WithFetchers sets the max number of concurrent fetches.
// Default: 100
func WithFetchers(n int) Option {
	return func(c *Cache) {
		c.maxFetchers = n
	}
}

// WithShards sets the number of shards used by the positive and negative LRU stores.
// A value of 1 uses a single LRU store.
// Default: 1
func WithShards(n int) Option {
	return func(c *Cache) {
		c.shards = n
	}
}

// WithStale allows to decide globally if expired items are served.
// If false, WithStaleValidator will be ignored.
// Default: true
func WithStale(useStale bool) Option {
	return func(c *Cache) {
		c.canUseStale = useStale
	}
}

// WithStaleValidator allows to use a function to decide if a stale item
// will be served. The duration is the extra time after expiration.
// Default: nil (WithStale() will decide if stale items are served)
func WithStaleValidator(f func(interface{}, time.Duration) bool) Option {
	return func(c *Cache) {
		c.staleValidator = f
	}
}

// New builds a cache given some options.
func New(opts ...Option) (*Cache, error) {
	c := &Cache{
		posSize:        5000,
		posTTL:         60 * time.Second,
		posPruneSize:   0,
		negSize:        500,
		negTTL:         5 * time.Second,
		negPruneSize:   0,
		negStale:       false,
		staleFetchers:  3,
		staleQueueSize: 1000,
		maxFetchers:    100,
		shards:         1,
		canUseStale:    true,
	}

	for _, o := range opts {
		o(c)
	}
	if err := c.validateOptions(); err != nil {
		return nil, err
	}

	c.fetchLimiter = make(chan struct{}, c.maxFetchers)

	if c.posPruneSize == 0 {
		c.posPruneSize = c.posSize/20 + 1
	}
	if c.negPruneSize == 0 {
		c.negPruneSize = c.negSize/20 + 1
	}

	c.posCache = newLRUStore(int64(c.posSize), uint32(c.posPruneSize), c.shards)
	c.negCache = newLRUStore(int64(c.negSize), uint32(c.negPruneSize), c.shards)

	// for cache locking
	c.fetching = make(map[string]struct{})
	c.fetchCond = sync.NewCond(&c.fetchLock)

	// for async stale fetch
	c.fetchQueue = make(chan fetchReq, c.staleQueueSize)
	c.queued = make(map[string]struct{})
	c.done = make(chan struct{})
	c.workersDone = make(chan struct{})
	for i := 0; i < c.staleFetchers; i++ {
		c.workers.Add(1)
		go c.staleFetcher()
	}
	return c, nil
}

// Close stops background refresh workers. It is safe to call multiple times.
// In-flight fetches are allowed to finish, but future calls to Fetch return ErrClosed.
func (c *Cache) Close() {
	_ = c.CloseContext(context.Background())
}

// CloseContext stops background refresh workers and waits until they finish or
// ctx is done. Shutdown continues in the background if ctx expires.
func (c *Cache) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.closeOnce.Do(func() {
		atomic.StoreUint32(&c.closed, 1)
		c.fetchLock.Lock()
		c.fetchCond.Broadcast()
		c.fetchLock.Unlock()
		c.queuedLock.Lock()
		close(c.done)
		c.queuedLock.Unlock()
		go func() {
			c.workers.Wait()
			close(c.workersDone)
		}()
	})
	select {
	case <-c.workersDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Cache) validateOptions() error {
	if c.posSize < 0 {
		return errors.New("positive cache size must be greater than or equal to 0")
	}
	if c.posPruneSize < 0 {
		return errors.New("positive cache prune size must be greater than or equal to 0")
	}
	if c.posTTL < 0 {
		return errors.New("positive cache TTL must be greater than or equal to 0")
	}
	if c.negSize < 0 {
		return errors.New("negative cache size must be greater than or equal to 0")
	}
	if c.negPruneSize < 0 {
		return errors.New("negative cache prune size must be greater than or equal to 0")
	}
	if c.negTTL < 0 {
		return errors.New("negative cache TTL must be greater than or equal to 0")
	}
	if c.staleFetchers <= 0 {
		return errors.New("stale fetchers must be greater than 0")
	}
	if c.staleQueueSize < 0 {
		return errors.New("stale queue size must be greater than or equal to 0")
	}
	if c.maxFetchers <= 0 {
		return errors.New("fetchers must be greater than 0")
	}
	if c.shards <= 0 {
		return errors.New("shards must be greater than 0")
	}
	return nil
}

// Fetch returns an object given its cache key and a Fetcher function for fetching it if
// it expired or missing.
//
// This function will usually be implemented as a closure in order to capture
// the various parameters needed for fetching from the backend the entry
// corresponding to the cache key.
//
// Entries are first looked up in the positive cache, then negative, then fetched.
//
// An asynchronous fetch will happen if the entry is stale.
func (c *Cache) Fetch(key string, f Fetcher) (interface{}, error) {
	if atomic.LoadUint32(&c.closed) != 0 {
		return nil, ErrClosed
	}
	atomic.AddUint64(&c.requests, 1)
	item, cached, err, refresh := c.tryCache(key)
	if refresh {
		c.enqueueFetch(key, f, nil)
	}
	if cached {
		atomic.AddUint64(&c.hits, 1)
		return item, err
	}

	c.fetchLock.Lock()
	_, fetching := c.fetching[key]
	if !fetching {
		c.fetching[key] = struct{}{}
		c.fetchLock.Unlock()
		if err := c.acquireFetch(context.Background()); err != nil {
			c.endFetch(key)
			c.fetchCond.Broadcast()
			return nil, err
		}
		atomic.AddUint64(&c.newFetches, 1)
		item, err = c.cacheItem(key, f)
		if err != nil {
			atomic.AddUint64(&c.fetchErrors, 1)
		}
		<-c.fetchLimiter
		c.endFetch(key)
		c.fetchCond.Broadcast()
		return item, err
	}

	for {
		if atomic.LoadUint32(&c.closed) != 0 {
			c.fetchLock.Unlock()
			return nil, ErrClosed
		}
		c.fetchCond.Wait()
		_, ok := c.fetching[key]
		if !ok {
			break
		}
	}
	c.fetchLock.Unlock()

	item, cached, err, refresh = c.tryCache(key)
	if refresh {
		c.enqueueFetch(key, f, nil)
	}
	if cached {
		return item, err
	}
	if err := c.acquireFetch(context.Background()); err != nil {
		return nil, err
	}
	item, _, err = f()
	if err != nil {
		atomic.AddUint64(&c.fetchErrors, 1)
	}
	<-c.fetchLimiter
	return item, err
}

// FetchContext is like Fetch, but the context can cancel waiting for another
// goroutine's fetch, waiting for the fetch concurrency limiter, and the fetcher
// itself.
func (c *Cache) FetchContext(ctx context.Context, key string, f ContextFetcher) (interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if atomic.LoadUint32(&c.closed) != 0 {
		return nil, ErrClosed
	}
	atomic.AddUint64(&c.requests, 1)
	item, cached, err, refresh := c.tryCache(key)
	if refresh {
		c.enqueueFetch(key, nil, f)
	}
	if cached {
		atomic.AddUint64(&c.hits, 1)
		// fresh or stale
		return item, err
	}

	// entry not in cache
	c.fetchLock.Lock()
	_, fetching := c.fetching[key]
	if !fetching {
		// nobody is fetching it yet, let's do it
		c.fetching[key] = struct{}{}
		c.fetchLock.Unlock()
		if err := c.acquireFetch(ctx); err != nil {
			c.endFetch(key)
			c.fetchCond.Broadcast()
			return nil, err
		}
		atomic.AddUint64(&c.newFetches, 1)
		item, err = c.cacheItemContext(ctx, key, f)
		if err != nil {
			atomic.AddUint64(&c.fetchErrors, 1)
		}
		<-c.fetchLimiter
		c.endFetch(key)
		c.fetchCond.Broadcast()
		return item, err
	}
	// wait for the fetcher to finish
	stopContextBroadcast := context.AfterFunc(ctx, func() {
		c.fetchLock.Lock()
		c.fetchCond.Broadcast()
		c.fetchLock.Unlock()
	})
	defer stopContextBroadcast()
	for {
		if err := ctx.Err(); err != nil {
			c.fetchLock.Unlock()
			return nil, err
		}
		if atomic.LoadUint32(&c.closed) != 0 {
			c.fetchLock.Unlock()
			return nil, ErrClosed
		}
		c.fetchCond.Wait()
		_, ok := c.fetching[key]
		if !ok {
			break
		}
	}
	c.fetchLock.Unlock()

	// get the hopefully newly cached entry
	item, cached, err, refresh = c.tryCache(key)
	if refresh {
		c.enqueueFetch(key, nil, f)
	}
	if cached {
		return item, err
	}
	// last resort (if too small a cache)
	if err := c.acquireFetch(ctx); err != nil {
		return nil, err
	}
	item, _, err = f(ctx)
	if err != nil {
		atomic.AddUint64(&c.fetchErrors, 1)
	}
	<-c.fetchLimiter
	return item, err
}

func (c *Cache) acquireFetch(ctx context.Context) error {
	select {
	case c.fetchLimiter <- struct{}{}:
		return nil
	case <-c.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// tryCache tries to find the given key in the positive and negative caches.
// If an element is expired, it will be queued for async fetch and its stale
// version will be returned immediately.
// The boolean in the return value indicates if the key has been found in cache.
func (c *Cache) tryCache(key string) (interface{}, bool, error, bool) {
	item := c.posCache.Get(key)
	if item != nil {
		refresh := false
		valid := true
		if item.Expired() {
			refresh = true
			if !c.useStale(item) {
				valid = false
			}
		}
		if valid { // not expired or can use stale
			if refresh {
				atomic.AddUint64(&c.staleHits, 1)
			}
			return item.Value(), true, nil, refresh
		}
		// if cannot use stale
		return nil, false, nil, refresh
	}

	item = c.negCache.Get(key)
	if item != nil {
		if item.Expired() {
			// stale negative, remove it from cache
			c.negCache.Delete(key)
			if !c.negStale {
				return nil, false, nil, false
			}
		}
		ne := item.Value().(*negCacheEntry)
		atomic.AddUint64(&c.negativeHits, 1)
		return ne.x, true, ne.err, false
	}
	return nil, false, nil, false
}

// enqueueFetch puts a fetch request in the queue.
// It does nothing if the request is already in the queue, or if the queue is full.
func (c *Cache) enqueueFetch(key string, f Fetcher, fctx ContextFetcher) {
	c.queuedLock.Lock()
	select {
	case <-c.done:
		c.queuedLock.Unlock()
		return
	default:
	}
	_, ok := c.queued[key]
	if !ok {
		select {
		case c.fetchQueue <- fetchReq{key: key, f: f, fctx: fctx}:
			c.queued[key] = struct{}{}
			atomic.AddUint64(&c.refreshQueued, 1)
		default:
			// drop request on full queue instead of blocking
			atomic.AddUint64(&c.refreshDropped, 1)
		}
	}
	c.queuedLock.Unlock()
}

// staleFetcher grabs a fetch request from the chan and executes it.
// Requests are guaranteed to be unique in the queue (using "queued" map): no need to protect
// against multiple concurrent fetches, no need to use cache locking.
func (c *Cache) staleFetcher() {
	defer c.workers.Done()
	for {
		select {
		case fr := <-c.fetchQueue:
			// fetch it
			atomic.AddUint64(&c.staleFetches, 1)
			var err error
			if fr.fctx != nil {
				_, err = c.cacheItemContext(context.Background(), fr.key, fr.fctx)
			} else {
				_, err = c.cacheItem(fr.key, fr.f)
			}
			if err != nil {
				atomic.AddUint64(&c.refreshErrors, 1)
			}
			c.endQueuing(fr.key)
		case <-c.done:
			return
		}
	}
}

// cacheItem fetches an object using the supplied closure and stores it
// in cache using the supplied key. Depending on the error and validity returned by
// the closure, the entry will end in either the positive or the negative cache.
// if error not nil, store in negative cache (but keep positive entry);
// else if validity is false, store in negative cache and delete positive entry;
// else (error nil and validity true) store in positive cache and remove neg entry
func (c *Cache) cacheItem(key string, f Fetcher) (interface{}, error) {
	item, valid, err := f()
	c.storeFetchedItem(key, item, valid, err)
	return item, err
}

func (c *Cache) cacheItemContext(ctx context.Context, key string, f ContextFetcher) (interface{}, error) {
	item, valid, err := f(ctx)

	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
			return item, err
		}
	}
	c.storeFetchedItem(key, item, valid, err)
	return item, err
}

func (c *Cache) storeFetchedItem(key string, item interface{}, valid bool, err error) {
	if err != nil {
		c.negCache.Set(key, &negCacheEntry{item, err}, c.negTTL)
	} else if !valid {
		c.negCache.Set(key, &negCacheEntry{item, err}, c.negTTL)
		c.posCache.Delete(key)
	} else {
		c.posCache.Set(key, item, c.posTTL)
		c.negCache.Delete(key)
	}
}

// endQueuing marks an item as not being in the fetch queue anymore.
func (c *Cache) endQueuing(key string) {
	c.queuedLock.Lock()
	delete(c.queued, key)
	c.queuedLock.Unlock()
}

// endFetch marks an item as not being fetched anymore.
func (c *Cache) endFetch(key string) {
	c.fetchLock.Lock()
	delete(c.fetching, key)
	c.fetchLock.Unlock()
}

// useStale decides if we serve a stale item. The behavior can be modified globally
// with WithStale, or per item with WithStaleValidator. The duration passed to the
// validator is the time elapsed since cache expiration.
func (c *Cache) useStale(item *Item) bool {
	if !c.canUseStale {
		return false
	}
	if c.staleValidator != nil {
		return c.staleValidator(item.Value(), -item.TTL())
	}
	return true
}

// Hits returns the number of cache hits since start.
func (c *Cache) Hits() uint64 {
	return atomic.LoadUint64(&c.hits)
}

// Requests returns the number of cache requests (hits and misses) since start.
func (c *Cache) Requests() uint64 {
	return atomic.LoadUint64(&c.requests)
}

// NewFetches returns the number of fetches for items not in cache, since start.
func (c *Cache) NewFetches() uint64 {
	return atomic.LoadUint64(&c.newFetches)
}

// RefreshFetches returns the number of background refresh fetches for expired items, since start.
func (c *Cache) RefreshFetches() uint64 {
	return atomic.LoadUint64(&c.staleFetches)
}

// StaleFetches returns the number of fetches for expired items, since start.
// Deprecated: use RefreshFetches.
func (c *Cache) StaleFetches() uint64 {
	return c.RefreshFetches()
}

// Stats returns an atomic snapshot of cache counters and current cache sizes.
func (c *Cache) Stats() Stats {
	requests := atomic.LoadUint64(&c.requests)
	hits := atomic.LoadUint64(&c.hits)
	misses := requests - hits
	return Stats{
		Requests:       requests,
		Hits:           hits,
		Misses:         misses,
		NewFetches:     atomic.LoadUint64(&c.newFetches),
		RefreshFetches: atomic.LoadUint64(&c.staleFetches),
		StaleHits:      atomic.LoadUint64(&c.staleHits),
		NegativeHits:   atomic.LoadUint64(&c.negativeHits),
		FetchErrors:    atomic.LoadUint64(&c.fetchErrors),
		RefreshErrors:  atomic.LoadUint64(&c.refreshErrors),
		RefreshQueued:  atomic.LoadUint64(&c.refreshQueued),
		RefreshDropped: atomic.LoadUint64(&c.refreshDropped),
		PositiveSize:   c.posCache.Size(),
		NegativeSize:   c.negCache.Size(),
	}
}
