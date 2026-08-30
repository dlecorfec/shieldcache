package shieldcache_test

import (
	"math/rand"
	"strconv"
	"sync"
	"time"
)

func fibo(n int) int {
	if n < 2 {
		return n
	}
	return fibo(n-1) + fibo(n-2)
}

func cachedFibo(c *fetchcache.Cache, n int) int {
	val, err := c.Fetch("fibo"+strconv.Itoa(n), func() (interface{}, bool, error) {
		return fibo(n), true, nil
	})
	if err != nil {
		panic(err.Error())
	}
	return val.(int)
}

func Example() {
	cache, err := fetchcache.New(fetchcache.WithSize(100), fetchcache.WithTTL(300*time.Second))
	if err != nil {
		// failed creating cache
		return
	}
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			cachedFibo(cache, rand.Intn(20)+2)
			wg.Done()
		}()
	}
	wg.Wait()
}
