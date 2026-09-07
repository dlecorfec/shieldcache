package shieldcache

import (
	"context"
	"errors"
	"testing"
)

func TestFetchTyped(t *testing.T) {
	cache, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	value, err := Fetch(cache, "key", func() (string, bool, error) {
		return "value", true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if value != "value" {
		t.Fatalf("got %q, want %q", value, "value")
	}

	value, err = Fetch(cache, "key", func() (string, bool, error) {
		t.Fatal("fetcher called for cached value")
		return "", false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if value != "value" {
		t.Fatalf("got %q, want %q", value, "value")
	}
}

func TestFetchTypedInvalidValue(t *testing.T) {
	cache, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	value, err := Fetch(cache, "missing", func() (string, bool, error) {
		return "", false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if value != "" {
		t.Fatalf("got %q, want zero value", value)
	}
}

func TestFetchTypedError(t *testing.T) {
	cache, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	wantErr := errors.New("backend failure")
	value, err := Fetch(cache, "key", func() (int, bool, error) {
		return 0, false, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got error %v, want %v", err, wantErr)
	}
	if value != 0 {
		t.Fatalf("got %d, want zero value", value)
	}
}

func TestFetchContextTypedCancellation(t *testing.T) {
	cache, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	value, err := FetchContext(ctx, cache, "key", func(context.Context) (string, bool, error) {
		called = true
		return "value", true, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got error %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("fetcher called after context cancellation")
	}
	if value != "" {
		t.Fatalf("got %q, want zero value", value)
	}
}
