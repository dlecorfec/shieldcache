package shieldcache

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
		return 42, false, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got error %v, want %v", err, wantErr)
	}
	if value != 42 {
		t.Fatalf("got %d, want fetched value", value)
	}

	value, err = Fetch(cache, "key", func() (int, bool, error) {
		t.Fatal("fetcher called for negative cache hit")
		return 0, false, nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got cached error %v, want %v", err, wantErr)
	}
	if value != 42 {
		t.Fatalf("got cached value %d, want 42", value)
	}
}

func TestFetchTypedTypeMismatch(t *testing.T) {
	cache, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	if _, err := Fetch(cache, "key", func() (string, bool, error) {
		return "value", true, nil
	}); err != nil {
		t.Fatal(err)
	}

	value, err := Fetch(cache, "key", func() (int, bool, error) {
		t.Fatal("fetcher called for cached value")
		return 0, false, nil
	})
	if err == nil {
		t.Fatal("expected a type mismatch error")
	}
	if !strings.Contains(err.Error(), "string") || !strings.Contains(err.Error(), "int") {
		t.Fatalf("expected error to describe string-to-int mismatch, got %v", err)
	}
	if value != 0 {
		t.Fatalf("got %d, want zero value", value)
	}
}

func TestFetchTypedInterfaceTypeMismatch(t *testing.T) {
	cache, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	if _, err := Fetch(cache, "key", func() (string, bool, error) {
		return "value", true, nil
	}); err != nil {
		t.Fatal(err)
	}

	value, err := Fetch[fmt.Stringer](cache, "key", func() (fmt.Stringer, bool, error) {
		t.Fatal("fetcher called for cached value")
		return nil, false, nil
	})
	if err == nil {
		t.Fatal("expected a type mismatch error")
	}
	if !strings.Contains(err.Error(), "string") || !strings.Contains(err.Error(), "fmt.Stringer") {
		t.Fatalf("expected error to describe string-to-fmt.Stringer mismatch, got %v", err)
	}
	if value != nil {
		t.Fatalf("got %v, want nil", value)
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
