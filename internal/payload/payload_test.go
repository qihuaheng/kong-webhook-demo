package payload

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompressRoundTrip(t *testing.T) {
	in := bytes.Repeat([]byte(`{"amount":100,"currency":"EUR"}`), 2000)
	c := Compress(in)
	if len(c) >= len(in) {
		t.Fatalf("expected compression, %d >= %d", len(c), len(in))
	}
	out, err := Decompress(c, len(in))
	if err != nil || !bytes.Equal(in, out) {
		t.Fatalf("roundtrip: %v", err)
	}
	if _, err := Decompress(c, len(in)-1); err == nil {
		t.Fatal("limit not enforced")
	}
}

func TestKey(t *testing.T) {
	k := Key("ten_1", "evt_1", time.Date(2026, 11, 11, 23, 0, 0, 0, time.UTC))
	if !strings.HasPrefix(k, "p/") || !strings.HasSuffix(k, "/ten_1/20261111/evt_1.zst") || len(strings.Split(k, "/")[1]) != 2 {
		t.Fatalf("bad key %s", k)
	}
}

func TestCacheEviction(t *testing.T) {
	c := NewCache(10)
	c.Add("a", []byte("12345"))
	c.Add("b", []byte("12345"))
	c.Get("a") // a is now most recent
	c.Add("c", []byte("12345"))
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should be evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should remain")
	}
	c.Add("big", make([]byte, 11))
	if _, ok := c.Get("big"); ok {
		t.Fatal("oversized entry must not be cached")
	}
}

type countingStore struct {
	ObjectStore
	gets atomic.Int32
}

func (s *countingStore) Get(ctx context.Context, k string) ([]byte, error) {
	s.gets.Add(1)
	time.Sleep(10 * time.Millisecond)
	return s.ObjectStore.Get(ctx, k)
}

func TestFetcherCollapsesAndCaches(t *testing.T) {
	mem := NewMemStore()
	mem.Put(context.Background(), "k", []byte("data"))
	cs := &countingStore{ObjectStore: mem}
	f := &Fetcher{Store: cs, Cache: NewCache(1 << 20)}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b, err := f.Get(context.Background(), "evt", "k"); err != nil || string(b) != "data" {
				t.Errorf("get: %v", err)
			}
		}()
	}
	wg.Wait()
	f.Get(context.Background(), "evt", "k")
	if n := cs.gets.Load(); n != 1 {
		t.Fatalf("expected 1 store GET, got %d", n)
	}
	if _, err := f.Get(context.Background(), "missing", "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
