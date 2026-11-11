// Package payload stores event payloads in object storage (zstd-compressed)
// and caches them in-process for fan-out and retries (design §3.1, §3.2, §8.3).
package payload

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/singleflight"
)

var ErrNotFound = errors.New("payload: not found")

var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	decoder, _ = zstd.NewReader(nil, zstd.WithDecoderMaxMemory(64<<20))
)

// Compress zstd-compresses b. Safe for concurrent use.
func Compress(b []byte) []byte { return encoder.EncodeAll(b, make([]byte, 0, len(b)/2)) }

// Decompress reverses Compress, refusing outputs larger than maxSize.
func Decompress(b []byte, maxSize int) ([]byte, error) {
	out, err := decoder.DecodeAll(b, nil)
	if err != nil {
		return nil, fmt.Errorf("payload: decompress: %w", err)
	}
	if len(out) > maxSize {
		return nil, errors.New("payload: decompressed size exceeds limit")
	}
	return out, nil
}

// Key builds the object key. The hash prefix spreads load across S3 partitions.
func Key(tenantID, eventID string, created time.Time) string {
	h := sha256.Sum256([]byte(eventID))
	return fmt.Sprintf("p/%s/%s/%s/%s.zst", hex.EncodeToString(h[:1]), tenantID, created.UTC().Format("20060102"), eventID)
}

// ObjectStore is the minimal blob API we need.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error) // ErrNotFound if absent
}

// MemStore is an in-memory ObjectStore for tests.
type MemStore struct {
	mu sync.Mutex
	m  map[string][]byte
}

func NewMemStore() *MemStore { return &MemStore{m: map[string][]byte{}} }

func (s *MemStore) Put(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = bytes.Clone(data)
	return nil
}

func (s *MemStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.m[key]
	if !ok {
		return nil, ErrNotFound
	}
	return b, nil
}

func (s *MemStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
}

// Cache is a byte-bounded LRU of compressed payloads keyed by event ID.
type Cache struct {
	mu       sync.Mutex
	maxBytes int64
	size     int64
	ll       *list.List
	items    map[string]*list.Element
}

type cacheEntry struct {
	key string
	val []byte
}

func NewCache(maxBytes int64) *Cache {
	return &Cache{maxBytes: maxBytes, ll: list.New(), items: map[string]*list.Element{}}
}

func (c *Cache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		c.ll.MoveToFront(e)
		return e.Value.(*cacheEntry).val, true
	}
	return nil, false
}

func (c *Cache) Add(key string, val []byte) {
	n := int64(len(val))
	if n > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		c.ll.MoveToFront(e)
		return
	}
	c.items[key] = c.ll.PushFront(&cacheEntry{key, val})
	c.size += n
	for c.size > c.maxBytes {
		e := c.ll.Back()
		ent := e.Value.(*cacheEntry)
		c.ll.Remove(e)
		delete(c.items, ent.key)
		c.size -= int64(len(ent.val))
	}
}

// Fetcher resolves compressed payloads: cache, then object store. Concurrent
// fetches of the same event are collapsed into one object-store GET.
type Fetcher struct {
	Store ObjectStore
	Cache *Cache
	group singleflight.Group
	OnHit func(hit bool) // optional metrics hook
}

func (f *Fetcher) Get(ctx context.Context, eventID, key string) ([]byte, error) {
	if b, ok := f.Cache.Get(eventID); ok {
		f.observe(true)
		return b, nil
	}
	f.observe(false)
	v, err, _ := f.group.Do(eventID, func() (any, error) {
		b, err := f.Store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		f.Cache.Add(eventID, b)
		return b, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

func (f *Fetcher) observe(hit bool) {
	if f.OnHit != nil {
		f.OnHit(hit)
	}
}

// readAllLimit reads at most limit bytes from r.
func readAllLimit(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errors.New("payload: object exceeds size limit")
	}
	return b, nil
}
