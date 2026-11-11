// Package testenv provides isolated real dependencies for integration tests.
// Tests are skipped unless WEBHOOKD_IT=1. Start deps with:
//
//	docker compose -f deploy/docker-compose.yml up -d postgres redis minio
//
// Set WEBHOOKD_IT_REDIS_MODE=cluster to run every test against Redis
// Cluster instead of standalone Redis:
//
//	docker compose -f deploy/docker-compose.yml --profile test up -d redis-cluster
package testenv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"

	"webhookd/internal/payload"
	"webhookd/internal/secretbox"
	"webhookd/internal/store"
)

type Env struct {
	Store      *store.Store
	Redis      redis.UniversalClient // standalone or Cluster (WEBHOOKD_IT_REDIS_MODE)
	Prefix     string                // Redis key prefix unique to this test
	S3         *payload.S3Store
	Box        *secretbox.Box
	DSN        string
	RedisAddr  string
	S3Endpoint string
	S3Bucket   string
}

func get(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// New provisions a fresh database, Redis prefix, and bucket for t.
func New(t testing.TB) *Env {
	t.Helper()
	if os.Getenv("WEBHOOKD_IT") != "1" {
		t.Skip("integration test: set WEBHOOKD_IT=1 and start deploy/docker-compose.yml deps")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adminDSN := get("WEBHOOKD_IT_POSTGRES", "postgres://webhookd:webhookd@127.0.0.1:55432/webhookd?sslmode=disable")
	id := randHex(4)
	dbName := "whk_it_" + id
	admin, err := ConnectPrimary(ctx, adminDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatal(err)
	}
	admin.Close(ctx)
	u, _ := url.Parse(adminDSN)
	u.Path = "/" + dbName
	dsn := u.String()

	key := make([]byte, 32)
	rand.Read(key)
	box, _ := secretbox.New(key)
	st, err := store.Open(ctx, dsn, 20, box)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	var rdb redis.UniversalClient
	raddr := get("WEBHOOKD_IT_REDIS", "127.0.0.1:56379")
	if ClusterMode() {
		raddr = get("WEBHOOKD_IT_REDIS_CLUSTER", "127.0.0.1:47000")
		rdb = redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{raddr}, IsClusterMode: true})
	} else {
		rdb = redis.NewClient(&redis.Options{Addr: raddr})
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis (%s): %v", raddr, err)
	}
	prefix := "it" + id

	s3ep := get("WEBHOOKD_IT_S3", "127.0.0.1:59000")
	bucket := "whk-it-" + id
	s3, err := payload.NewS3Store(s3ep, "minioadmin", "minioadmin", bucket, "us-east-1", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s3.EnsureBucket(ctx, 10); err != nil {
		t.Fatalf("minio: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		st.Close()
		for _, k := range ScanKeys(ctx, rdb, prefix+":*") {
			rdb.Del(ctx, k) // one key per DEL: multi-key DEL is CROSSSLOT on Cluster
		}
		rdb.Close()
		if a, err := ConnectPrimary(ctx, adminDSN); err == nil {
			a.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
			a.Close(ctx)
		}
		// Bucket is left to the lifecycle rule; MinIO data is disposable.
	})
	return &Env{Store: st, Redis: rdb, Prefix: prefix, S3: s3, Box: box, DSN: dsn,
		RedisAddr: raddr, S3Endpoint: s3ep, S3Bucket: bucket}
}

// Eventually polls cond until it returns true or timeout elapses.
func Eventually(t testing.TB, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, msg)
}

// Sanitize is a helper for test names used in IDs.
func Sanitize(s string) string { return strings.NewReplacer("/", "_", " ", "_").Replace(fmt.Sprint(s)) }

// NewRedisCluster connects to the single-node Redis Cluster started with
//
//	docker compose -f deploy/docker-compose.yml --profile test up -d redis-cluster
//
// and returns a cluster-mode client plus a unique key prefix. The cluster
// enforces CROSSSLOT rules on multi-key commands and Lua scripts exactly as
// a multi-node cluster does.
func NewRedisCluster(t testing.TB) (redis.UniversalClient, string) {
	t.Helper()
	if os.Getenv("WEBHOOKD_IT") != "1" {
		t.Skip("integration test: set WEBHOOKD_IT=1 and start the redis-cluster compose service")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{get("WEBHOOKD_IT_REDIS_CLUSTER", "127.0.0.1:47000")}, IsClusterMode: true})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis cluster: %v", err)
	}
	prefix := "itc" + randHex(4)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, k := range ScanKeys(ctx, rdb, prefix+":*") {
			rdb.Del(ctx, k)
		}
		rdb.Close()
	})
	return rdb, prefix
}

// ClusterMode reports whether tests should use Redis Cluster.
func ClusterMode() bool { return os.Getenv("WEBHOOKD_IT_REDIS_MODE") == "cluster" }

// ScanKeys returns keys matching pattern on every master (KEYS/SCAN on a
// cluster client only reach one node).
func ScanKeys(ctx context.Context, rdb redis.UniversalClient, pattern string) []string {
	var mu sync.Mutex
	var out []string
	scan := func(ctx context.Context, c *redis.Client) error {
		iter := c.Scan(ctx, 0, pattern, 1000).Iterator()
		for iter.Next(ctx) {
			mu.Lock()
			out = append(out, iter.Val())
			mu.Unlock()
		}
		return iter.Err()
	}
	if cc, ok := rdb.(*redis.ClusterClient); ok {
		cc.ForEachMaster(ctx, scan)
	} else if c, ok := rdb.(*redis.Client); ok {
		scan(ctx, c)
	}
	return out
}

// ConnectPrimary opens a single connection to the writable node of a
// (possibly multi-host) DSN. Admin operations such as CREATE DATABASE must
// not land on a standby.
func ConnectPrimary(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ValidateConnect = pgconn.ValidateConnectTargetSessionAttrsReadWrite
	return pgx.ConnectConfig(ctx, cfg)
}
