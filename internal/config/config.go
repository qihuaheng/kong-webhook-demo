// Package config loads webhookd configuration from environment variables.
package config

import (
	"encoding/base64"
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr    string // API listen address
	MetricsAddr string // metrics/health listen address for non-API roles
	PostgresDSN string
	// Redis topology (see UniversalOptions): one address = standalone;
	// WEBHOOKD_REDIS_MASTER_NAME set = Sentinel (addrs are sentinels);
	// several addresses or WEBHOOKD_REDIS_CLUSTER=true = Redis Cluster.
	RedisAddrs      []string
	RedisPass       string
	RedisMasterName string
	RedisCluster    bool

	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Bucket    string
	S3UseSSL    bool
	S3Region    string

	MasterKey []byte // 32 bytes, AES-256-GCM key for endpoint secrets
	AdminKey  string // bearer token for /admin

	Shards            int
	InlineThreshold   int // bytes of compressed payload inlined into stream messages
	MaxPayloadBytes   int
	PayloadCacheBytes int64

	WorkerConcurrency   int
	EndpointConcurrency int // default per-pod, per-endpoint cap
	ClaimIdle           time.Duration
	AllowHTTP           bool           // allow http:// endpoints (dev only)
	SSRFAllow           []netip.Prefix // CIDRs exempt from SSRF blocking (test only)
	SecretGrace         time.Duration
	BreakerThreshold    int
	BreakerBaseOpen     time.Duration
	BreakerMaxOpen      time.Duration
	PayloadRetention    time.Duration
	MetadataRetention   time.Duration
	ReconcileInterval   time.Duration
	ReconcileStaleAfter time.Duration
	NotifyWindow        time.Duration
	OTLPEndpoint        string // empty disables tracing export
	ShutdownTimeout     time.Duration
}

func Load() (*Config, error) {
	c := &Config{
		HTTPAddr:            env("WEBHOOKD_HTTP_ADDR", ":8080"),
		MetricsAddr:         env("WEBHOOKD_METRICS_ADDR", ":9090"),
		PostgresDSN:         env("WEBHOOKD_POSTGRES_DSN", "postgres://webhookd:webhookd@localhost:5432/webhookd?sslmode=disable"),
		RedisPass:           env("WEBHOOKD_REDIS_PASSWORD", ""),
		RedisMasterName:     env("WEBHOOKD_REDIS_MASTER_NAME", ""),
		RedisCluster:        envBool("WEBHOOKD_REDIS_CLUSTER", false),
		S3Endpoint:          env("WEBHOOKD_S3_ENDPOINT", "localhost:9000"),
		S3AccessKey:         env("WEBHOOKD_S3_ACCESS_KEY", "minioadmin"),
		S3SecretKey:         env("WEBHOOKD_S3_SECRET_KEY", "minioadmin"),
		S3Bucket:            env("WEBHOOKD_S3_BUCKET", "webhookd-payloads"),
		S3UseSSL:            envBool("WEBHOOKD_S3_USE_SSL", false),
		S3Region:            env("WEBHOOKD_S3_REGION", "us-east-1"),
		AdminKey:            env("WEBHOOKD_ADMIN_KEY", ""),
		Shards:              envInt("WEBHOOKD_SHARDS", 16),
		InlineThreshold:     envInt("WEBHOOKD_INLINE_THRESHOLD", 64<<10),
		MaxPayloadBytes:     envInt("WEBHOOKD_MAX_PAYLOAD_BYTES", 256<<10),
		PayloadCacheBytes:   int64(envInt("WEBHOOKD_PAYLOAD_CACHE_BYTES", 512<<20)),
		WorkerConcurrency:   envInt("WEBHOOKD_WORKER_CONCURRENCY", 1000),
		EndpointConcurrency: envInt("WEBHOOKD_ENDPOINT_CONCURRENCY", 50),
		ClaimIdle:           envDur("WEBHOOKD_CLAIM_IDLE", 60*time.Second),
		AllowHTTP:           envBool("WEBHOOKD_ALLOW_HTTP", false),
		SecretGrace:         envDur("WEBHOOKD_SECRET_GRACE", 24*time.Hour),
		BreakerThreshold:    envInt("WEBHOOKD_BREAKER_THRESHOLD", 10),
		BreakerBaseOpen:     envDur("WEBHOOKD_BREAKER_BASE_OPEN", 60*time.Second),
		BreakerMaxOpen:      envDur("WEBHOOKD_BREAKER_MAX_OPEN", 30*time.Minute),
		PayloadRetention:    envDur("WEBHOOKD_PAYLOAD_RETENTION", 10*24*time.Hour),
		MetadataRetention:   envDur("WEBHOOKD_METADATA_RETENTION", 30*24*time.Hour),
		ReconcileInterval:   envDur("WEBHOOKD_RECONCILE_INTERVAL", 30*time.Second),
		ReconcileStaleAfter: envDur("WEBHOOKD_RECONCILE_STALE_AFTER", 5*time.Minute),
		NotifyWindow:        envDur("WEBHOOKD_NOTIFY_WINDOW", 10*time.Minute),
		OTLPEndpoint:        env("WEBHOOKD_OTLP_ENDPOINT", ""),
		ShutdownTimeout:     envDur("WEBHOOKD_SHUTDOWN_TIMEOUT", 30*time.Second),
	}
	mk := os.Getenv("WEBHOOKD_MASTER_KEY")
	if mk == "" {
		return nil, fmt.Errorf("WEBHOOKD_MASTER_KEY is required (base64 of 32 random bytes)")
	}
	key, err := base64.StdEncoding.DecodeString(mk)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("WEBHOOKD_MASTER_KEY must be base64 of exactly 32 bytes")
	}
	c.MasterKey = key
	for _, a := range strings.Split(env("WEBHOOKD_REDIS_ADDR", "localhost:6379"), ",") {
		if a = strings.TrimSpace(a); a != "" {
			c.RedisAddrs = append(c.RedisAddrs, a)
		}
	}
	if len(c.RedisAddrs) == 0 {
		return nil, fmt.Errorf("WEBHOOKD_REDIS_ADDR must list at least one host:port")
	}
	for _, s := range strings.Split(os.Getenv("WEBHOOKD_SSRF_ALLOW_CIDRS"), ",") {
		if s = strings.TrimSpace(s); s == "" {
			continue
		}
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return nil, fmt.Errorf("WEBHOOKD_SSRF_ALLOW_CIDRS: %w", err)
		}
		c.SSRFAllow = append(c.SSRFAllow, p)
	}
	if c.Shards < 1 || c.Shards > 1024 {
		return nil, fmt.Errorf("WEBHOOKD_SHARDS must be in [1,1024]")
	}
	return c, nil
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func envBool(k string, def bool) bool {
	if v, err := strconv.ParseBool(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return v
	}
	return def
}
