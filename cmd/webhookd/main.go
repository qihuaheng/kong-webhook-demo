// Command webhookd is the reliable webhook delivery service.
//
//	webhookd api        HTTP API: ingest, endpoint and DLQ management
//	webhookd worker     queue consumer: signs and delivers webhooks
//	webhookd scheduler  delayed-retry mover, reconciler, partitions, DLQ notifier
//	webhookd all        every role in one process (local development)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"webhookd/internal/api"
	"webhookd/internal/breaker"
	"webhookd/internal/config"
	"webhookd/internal/dispatch"
	"webhookd/internal/ingest"
	"webhookd/internal/notify"
	"webhookd/internal/payload"
	"webhookd/internal/queue"
	"webhookd/internal/retry"
	"webhookd/internal/scheduler"
	"webhookd/internal/secretbox"
	"webhookd/internal/ssrf"
	"webhookd/internal/store"
	"webhookd/internal/telemetry"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: webhookd api|worker|scheduler|all")
		os.Exit(2)
	}
	role := os.Args[1]
	switch role {
	case "api", "worker", "scheduler", "all":
	default:
		fmt.Fprintf(os.Stderr, "unknown role %q\n", role)
		os.Exit(2)
	}
	if err := run(role); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(role string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	shutdownTracing, err := telemetry.Setup(ctx, role, cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background())

	box, err := secretbox.New(cfg.MasterKey)
	if err != nil {
		return err
	}
	startCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	st, err := retryOpen(startCtx, func(ctx context.Context) (*store.Store, error) {
		return store.Open(ctx, cfg.PostgresDSN, 50, box)
	})
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer st.Close()
	if err := st.Migrate(startCtx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: cfg.RedisAddrs, Password: cfg.RedisPass, MasterName: cfg.RedisMasterName, IsClusterMode: cfg.RedisCluster,
		PoolSize: cfg.WorkerConcurrency/4 + 64, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second})
	defer rdb.Close()
	q := queue.New(rdb, cfg.Shards, "whk")
	if _, err := retryOpen(startCtx, func(ctx context.Context) (struct{}, error) { return struct{}{}, q.Init(ctx) }); err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	s3, err := payload.NewS3Store(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket, cfg.S3Region, cfg.S3UseSSL)
	if err != nil {
		return err
	}
	if role == "api" || role == "all" {
		days := int(cfg.PayloadRetention.Hours()/24 + 0.5)
		if _, err := retryOpen(startCtx, func(ctx context.Context) (struct{}, error) {
			return struct{}{}, s3.EnsureBucket(ctx, days)
		}); err != nil {
			return fmt.Errorf("object storage: %w", err)
		}
	}

	guard := &ssrf.Guard{Allow: cfg.SSRFAllow}
	sender := dispatch.NewSender(guard, cfg.AllowHTTP, 64)
	host, _ := os.Hostname()
	ready := func(ctx context.Context) error {
		if err := st.Ping(ctx); err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
		if err := rdb.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		return s3.Ping(ctx)
	}

	var wg sync.WaitGroup
	errc := make(chan error, 4)
	launch := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errc <- fmt.Errorf("%s: %w", name, err)
			}
		}()
	}

	if role == "worker" || role == "all" {
		fetch := &payload.Fetcher{Store: s3, Cache: payload.NewCache(cfg.PayloadCacheBytes)}
		w := dispatch.NewWorker(dispatch.WorkerConfig{
			Consumer: host + "-" + fmt.Sprint(os.Getpid()), Concurrency: cfg.WorkerConcurrency,
			EndpointConcurrency: cfg.EndpointConcurrency, ClaimIdle: cfg.ClaimIdle,
			MaxPayloadBytes: cfg.MaxPayloadBytes, DefaultPolicy: retry.Default, DrainTimeout: cfg.ShutdownTimeout,
		}, st, q, breaker.New(rdb, "whk", cfg.BreakerThreshold, cfg.BreakerBaseOpen, cfg.BreakerMaxOpen), fetch, sender)
		launch("worker", w.Run)
	}
	if role == "scheduler" || role == "all" {
		sc := scheduler.New(scheduler.Config{ReconcileInterval: cfg.ReconcileInterval,
			ReconcileStaleAfter: cfg.ReconcileStaleAfter, MetadataRetention: cfg.MetadataRetention}, st, q)
		launch("scheduler", sc.Run)
		n := notify.New(q, st, sender, cfg.NotifyWindow, host)
		launch("notifier", n.Run)
	}

	var srv *http.Server
	var in *ingest.Ingester
	// The ingester outlives ctx so requests still in flight during HTTP
	// shutdown can commit; it is stopped after the server has drained.
	ingestCtx, stopIngest := context.WithCancel(context.WithoutCancel(ctx))
	defer stopIngest()
	if role == "api" || role == "all" {
		in = ingest.New(ingest.Config{MaxPayloadBytes: cfg.MaxPayloadBytes, InlineThreshold: cfg.InlineThreshold}, st, s3, q)
		in.Start(ingestCtx)
		h := api.New(api.Config{AdminKey: cfg.AdminKey, AllowHTTP: cfg.AllowHTTP, MaxPayloadBytes: cfg.MaxPayloadBytes,
			SecretGrace: cfg.SecretGrace, DefaultPolicy: retry.Default}, st, q, in, s3, guard, ready).Handler()
		srv = &http.Server{Addr: cfg.HTTPAddr, Handler: h, ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second}
	} else {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
			if err := ready(r.Context()); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.Write([]byte("ok"))
		})
		srv = &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	}
	go func() {
		slog.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("http: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutting down")
	case err := <-errc:
		slog.Error("component failed; shutting down", "err", err)
		stop()
	}
	sctx, scancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer scancel()
	srv.Shutdown(sctx) // stop accepting; finish in-flight requests
	stopIngest()
	if in != nil {
		in.Wait() // flush pending group commits
	}
	wg.Wait()
	slog.Info("stopped")
	return nil
}

// retryOpen retries fn with backoff until it succeeds or ctx expires, so
// pods tolerate dependencies that start slightly later.
func retryOpen[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	delay := 500 * time.Millisecond
	for {
		v, err := fn(ctx)
		if err == nil {
			return v, nil
		}
		slog.Warn("dependency not ready; retrying", "err", err)
		select {
		case <-ctx.Done():
			return v, err
		case <-time.After(delay):
		}
		delay = min(delay*2, 5*time.Second)
	}
}
