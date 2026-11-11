// Package integration runs the full pipeline in-process (API, ingest,
// worker, scheduler, notifier) against real PostgreSQL, Redis, and MinIO.
// Run with WEBHOOKD_IT=1 after starting deploy/docker-compose.yml deps.
package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"testing"
	"time"

	"webhookd/internal/api"
	"webhookd/internal/breaker"
	"webhookd/internal/dispatch"
	"webhookd/internal/ingest"
	"webhookd/internal/notify"
	"webhookd/internal/payload"
	"webhookd/internal/queue"
	"webhookd/internal/retry"
	"webhookd/internal/scheduler"
	"webhookd/internal/ssrf"
	"webhookd/internal/testenv"
	"webhookd/pkg/sdk"
	"webhookd/pkg/webhook"
)

const adminKey = "it-admin-key"

type opts struct {
	noWorker         bool
	breakerThreshold int
	breakerOpen      time.Duration
	claimIdle        time.Duration
	inlineThreshold  int
}

type harness struct {
	t      *testing.T
	env    *testenv.Env
	q      *queue.Queue
	api    *httptest.Server
	admin  *sdk.Client
	client *sdk.Client // tenant client
	ctx    context.Context
	start  func() // starts the worker if noWorker was set
}

func newHarness(t *testing.T, o opts) *harness {
	t.Helper()
	env := testenv.New(t)
	if o.breakerThreshold == 0 {
		o.breakerThreshold = 1000 // effectively off unless a test wants it
	}
	if o.breakerOpen == 0 {
		o.breakerOpen = 2 * time.Second
	}
	if o.claimIdle == 0 {
		o.claimIdle = 2 * time.Second
	}
	if o.inlineThreshold == 0 {
		o.inlineThreshold = 64 << 10
	}
	ctx, cancel := context.WithCancel(context.Background())
	q := queue.New(env.Redis, 4, env.Prefix)
	if err := q.Init(ctx); err != nil {
		t.Fatal(err)
	}
	guard := &ssrf.Guard{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128")}}
	sender := dispatch.NewSender(guard, true, 16)

	in := ingest.New(ingest.Config{MaxPayloadBytes: 256 << 10, InlineThreshold: o.inlineThreshold}, env.Store, env.S3, q)
	in.Start(ctx)
	srv := httptest.NewServer(api.New(api.Config{AdminKey: adminKey, AllowHTTP: true, MaxPayloadBytes: 256 << 10,
		SecretGrace: time.Hour, DefaultPolicy: retry.Default}, env.Store, q, in, env.S3, guard,
		func(context.Context) error { return nil }).Handler())

	var wg sync.WaitGroup
	run := func(fn func(context.Context) error) {
		wg.Add(1)
		go func() { defer wg.Done(); fn(ctx) }()
	}
	sc := scheduler.New(scheduler.Config{MoveInterval: 50 * time.Millisecond, ReconcileInterval: 300 * time.Millisecond,
		ReconcileStaleAfter: time.Second, MetadataRetention: 24 * time.Hour}, env.Store, q)
	run(sc.Run)
	n := notify.New(q, env.Store, sender, 500*time.Millisecond, "it")
	run(n.Run)

	startWorker := func() {
		w := dispatch.NewWorker(dispatch.WorkerConfig{Consumer: "it-worker", Concurrency: 64, EndpointConcurrency: 16,
			ClaimIdle: o.claimIdle, MaxPayloadBytes: 256 << 10, DefaultPolicy: retry.Default,
			TargetCacheTTL: 100 * time.Millisecond, DrainTimeout: 5 * time.Second},
			env.Store, q, breaker.New(env.Redis, env.Prefix, o.breakerThreshold, o.breakerOpen, 4*o.breakerOpen),
			&payload.Fetcher{Store: env.S3, Cache: payload.NewCache(64 << 20)}, sender)
		run(w.Run)
	}
	if !o.noWorker {
		startWorker()
	}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		in.Wait()
		srv.Close()
	})

	h := &harness{t: t, env: env, q: q, api: srv, ctx: ctx, start: startWorker}
	h.admin = sdk.New(srv.URL, adminKey)
	_, key, err := h.admin.CreateTenant(context.Background(), "acme", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.client = sdk.New(srv.URL, key)
	return h
}

// receiver is a programmable webhook endpoint that verifies signatures.
type receiver struct {
	t       *testing.T
	srv     *httptest.Server
	mu      sync.Mutex
	secret  string
	handler func(n int, r *http.Request) (int, http.Header)
	reqs    []recorded
}

type recorded struct {
	At       time.Time
	Path     string
	Method   string
	MsgID    string
	Encoding string
	Body     []byte
	VerifyOK bool
}

func newReceiver(t *testing.T, handler func(n int, r *http.Request) (int, http.Header)) *receiver {
	rc := &receiver{t: t, handler: handler}
	rc.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc.mu.Lock()
		secret := rc.secret
		n := len(rc.reqs)
		h := rc.handler
		rc.mu.Unlock()

		enc := r.Header.Get("Content-Encoding")
		body, err := webhook.VerifyRequest(secret, r, time.Minute)
		if body == nil {
			body, _ = io.ReadAll(r.Body)
		}
		rc.mu.Lock()
		rc.reqs = append(rc.reqs, recorded{At: time.Now(), Path: r.URL.Path, Method: r.Method,
			MsgID: r.Header.Get(webhook.HeaderID), Encoding: enc, Body: body, VerifyOK: err == nil})
		rc.mu.Unlock()

		code, hdr := 200, http.Header(nil)
		if h != nil {
			code, hdr = h(n, r)
		}
		for k, v := range hdr {
			w.Header()[k] = v
		}
		if code >= 300 && code < 400 {
			http.Redirect(w, r, hdr.Get("Location"), code)
			return
		}
		w.WriteHeader(code)
	}))
	t.Cleanup(rc.srv.Close)
	return rc
}

func (rc *receiver) setHandler(h func(n int, r *http.Request) (int, http.Header)) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.handler = h
}

func (rc *receiver) requests() []recorded {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]recorded(nil), rc.reqs...)
}

func (rc *receiver) count() int { return len(rc.requests()) }

func ptr[T any](v T) *T { return &v }

// endpoint registers rc for patterns with an optional retry policy.
func (h *harness) endpoint(rc *receiver, path string, patterns []string, p *sdk.RetryPolicy, mutate ...func(*sdk.EndpointInput)) *sdk.Endpoint {
	h.t.Helper()
	in := sdk.EndpointInput{URL: ptr(rc.srv.URL + path), EventTypes: patterns, RetryPolicy: p}
	for _, m := range mutate {
		m(&in)
	}
	ep, secret, err := h.client.CreateEndpoint(context.Background(), in)
	if err != nil {
		h.t.Fatal(err)
	}
	rc.mu.Lock()
	rc.secret = secret
	rc.mu.Unlock()
	return ep
}

func (h *harness) send(eventType string, data any) *sdk.SendResult {
	h.t.Helper()
	res, err := h.client.SendEvent(context.Background(), eventType, data, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	return res
}

// waitStatus polls until the delivery reaches status.
func (h *harness) waitStatus(id, status string, timeout time.Duration) *sdk.Delivery {
	h.t.Helper()
	var d *sdk.Delivery
	testenv.Eventually(h.t, timeout, "delivery "+id+" to reach "+status, func() bool {
		var err error
		d, err = h.client.GetDelivery(context.Background(), id)
		return err == nil && d.Status == status
	})
	return d
}

// fastPolicy retries quickly: base 1s (the minimum), no growth.
func fastPolicy(attempts int) *sdk.RetryPolicy {
	return &sdk.RetryPolicy{MaxAttempts: ptr(attempts), BaseDelay: "1s", Multiplier: ptr(1.0), MaxDelay: "1s", Timeout: "2s"}
}

type envelope struct {
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}
