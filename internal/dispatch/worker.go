package dispatch

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"webhookd/internal/breaker"
	"webhookd/internal/metrics"
	"webhookd/internal/model"
	"webhookd/internal/payload"
	"webhookd/internal/queue"
	"webhookd/internal/retry"
	"webhookd/internal/store"
	"webhookd/internal/telemetry"
	"webhookd/pkg/webhook"
)

// Store is the subset of *store.Store the worker uses.
type Store interface {
	GetDispatchTarget(ctx context.Context, endpointID string) (*store.DispatchTarget, error)
	Claim(ctx context.Context, deliveryID, eventID string, attempt int, lease time.Duration) (*store.Claimed, error)
	Release(ctx context.Context, deliveryID string, attempt int) error
	Defer(ctx context.Context, deliveryID string, attempt int, at time.Time) error
	Complete(ctx context.Context, o store.Outcome) error
}

type WorkerConfig struct {
	Consumer            string // unique per pod
	Concurrency         int
	EndpointConcurrency int // default per-endpoint cap when the endpoint sets 0
	ClaimIdle           time.Duration
	MaxPayloadBytes     int
	DefaultPolicy       retry.Policy
	TargetCacheTTL      time.Duration
	DrainTimeout        time.Duration
}

type Worker struct {
	cfg     WorkerConfig
	store   Store
	q       *queue.Queue
	br      *breaker.Breaker
	fetch   *payload.Fetcher
	send    *Sender
	log     *slog.Logger
	sem     chan struct{}
	batch   int // max messages per read, per shard
	targets *targetCache
	epMu    sync.Mutex
	epBusy  map[string]int
	wg      sync.WaitGroup
}

func NewWorker(cfg WorkerConfig, st Store, q *queue.Queue, br *breaker.Breaker, f *payload.Fetcher, s *Sender) *Worker {
	if cfg.TargetCacheTTL == 0 {
		cfg.TargetCacheTTL = 5 * time.Second
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = 30 * time.Second
	}
	// Each shard's read and reclaim loops hold up to `batch` slots while
	// blocked in Redis. Keep their sum at or below half the pool so every
	// shard can always read and the rest is left for in-flight deliveries.
	batch := max(1, min(64, cfg.Concurrency/(4*q.Shards())))
	return &Worker{
		cfg: cfg, store: st, q: q, br: br, fetch: f, send: s, batch: batch,
		log:     slog.Default().With("component", "worker", "consumer", cfg.Consumer),
		sem:     make(chan struct{}, cfg.Concurrency),
		targets: &targetCache{ttl: cfg.TargetCacheTTL, m: map[string]targetEntry{}},
		epBusy:  map[string]int{},
	}
}

// Run consumes all shards until ctx is cancelled, then drains in-flight
// deliveries for up to DrainTimeout.
func (w *Worker) Run(ctx context.Context) error {
	// Deliveries use a context that survives ctx so an in-flight attempt can
	// finish and record its outcome during graceful shutdown.
	work, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()

	var readers sync.WaitGroup
	for s := 0; s < w.q.Shards(); s++ {
		readers.Add(2)
		go func() { defer readers.Done(); w.readLoop(ctx, work, s) }()
		go func() { defer readers.Done(); w.reclaimLoop(ctx, work, s) }()
	}
	readers.Wait()

	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(w.cfg.DrainTimeout):
		w.log.Warn("drain timeout; abandoning in-flight deliveries (they will be reclaimed)")
		cancelWork()
		<-done
	}
	return nil
}

// acquire blocks for one slot then grabs up to max-1 more without blocking.
func (w *Worker) acquire(ctx context.Context, max int) int {
	select {
	case w.sem <- struct{}{}:
	case <-ctx.Done():
		return 0
	}
	n := 1
	for n < max {
		select {
		case w.sem <- struct{}{}:
			n++
		default:
			return n
		}
	}
	return n
}

func (w *Worker) release(n int) {
	for range n {
		<-w.sem
	}
}

func (w *Worker) readLoop(ctx, work context.Context, shard int) {
	for ctx.Err() == nil {
		slots := w.acquire(ctx, w.batch)
		if slots == 0 {
			return
		}
		msgs, err := w.q.Read(ctx, shard, w.cfg.Consumer, slots, 2*time.Second)
		if err != nil {
			w.release(slots)
			if ctx.Err() == nil {
				w.log.Error("read failed", "shard", shard, "err", err)
				sleep(ctx, time.Second)
			}
			continue
		}
		w.release(slots - len(msgs))
		w.dispatch(work, msgs)
	}
}

func (w *Worker) reclaimLoop(ctx, work context.Context, shard int) {
	t := time.NewTicker(w.cfg.ClaimIdle / 4)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cursor := "0-0"
		for ctx.Err() == nil {
			slots := w.acquire(ctx, w.batch)
			if slots == 0 {
				return
			}
			msgs, next, err := w.q.Reclaim(ctx, shard, w.cfg.Consumer, w.cfg.ClaimIdle, cursor, slots)
			if err != nil {
				w.release(slots)
				if ctx.Err() == nil {
					w.log.Error("reclaim failed", "shard", shard, "err", err)
				}
				break
			}
			w.release(slots - len(msgs))
			if len(msgs) > 0 {
				w.log.Info("reclaimed idle messages", "shard", shard, "count", len(msgs))
			}
			w.dispatch(work, msgs)
			if cursor = next; cursor == "0-0" {
				break
			}
		}
	}
}

// dispatch processes msgs; each already holds one semaphore slot.
func (w *Worker) dispatch(ctx context.Context, msgs []queue.Msg) {
	for _, m := range msgs {
		w.wg.Add(1)
		metrics.InFlight.Inc()
		go func() {
			defer func() { metrics.InFlight.Dec(); w.release(1); w.wg.Done() }()
			w.process(ctx, m)
		}()
	}
}

// process handles one message. Order is PG → Redis → XACK; a crash at any
// point leaves the message pending, so it is redelivered (at-least-once).
func (w *Worker) process(ctx context.Context, m queue.Msg) {
	log := w.log.With("delivery_id", m.DeliveryID, "endpoint_id", m.EndpointID, "event_id", m.EventID, "attempt", m.Attempt)
	ack := func() {
		if err := w.q.Ack(ctx, m); err != nil {
			log.Error("ack failed", "err", err)
		}
	}
	if m.DeliveryID == "" || m.EventID == "" || m.EndpointID == "" {
		log.Error("malformed queue message; dropping", "stream_id", m.StreamID)
		ack()
		return
	}

	tgt, policy, err := w.target(ctx, m.EndpointID)
	if errors.Is(err, store.ErrNotFound) {
		w.finishWithoutAttempt(ctx, log, m, "", model.DeadEndpointDisabled, "endpoint not found")
		ack()
		return
	}
	if err != nil {
		log.Error("load endpoint failed; will be reclaimed", "err", err)
		return
	}
	ep := tgt.Endpoint
	if ep.Status != model.EndpointActive {
		w.finishWithoutAttempt(ctx, log, m, ep.TenantID, model.DeadEndpointDisabled, "endpoint "+string(ep.Status))
		ack()
		return
	}

	// Per-endpoint concurrency cap (per pod): defer briefly, no attempt used.
	limit := ep.MaxConcurrency
	if limit <= 0 {
		limit = w.cfg.EndpointConcurrency
	}
	if !w.epAcquire(ep.ID, limit) {
		metrics.Deferred.WithLabelValues("concurrency").Inc()
		w.reschedule(ctx, log, m, time.Now().Add(jitter(200*time.Millisecond, time.Second)))
		ack()
		return
	}
	defer w.epRelease(ep.ID)

	// Circuit breaker: defer until it half-opens, no attempt used.
	dec, err := w.br.Allow(ctx, ep.ID, policy.Timeout+5*time.Second)
	if err != nil {
		log.Warn("breaker unavailable; failing open", "err", err)
	}
	if !dec.Allowed {
		metrics.Deferred.WithLabelValues("breaker").Inc()
		at := dec.Until.Add(jitter(0, 5*time.Second))
		if err := w.store.Defer(ctx, m.DeliveryID, m.Attempt, at); err != nil {
			log.Error("defer failed", "err", err)
			return
		}
		w.reschedule(ctx, log, m, at)
		ack()
		return
	}

	claim, err := w.store.Claim(ctx, m.DeliveryID, m.EventID, m.Attempt, policy.Timeout+30*time.Second)
	if errors.Is(err, store.ErrStale) {
		ack() // duplicate or superseded message
		return
	}
	if err != nil {
		log.Error("claim failed; will be reclaimed", "err", err)
		return
	}

	ctx, span := telemetry.Tracer().Start(ctx, "webhook.deliver",
		trace.WithSpanKind(trace.SpanKindClient), trace.WithLinks(telemetry.LinkFrom(claim.TraceParent)...),
		trace.WithAttributes(attribute.String("webhook.delivery_id", m.DeliveryID),
			attribute.String("webhook.endpoint_id", ep.ID), attribute.String("webhook.tenant_id", ep.TenantID),
			attribute.String("webhook.event_type", claim.EventType), attribute.Int("webhook.attempt", m.Attempt+1)))
	defer span.End()

	stored, err := w.loadPayload(ctx, m, claim)
	if errors.Is(err, payload.ErrNotFound) {
		w.complete(ctx, log, m, ep.TenantID, store.Outcome{Status: model.StatusDead, DeadReason: model.DeadPayloadExpired,
			Error: "payload object not found"}, nil)
		ack()
		return
	}
	var body []byte
	if err == nil {
		body, err = payload.Decompress(stored, w.cfg.MaxPayloadBytes+64<<10)
	}
	if err != nil {
		// Transient storage problem: give the lease back and retry soon
		// without consuming an attempt.
		metrics.ObjectStoreErrors.WithLabelValues("get").Inc()
		log.Error("payload unavailable", "err", err)
		span.SetStatus(codes.Error, "payload unavailable")
		if err := w.store.Release(ctx, m.DeliveryID, m.Attempt); err != nil {
			log.Error("release failed", "err", err)
			return
		}
		w.reschedule(ctx, log, m, time.Now().Add(jitter(2*time.Second, 10*time.Second)))
		ack()
		return
	}

	req := Request{URL: ep.URL, MsgID: m.DeliveryID, Body: body, WireBody: body,
		Timeout: policy.Timeout, MaxRedirects: policy.MaxRedirects}
	if ep.Compression == model.CompressionZstd {
		req.WireBody, req.ContentEncoding = stored, "zstd"
	}
	for _, sec := range ep.ActiveSecrets(time.Now()) {
		s, err := webhook.NewHMACSigner(sec)
		if err != nil {
			log.Error("invalid endpoint secret", "err", err)
			return
		}
		req.Signers = append(req.Signers, s)
	}

	res := w.send.Send(ctx, req)
	attempts := m.Attempt + 1
	metrics.DeliveryDuration.Observe(res.Duration.Seconds())

	// Breaker bookkeeping: transient failures count; any HTTP response that
	// isn't transient means the endpoint is alive.
	switch {
	case res.Verdict == Retry:
		if until, err := w.br.Failure(ctx, ep.ID); err == nil && !until.IsZero() {
			metrics.CircuitTransitions.WithLabelValues("open").Inc()
			log.Warn("circuit opened", "until", until)
		}
	case res.StatusCode > 0:
		if closed, err := w.br.Success(ctx, ep.ID); err == nil && closed {
			metrics.CircuitTransitions.WithLabelValues("closed").Inc()
			log.Info("circuit closed")
		}
	}

	o := store.Outcome{Attempted: true, Error: res.Err}
	if res.StatusCode > 0 {
		o.StatusCode = &res.StatusCode
	}
	switch {
	case res.Verdict == Success:
		o.Status = model.StatusSucceeded
		metrics.DeliveryE2ELatency.Observe(time.Since(claim.EventTime).Seconds())
	case res.Verdict == Retry && !policy.Exhausted(attempts):
		o.Status = model.StatusRetrying
		o.NextAttemptAt = time.Now().Add(policy.Delay(attempts, res.RetryAfter))
	case res.Verdict == Retry:
		o.Status, o.DeadReason = model.StatusDead, model.DeadExhausted
	default:
		o.Status, o.DeadReason = model.StatusDead, res.Reason
	}
	result := res.Verdict.String()
	if o.Status == model.StatusDead {
		result = "dead"
	}
	metrics.DeliveryAttempts.WithLabelValues(result, metrics.StatusClass(res.StatusCode)).Inc()
	span.SetAttributes(attribute.Int("http.response.status_code", res.StatusCode), attribute.String("webhook.result", result))
	if o.Status != model.StatusSucceeded {
		span.SetStatus(codes.Error, res.Err)
	}

	rec := &model.Attempt{DeliveryID: m.DeliveryID, Attempt: attempts, StartedAt: time.Now().Add(-res.Duration),
		DurationMS: int(res.Duration.Milliseconds()), StatusCode: o.StatusCode, Error: res.Err,
		ResponseBody: res.RespBody, Redirects: res.Redirects}
	if !w.complete(ctx, log, m, ep.TenantID, o, rec) {
		return // not acked: lease expiry + reclaim will redeliver
	}
	if o.Status == model.StatusRetrying {
		next := m
		next.Attempt, next.Payload = attempts, nil
		w.reschedule(ctx, log, next, o.NextAttemptAt)
	}
	log.Info("delivery attempt", "result", result, "status_code", res.StatusCode, "error", res.Err,
		"duration_ms", res.Duration.Milliseconds(), "redirects", res.Redirects)
	ack()
}

// complete persists the outcome and publishes DLQ notices. Returns false if
// the message must not be acked.
func (w *Worker) complete(ctx context.Context, log *slog.Logger, m queue.Msg, tenantID string, o store.Outcome, rec *model.Attempt) bool {
	o.DeliveryID, o.PrevAttempt, o.Record = m.DeliveryID, m.Attempt, rec
	err := w.store.Complete(ctx, o)
	if errors.Is(err, store.ErrStale) {
		log.Warn("delivery changed concurrently; outcome dropped")
		return true
	}
	if err != nil {
		log.Error("complete failed", "err", err)
		return false
	}
	if o.Status == model.StatusDead {
		metrics.DeliveriesDead.WithLabelValues(string(o.DeadReason)).Inc()
		if tenantID != "" {
			if err := w.q.PublishDead(ctx, queue.DeadNotice{TenantID: tenantID, EndpointID: m.EndpointID,
				DeliveryID: m.DeliveryID, Reason: string(o.DeadReason)}); err != nil {
				log.Error("publish dead notice failed", "err", err)
			}
		}
		log.Warn("delivery dead", "reason", o.DeadReason, "error", o.Error)
	}
	return true
}

// finishWithoutAttempt dead-letters a delivery whose endpoint is gone. It
// claims first so the attempt guard rejects stale duplicates.
func (w *Worker) finishWithoutAttempt(ctx context.Context, log *slog.Logger, m queue.Msg, tenantID string, reason model.DeadReason, msg string) {
	c, err := w.store.Claim(ctx, m.DeliveryID, m.EventID, m.Attempt, time.Minute)
	if err != nil {
		return
	}
	if tenantID == "" {
		tenantID = c.TenantID
	}
	w.complete(ctx, log, m, tenantID, store.Outcome{Status: model.StatusDead, DeadReason: reason, Error: msg}, nil)
}

func (w *Worker) loadPayload(ctx context.Context, m queue.Msg, c *store.Claimed) ([]byte, error) {
	if m.Payload != nil {
		metrics.PayloadCache.WithLabelValues("inline").Inc()
		return m.Payload, nil
	}
	start := time.Now()
	b, err := w.fetch.Get(ctx, m.EventID, c.PayloadKey)
	metrics.PayloadFetchDuration.Observe(time.Since(start).Seconds())
	return b, err
}

func (w *Worker) reschedule(ctx context.Context, log *slog.Logger, m queue.Msg, at time.Time) {
	m.Payload = nil
	if err := w.q.Schedule(ctx, m, at); err != nil {
		// The reconciler will re-enqueue from PostgreSQL.
		log.Error("schedule failed; reconciler will recover", "err", err)
	}
}

func (w *Worker) epAcquire(id string, limit int) bool {
	w.epMu.Lock()
	defer w.epMu.Unlock()
	if w.epBusy[id] >= limit {
		return false
	}
	w.epBusy[id]++
	return true
}

func (w *Worker) epRelease(id string) {
	w.epMu.Lock()
	defer w.epMu.Unlock()
	if w.epBusy[id]--; w.epBusy[id] <= 0 {
		delete(w.epBusy, id)
	}
}

func (w *Worker) target(ctx context.Context, id string) (*store.DispatchTarget, retry.Policy, error) {
	if e, ok := w.targets.get(id); ok {
		return e.t, e.p, nil
	}
	t, err := w.store.GetDispatchTarget(ctx, id)
	if err != nil {
		return nil, retry.Policy{}, err
	}
	p, err := retry.Resolve(w.cfg.DefaultPolicy, t.TenantPolicy, t.Endpoint.RetryPolicy)
	if err != nil {
		w.log.Warn("invalid stored retry policy; using default", "endpoint_id", id, "err", err)
		p = w.cfg.DefaultPolicy
	}
	w.targets.put(id, targetEntry{t: t, p: p})
	return t, p, nil
}

type targetEntry struct {
	t   *store.DispatchTarget
	p   retry.Policy
	exp time.Time
}

type targetCache struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]targetEntry
}

func (c *targetCache) get(id string) (targetEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[id]
	if !ok || time.Now().After(e.exp) {
		return targetEntry{}, false
	}
	return e, true
}

func (c *targetCache) put(id string, e targetEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) > 100_000 { // crude bound; entries are short-lived anyway
		clear(c.m)
	}
	e.exp = time.Now().Add(c.ttl)
	c.m[id] = e
}

func jitter(lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rand.Int64N(int64(hi-lo)))
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
