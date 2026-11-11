// Package ingest accepts events, fans them out to subscribed endpoints, and
// hands deliveries to the queue (design §3.1).
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"webhookd/internal/metrics"
	"webhookd/internal/model"
	"webhookd/internal/payload"
	"webhookd/internal/queue"
	"webhookd/internal/store"
	"webhookd/internal/telemetry"
)

var (
	ErrInvalidEventType = errors.New("event_type must be dot-separated segments of [A-Za-z0-9_-], max 128 chars")
	ErrPayloadTooLarge  = errors.New("payload exceeds maximum size")
	ErrInvalidPayload   = errors.New("data must be valid JSON")
	ErrUnavailable      = errors.New("ingest temporarily unavailable")
)

var (
	eventTypeRe = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$`)
	patternRe   = regexp.MustCompile(`^(\*|[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*(\.\*)?)$`)
)

// ValidEventType reports whether t is a valid concrete event type.
func ValidEventType(t string) bool { return len(t) <= 128 && eventTypeRe.MatchString(t) }

// ValidPattern reports whether p is a valid subscription pattern:
// "*", an exact type, or a prefix wildcard like "payment.*".
func ValidPattern(p string) bool { return len(p) <= 128 && patternRe.MatchString(p) }

// Matches reports whether eventType matches subscription pattern p.
func Matches(p, eventType string) bool {
	if p == "*" || p == eventType {
		return true
	}
	if prefix, ok := strings.CutSuffix(p, "*"); ok {
		return strings.HasPrefix(eventType, prefix)
	}
	return false
}

// Store is the subset of *store.Store ingest needs.
type Store interface {
	ListEndpoints(ctx context.Context, tenantID string, activeOnly bool) ([]*model.Endpoint, error)
	InsertBatch(ctx context.Context, items []store.IngestItem) ([]store.IngestResult, error)
	GetEvent(ctx context.Context, tenantID, id string) (*model.Event, []*model.Delivery, error)
}

type Config struct {
	MaxPayloadBytes int
	InlineThreshold int
	SubCacheTTL     time.Duration // default 10s
	BatchMax        int           // default 200
	BatchWait       time.Duration // default 5ms
	Committers      int           // parallel group-commit loops, default 4
}

type Ingester struct {
	cfg  Config
	st   Store
	obj  payload.ObjectStore
	q    *queue.Queue
	subs *subCache
	reqs chan *commitReq
	log  *slog.Logger
	wg   sync.WaitGroup
}

func New(cfg Config, st Store, obj payload.ObjectStore, q *queue.Queue) *Ingester {
	if cfg.SubCacheTTL == 0 {
		cfg.SubCacheTTL = 10 * time.Second
	}
	if cfg.BatchMax == 0 {
		cfg.BatchMax = 200
	}
	if cfg.BatchWait == 0 {
		cfg.BatchWait = 5 * time.Millisecond
	}
	if cfg.Committers == 0 {
		cfg.Committers = 4
	}
	return &Ingester{cfg: cfg, st: st, obj: obj, q: q,
		subs: &subCache{ttl: cfg.SubCacheTTL, m: map[string]subEntry{}},
		reqs: make(chan *commitReq, cfg.BatchMax*cfg.Committers*4),
		log:  slog.Default().With("component", "ingest")}
}

// Start launches the group-commit loops; they stop when ctx is cancelled
// and pending requests are flushed.
func (in *Ingester) Start(ctx context.Context) {
	for range in.cfg.Committers {
		in.wg.Add(1)
		go func() { defer in.wg.Done(); in.commitLoop(ctx) }()
	}
}

// Wait blocks until the commit loops exit.
func (in *Ingester) Wait() { in.wg.Wait() }

// Event is a publish request.
type Event struct {
	TenantID       string
	EventType      string
	Data           json.RawMessage
	IdempotencyKey string
}

// Result is returned to the API.
type Result struct {
	EventID     string   `json:"event_id"`
	DeliveryIDs []string `json:"delivery_ids"`
	Duplicate   bool     `json:"duplicate,omitempty"`
}

// envelope is the JSON body delivered to receivers.
type envelope struct {
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

func (in *Ingester) Publish(ctx context.Context, ev Event) (*Result, error) {
	start := time.Now()
	ctx, span := telemetry.Tracer().Start(ctx, "webhook.ingest")
	defer span.End()
	span.SetAttributes(attribute.String("webhook.tenant_id", ev.TenantID), attribute.String("webhook.event_type", ev.EventType))

	if !ValidEventType(ev.EventType) {
		return nil, ErrInvalidEventType
	}
	if len(ev.Data) > in.cfg.MaxPayloadBytes {
		return nil, ErrPayloadTooLarge
	}
	if !json.Valid(ev.Data) {
		return nil, ErrInvalidPayload
	}
	metrics.IngestPayloadBytes.Observe(float64(len(ev.Data)))

	eps, err := in.subscribers(ctx, ev.TenantID, ev.EventType)
	if err != nil {
		in.log.Error("subscription lookup failed", "err", err)
		return nil, ErrUnavailable
	}
	eventID := model.NewID("evt")
	created, _ := model.IDTime(eventID)
	if len(eps) == 0 {
		// Nothing subscribed: accept, store nothing (design §3.1 step 3).
		metrics.EventsIngested.WithLabelValues(ev.TenantID).Inc()
		return &Result{EventID: eventID, DeliveryIDs: []string{}}, nil
	}

	body, _ := json.Marshal(envelope{Type: ev.EventType, Timestamp: created.UTC(), Data: ev.Data})
	stored := payload.Compress(body)
	key := payload.Key(ev.TenantID, eventID, created)
	t0 := time.Now()
	if err := in.obj.Put(ctx, key, stored); err != nil {
		metrics.ObjectStoreErrors.WithLabelValues("put").Inc()
		in.log.Error("payload put failed", "err", err)
		return nil, ErrUnavailable
	}
	metrics.IngestDuration.WithLabelValues("s3_put").Observe(time.Since(t0).Seconds())

	item := store.IngestItem{Event: model.Event{ID: eventID, TenantID: ev.TenantID, EventType: ev.EventType,
		PayloadKey: key, PayloadSize: len(body), StoredSize: len(stored), IdempotencyKey: ev.IdempotencyKey,
		TraceParent: telemetry.TraceParent(ctx), CreatedAt: created}}
	res := &Result{EventID: eventID}
	for _, ep := range eps {
		id := model.NewID("msg")
		dt, _ := model.IDTime(id)
		item.Deliveries = append(item.Deliveries, model.Delivery{ID: id, EventID: eventID, EndpointID: ep.ID,
			TenantID: ev.TenantID, CreatedAt: dt})
		res.DeliveryIDs = append(res.DeliveryIDs, id)
	}

	t0 = time.Now()
	cr, err := in.commit(ctx, item)
	if err != nil {
		in.log.Error("commit failed", "err", err)
		return nil, ErrUnavailable // orphaned object expires via lifecycle
	}
	metrics.IngestDuration.WithLabelValues("pg_commit").Observe(time.Since(t0).Seconds())
	if cr.Duplicate {
		_, ds, err := in.st.GetEvent(ctx, ev.TenantID, cr.EventID)
		if err != nil {
			return nil, ErrUnavailable
		}
		dup := &Result{EventID: cr.EventID, DeliveryIDs: []string{}, Duplicate: true}
		for _, d := range ds {
			dup.DeliveryIDs = append(dup.DeliveryIDs, d.ID)
		}
		return dup, nil
	}

	var inline []byte
	if len(stored) <= in.cfg.InlineThreshold {
		inline = stored
	}
	msgs := make([]queue.Msg, len(item.Deliveries))
	for i, d := range item.Deliveries {
		msgs[i] = queue.Msg{DeliveryID: d.ID, EndpointID: d.EndpointID, EventID: eventID, Payload: inline}
	}
	t0 = time.Now()
	if err := in.q.Enqueue(ctx, msgs...); err != nil {
		// Committed: the reconciler re-enqueues from PostgreSQL.
		in.log.Error("enqueue failed; reconciler will recover", "event_id", eventID, "err", err)
	}
	metrics.IngestDuration.WithLabelValues("enqueue").Observe(time.Since(t0).Seconds())
	metrics.IngestDuration.WithLabelValues("total").Observe(time.Since(start).Seconds())
	metrics.EventsIngested.WithLabelValues(ev.TenantID).Inc()
	return res, nil
}

func (in *Ingester) subscribers(ctx context.Context, tenantID, eventType string) ([]*model.Endpoint, error) {
	eps, ok := in.subs.get(tenantID)
	if !ok {
		var err error
		if eps, err = in.st.ListEndpoints(ctx, tenantID, true); err != nil {
			return nil, err
		}
		in.subs.put(tenantID, eps)
	}
	var out []*model.Endpoint
	for _, ep := range eps {
		for _, p := range ep.EventTypes {
			if Matches(p, eventType) {
				out = append(out, ep)
				break
			}
		}
	}
	return out, nil
}

// InvalidateSubscriptions drops a tenant's cached subscriptions on this
// replica (called after endpoint changes; other replicas expire by TTL).
func (in *Ingester) InvalidateSubscriptions(tenantID string) { in.subs.del(tenantID) }

// --- group commit ---

type commitReq struct {
	item store.IngestItem
	done chan commitResp
}

type commitResp struct {
	res store.IngestResult
	err error
}

func (in *Ingester) commit(ctx context.Context, item store.IngestItem) (store.IngestResult, error) {
	r := &commitReq{item: item, done: make(chan commitResp, 1)}
	select {
	case in.reqs <- r:
	case <-ctx.Done():
		return store.IngestResult{}, ctx.Err()
	}
	select {
	case resp := <-r.done:
		return resp.res, resp.err
	case <-ctx.Done():
		// The batch may still commit; the event is then delivered even though
		// the client saw an error, and its retry dedupes via Idempotency-Key.
		return store.IngestResult{}, ctx.Err()
	}
}

func (in *Ingester) commitLoop(ctx context.Context) {
	batch := make([]*commitReq, 0, in.cfg.BatchMax)
	for {
		var first *commitReq
		select {
		case first = <-in.reqs:
		case <-ctx.Done():
			// Flush whatever is queued before exiting.
			for {
				select {
				case r := <-in.reqs:
					in.flush([]*commitReq{r})
				default:
					return
				}
			}
		}
		batch = append(batch[:0], first)
		timer := time.NewTimer(in.cfg.BatchWait)
	collect:
		for len(batch) < in.cfg.BatchMax {
			select {
			case r := <-in.reqs:
				batch = append(batch, r)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		in.flush(batch)
	}
}

func (in *Ingester) flush(batch []*commitReq) {
	items := make([]store.IngestItem, len(batch))
	for i, r := range batch {
		items[i] = r.item
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := in.st.InsertBatch(ctx, items)
	for i, r := range batch {
		if err != nil {
			r.done <- commitResp{err: fmt.Errorf("insert batch: %w", err)}
		} else {
			r.done <- commitResp{res: res[i]}
		}
	}
}

// --- subscription cache ---

type subEntry struct {
	eps []*model.Endpoint
	exp time.Time
}

type subCache struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]subEntry
}

func (c *subCache) get(t string) ([]*model.Endpoint, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[t]
	if !ok || time.Now().After(e.exp) {
		return nil, false
	}
	return e.eps, true
}

func (c *subCache) put(t string, eps []*model.Endpoint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[t] = subEntry{eps: eps, exp: time.Now().Add(c.ttl)}
}

func (c *subCache) del(t string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, t)
}
