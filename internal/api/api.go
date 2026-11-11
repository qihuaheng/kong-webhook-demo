// Package api implements the HTTP API (design §12).
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"webhookd/internal/ingest"
	"webhookd/internal/model"
	"webhookd/internal/payload"
	"webhookd/internal/queue"
	"webhookd/internal/retry"
	"webhookd/internal/ssrf"
	"webhookd/internal/store"
)

type Config struct {
	AdminKey        string
	AllowHTTP       bool
	MaxPayloadBytes int
	SecretGrace     time.Duration
	DefaultPolicy   retry.Policy
}

type Server struct {
	cfg   Config
	st    *store.Store
	q     *queue.Queue
	in    *ingest.Ingester
	obj   payload.ObjectStore
	guard *ssrf.Guard
	ready func(context.Context) error
	auth  *authCache
	log   *slog.Logger
}

func New(cfg Config, st *store.Store, q *queue.Queue, in *ingest.Ingester, obj payload.ObjectStore,
	guard *ssrf.Guard, ready func(context.Context) error) *Server {
	return &Server{cfg: cfg, st: st, q: q, in: in, obj: obj, guard: guard, ready: ready,
		auth: &authCache{ttl: 30 * time.Second, m: map[string]authEntry{}},
		log:  slog.Default().With("component", "api")}
}

func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, s.accessLog, middleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.ready(ctx); err != nil {
			http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	})
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/admin/v1", func(r chi.Router) {
		r.Use(s.adminAuth)
		r.Post("/tenants", s.createTenant)
	})

	r.Route("/v1", func(r chi.Router) {
		r.Use(s.tenantAuth)
		r.Post("/events", s.publish)
		r.Get("/events/{id}", s.getEvent)

		r.Post("/endpoints", s.createEndpoint)
		r.Get("/endpoints", s.listEndpoints)
		r.Get("/endpoints/{id}", s.getEndpoint)
		r.Patch("/endpoints/{id}", s.updateEndpoint)
		r.Delete("/endpoints/{id}", s.deleteEndpoint)
		r.Post("/endpoints/{id}/secret/rotate", s.rotateSecret)

		r.Get("/deliveries", s.listDeliveries)
		r.Get("/deliveries/{id}", s.getDelivery)
		r.Get("/deliveries/{id}/attempts", s.listAttempts)

		r.Get("/dlq", s.listDLQ)
		r.Post("/dlq/replay", s.replayBulk)
		r.Post("/dlq/{id}/replay", s.replayOne)
		r.Delete("/dlq/{id}", s.deleteDead)

		r.Put("/tenant/notify", s.setNotify)
	})
	return r
}

// --- auth ---

type ctxKey struct{}

func tenantFrom(r *http.Request) *model.Tenant { return r.Context().Value(ctxKey{}).(*model.Tenant) }

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if t, ok := strings.CutPrefix(h, "Bearer "); ok {
		return strings.TrimSpace(t)
	}
	return ""
}

func (s *Server) adminAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k := bearer(r)
		if s.cfg.AdminKey == "" || subtle.ConstantTimeCompare([]byte(k), []byte(s.cfg.AdminKey)) != 1 {
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) tenantAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k := bearer(r)
		if k == "" {
			writeErr(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		t, ok := s.auth.get(k)
		if !ok {
			var err error
			t, err = s.st.TenantByAPIKey(r.Context(), k)
			if errors.Is(err, store.ErrNotFound) {
				writeErr(w, http.StatusUnauthorized, "invalid api key")
				return
			}
			if err != nil {
				s.internal(w, r, err)
				return
			}
			s.auth.put(k, t)
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, t)))
	})
}

// authCache avoids a PG lookup per request at 10k req/s. Keyed by the API
// key hash so raw keys are not retained in memory longer than needed.
type authEntry struct {
	t   *model.Tenant
	exp time.Time
}

type authCache struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]authEntry
}

func (c *authCache) get(k string) (*model.Tenant, bool) {
	h := store.HashAPIKey(k)
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[h]
	if !ok || time.Now().After(e.exp) {
		return nil, false
	}
	return e.t, true
}

func (c *authCache) put(k string, t *model.Tenant) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) > 10_000 {
		clear(c.m)
	}
	c.m[store.HashAPIKey(k)] = authEntry{t: t, exp: time.Now().Add(c.ttl)}
}

// --- handlers: admin ---

func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string             `json:"name"`
		RetryPolicy *model.RetryPolicy `json:"retry_policy"`
	}
	if !decode(w, r, &req, 64<<10) {
		return
	}
	if req.Name == "" || len(req.Name) > 200 {
		writeErr(w, http.StatusBadRequest, "name is required (max 200 chars)")
		return
	}
	if _, err := retry.Resolve(s.cfg.DefaultPolicy, req.RetryPolicy); err != nil {
		writeErr(w, http.StatusBadRequest, "retry_policy: "+err.Error())
		return
	}
	t, key, err := s.st.CreateTenant(r.Context(), req.Name, req.RetryPolicy)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"tenant": t, "api_key": key})
}

// --- handlers: events ---

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	var req struct {
		EventType      string          `json:"event_type"`
		Data           json.RawMessage `json:"data"`
		IdempotencyKey string          `json:"idempotency_key"`
	}
	if !decode(w, r, &req, int64(s.cfg.MaxPayloadBytes)+16<<10) {
		return
	}
	if k := r.Header.Get("Idempotency-Key"); k != "" {
		req.IdempotencyKey = k
	}
	if len(req.IdempotencyKey) > 256 {
		writeErr(w, http.StatusBadRequest, "idempotency key too long (max 256)")
		return
	}
	if len(req.Data) == 0 {
		writeErr(w, http.StatusBadRequest, "data is required")
		return
	}
	res, err := s.in.Publish(r.Context(), ingest.Event{TenantID: tenantFrom(r).ID, EventType: req.EventType,
		Data: req.Data, IdempotencyKey: req.IdempotencyKey})
	switch {
	case errors.Is(err, ingest.ErrInvalidEventType), errors.Is(err, ingest.ErrInvalidPayload):
		writeErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ingest.ErrPayloadTooLarge):
		writeErr(w, http.StatusRequestEntityTooLarge, err.Error())
	case errors.Is(err, ingest.ErrUnavailable), errors.Is(err, context.DeadlineExceeded):
		w.Header().Set("Retry-After", "1")
		writeErr(w, http.StatusServiceUnavailable, ingest.ErrUnavailable.Error())
	case err != nil:
		s.internal(w, r, err)
	default:
		writeJSON(w, http.StatusAccepted, res)
	}
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	ev, ds, err := s.st.GetEvent(r.Context(), tenantFrom(r).ID, chi.URLParam(r, "id"))
	if s.storeErr(w, r, err) {
		return
	}
	if ds == nil {
		ds = []*model.Delivery{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"event": ev, "deliveries": ds})
}

// --- handlers: endpoints ---

type endpointReq struct {
	URL            *string            `json:"url"`
	Description    *string            `json:"description"`
	EventTypes     []string           `json:"event_types"`
	Status         *string            `json:"status"`
	Compression    *string            `json:"compression"`
	RetryPolicy    *model.RetryPolicy `json:"retry_policy"`
	ClearPolicy    bool               `json:"clear_retry_policy"`
	MaxConcurrency *int               `json:"max_concurrency"`
}

func (s *Server) validateEndpoint(t *model.Tenant, req *endpointReq, create bool) string {
	if create && (req.URL == nil || req.EventTypes == nil) {
		return "url and event_types are required"
	}
	if req.URL != nil {
		if len(*req.URL) > 2048 {
			return "url too long"
		}
		if err := s.guard.ValidateURL(*req.URL, s.cfg.AllowHTTP); err != nil {
			return "url: " + err.Error()
		}
	}
	if req.EventTypes != nil {
		if len(req.EventTypes) == 0 || len(req.EventTypes) > 100 {
			return "event_types must contain 1..100 patterns"
		}
		for _, p := range req.EventTypes {
			if !ingest.ValidPattern(p) {
				return "invalid event type pattern: " + p
			}
		}
	}
	if req.Description != nil && len(*req.Description) > 1000 {
		return "description too long"
	}
	if req.Status != nil && *req.Status != string(model.EndpointActive) && *req.Status != string(model.EndpointDisabled) {
		return "status must be active or disabled"
	}
	if req.Compression != nil && *req.Compression != string(model.CompressionNone) && *req.Compression != string(model.CompressionZstd) {
		return "compression must be none or zstd"
	}
	if req.MaxConcurrency != nil && (*req.MaxConcurrency < 0 || *req.MaxConcurrency > 1000) {
		return "max_concurrency must be in [0,1000]"
	}
	if req.RetryPolicy != nil {
		if _, err := retry.Resolve(s.cfg.DefaultPolicy, t.RetryPolicy, req.RetryPolicy); err != nil {
			return "retry_policy: " + err.Error()
		}
	}
	return ""
}

func (s *Server) createEndpoint(w http.ResponseWriter, r *http.Request) {
	t := tenantFrom(r)
	var req endpointReq
	if !decode(w, r, &req, 64<<10) {
		return
	}
	if msg := s.validateEndpoint(t, &req, true); msg != "" {
		writeErr(w, http.StatusBadRequest, msg)
		return
	}
	ep := &model.Endpoint{TenantID: t.ID, URL: *req.URL, EventTypes: req.EventTypes, RetryPolicy: req.RetryPolicy}
	if req.Description != nil {
		ep.Description = *req.Description
	}
	if req.Compression != nil {
		ep.Compression = model.Compression(*req.Compression)
	}
	if req.MaxConcurrency != nil {
		ep.MaxConcurrency = *req.MaxConcurrency
	}
	secret, err := s.st.CreateEndpoint(r.Context(), ep)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	if req.Status != nil && *req.Status == string(model.EndpointDisabled) {
		st := model.EndpointDisabled
		if ep, err = s.st.UpdateEndpoint(r.Context(), t.ID, ep.ID, store.EndpointPatch{Status: &st}); err != nil {
			s.internal(w, r, err)
			return
		}
	}
	s.in.InvalidateSubscriptions(t.ID)
	writeJSON(w, http.StatusCreated, map[string]any{"endpoint": ep, "secret": secret})
}

func (s *Server) listEndpoints(w http.ResponseWriter, r *http.Request) {
	eps, err := s.st.ListEndpoints(r.Context(), tenantFrom(r).ID, false)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	if eps == nil {
		eps = []*model.Endpoint{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"endpoints": eps})
}

func (s *Server) getEndpoint(w http.ResponseWriter, r *http.Request) {
	ep, err := s.st.GetEndpoint(r.Context(), tenantFrom(r).ID, chi.URLParam(r, "id"))
	if s.storeErr(w, r, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"endpoint": ep})
}

func (s *Server) updateEndpoint(w http.ResponseWriter, r *http.Request) {
	t := tenantFrom(r)
	var req endpointReq
	if !decode(w, r, &req, 64<<10) {
		return
	}
	if msg := s.validateEndpoint(t, &req, false); msg != "" {
		writeErr(w, http.StatusBadRequest, msg)
		return
	}
	p := store.EndpointPatch{URL: req.URL, Description: req.Description, EventTypes: req.EventTypes,
		RetryPolicy: req.RetryPolicy, ClearPolicy: req.ClearPolicy, MaxConcurrency: req.MaxConcurrency}
	if req.Status != nil {
		st := model.EndpointStatus(*req.Status)
		p.Status = &st
	}
	if req.Compression != nil {
		c := model.Compression(*req.Compression)
		p.Compression = &c
	}
	ep, err := s.st.UpdateEndpoint(r.Context(), t.ID, chi.URLParam(r, "id"), p)
	if s.storeErr(w, r, err) {
		return
	}
	s.in.InvalidateSubscriptions(t.ID)
	writeJSON(w, http.StatusOK, map[string]any{"endpoint": ep})
}

func (s *Server) deleteEndpoint(w http.ResponseWriter, r *http.Request) {
	t := tenantFrom(r)
	if s.storeErr(w, r, s.st.DeleteEndpoint(r.Context(), t.ID, chi.URLParam(r, "id"))) {
		return
	}
	s.in.InvalidateSubscriptions(t.ID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) rotateSecret(w http.ResponseWriter, r *http.Request) {
	secret, err := s.st.RotateSecret(r.Context(), tenantFrom(r).ID, chi.URLParam(r, "id"), s.cfg.SecretGrace)
	if s.storeErr(w, r, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"secret": secret, "previous_valid_until": time.Now().Add(s.cfg.SecretGrace).UTC()})
}

// --- handlers: deliveries & DLQ ---

func (s *Server) filter(w http.ResponseWriter, r *http.Request) (store.DeliveryFilter, bool) {
	qv := r.URL.Query()
	f := store.DeliveryFilter{TenantID: tenantFrom(r).ID, EndpointID: qv.Get("endpoint_id"), Before: qv.Get("before")}
	if st := qv.Get("status"); st != "" {
		switch model.DeliveryStatus(st) {
		case model.StatusPending, model.StatusRetrying, model.StatusSucceeded, model.StatusDead:
			f.Status = model.DeliveryStatus(st)
		default:
			writeErr(w, http.StatusBadRequest, "invalid status")
			return f, false
		}
	}
	for name, dst := range map[string]*time.Time{"since": &f.Since, "until": &f.Until} {
		if v := qv.Get(name); v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				writeErr(w, http.StatusBadRequest, name+" must be RFC3339")
				return f, false
			}
			*dst = t
		}
	}
	if v := qv.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			writeErr(w, http.StatusBadRequest, "limit must be in [1,1000]")
			return f, false
		}
		f.Limit = n
	}
	return f, true
}

func (s *Server) listDeliveries(w http.ResponseWriter, r *http.Request) {
	f, ok := s.filter(w, r)
	if !ok {
		return
	}
	s.writeDeliveries(w, r, f)
}

func (s *Server) listDLQ(w http.ResponseWriter, r *http.Request) {
	f, ok := s.filter(w, r)
	if !ok {
		return
	}
	f.Status = model.StatusDead
	s.writeDeliveries(w, r, f)
}

func (s *Server) writeDeliveries(w http.ResponseWriter, r *http.Request, f store.DeliveryFilter) {
	ds, err := s.st.ListDeliveries(r.Context(), f)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	resp := map[string]any{"deliveries": ds}
	if ds == nil {
		resp["deliveries"] = []*model.Delivery{}
	}
	if f.Limit == 0 {
		f.Limit = 100
	}
	if len(ds) == f.Limit {
		resp["next_before"] = ds[len(ds)-1].ID
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getDelivery(w http.ResponseWriter, r *http.Request) {
	d, err := s.st.GetDelivery(r.Context(), tenantFrom(r).ID, chi.URLParam(r, "id"))
	if s.storeErr(w, r, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"delivery": d})
}

func (s *Server) listAttempts(w http.ResponseWriter, r *http.Request) {
	as, err := s.st.ListAttempts(r.Context(), tenantFrom(r).ID, chi.URLParam(r, "id"))
	if s.storeErr(w, r, err) {
		return
	}
	if as == nil {
		as = []*model.Attempt{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"attempts": as})
}

func (s *Server) replayOne(w http.ResponseWriter, r *http.Request) {
	t := tenantFrom(r)
	id := chi.URLParam(r, "id")
	d, err := s.st.GetDelivery(r.Context(), t.ID, id)
	if s.storeErr(w, r, err) {
		return
	}
	if d.Status != model.StatusDead {
		writeErr(w, http.StatusConflict, "delivery is not in the DLQ (status "+string(d.Status)+")")
		return
	}
	ev, _, err := s.st.GetEvent(r.Context(), t.ID, d.EventID)
	if s.storeErr(w, r, err) {
		return
	}
	if _, err := s.obj.Get(r.Context(), ev.PayloadKey); errors.Is(err, payload.ErrNotFound) {
		writeErr(w, http.StatusGone, "payload has expired; delivery cannot be replayed")
		return
	} else if err != nil {
		s.internal(w, r, err)
		return
	}
	rep, err := s.st.Replay(r.Context(), store.DeliveryFilter{TenantID: t.ID}, []string{id})
	if err != nil {
		s.internal(w, r, err)
		return
	}
	if len(rep) == 0 {
		writeErr(w, http.StatusConflict, "delivery changed concurrently")
		return
	}
	s.enqueue(r.Context(), rep)
	writeJSON(w, http.StatusAccepted, map[string]any{"replayed": 1, "delivery_ids": []string{id}})
}

// replayBulk replays up to `limit` (max 1000) dead deliveries matching the
// filter in one call; callers loop until replayed == 0. Deliveries whose
// payload has expired end as dead/payload_expired again.
func (s *Server) replayBulk(w http.ResponseWriter, r *http.Request) {
	var req struct {
		EndpointID string     `json:"endpoint_id"`
		Since      *time.Time `json:"since"`
		Until      *time.Time `json:"until"`
		Limit      int        `json:"limit"`
	}
	if !decode(w, r, &req, 16<<10) {
		return
	}
	if req.Limit < 0 || req.Limit > 1000 {
		writeErr(w, http.StatusBadRequest, "limit must be in [1,1000]")
		return
	}
	f := store.DeliveryFilter{TenantID: tenantFrom(r).ID, EndpointID: req.EndpointID, Limit: req.Limit}
	if req.Since != nil {
		f.Since = *req.Since
	}
	if req.Until != nil {
		f.Until = *req.Until
	}
	rep, err := s.st.Replay(r.Context(), f, nil)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	s.enqueue(r.Context(), rep)
	ids := make([]string, len(rep))
	for i, d := range rep {
		ids[i] = d.DeliveryID
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"replayed": len(rep), "delivery_ids": ids})
}

func (s *Server) enqueue(ctx context.Context, rep []store.Stale) {
	msgs := make([]queue.Msg, len(rep))
	for i, d := range rep {
		msgs[i] = queue.Msg{DeliveryID: d.DeliveryID, EndpointID: d.EndpointID, EventID: d.EventID, Attempt: d.Attempt}
	}
	if err := s.q.Enqueue(ctx, msgs...); err != nil {
		s.log.Error("replay enqueue failed; reconciler will recover", "err", err)
	}
}

func (s *Server) deleteDead(w http.ResponseWriter, r *http.Request) {
	if s.storeErr(w, r, s.st.DeleteDead(r.Context(), tenantFrom(r).ID, chi.URLParam(r, "id"))) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) setNotify(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	if !decode(w, r, &req, 16<<10) {
		return
	}
	if req.URL != "" {
		if err := s.guard.ValidateURL(req.URL, s.cfg.AllowHTTP); err != nil {
			writeErr(w, http.StatusBadRequest, "url: "+err.Error())
			return
		}
	}
	secret, err := s.st.SetNotify(r.Context(), tenantFrom(r).ID, req.URL)
	if s.storeErr(w, r, err) {
		return
	}
	if req.URL == "" {
		writeJSON(w, http.StatusOK, map[string]any{"url": nil})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"url": req.URL, "secret": secret})
}

// --- helpers ---

func decode(w http.ResponseWriter, r *http.Request, dst any, limit int64) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErr(w, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			writeErr(w, http.StatusBadRequest, "could not read body")
		}
		return false
	}
	if err := json.Unmarshal(body, dst); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (s *Server) internal(w http.ResponseWriter, r *http.Request, err error) {
	s.log.Error("internal error", "path", r.URL.Path, "request_id", middleware.GetReqID(r.Context()), "err", err)
	writeErr(w, http.StatusInternalServerError, "internal error")
}

// storeErr writes 404/500 for err and reports whether it wrote a response.
func (s *Server) storeErr(w http.ResponseWriter, r *http.Request, err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not found")
	default:
		s.internal(w, r, err)
	}
	return true
}

func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
			return
		}
		lvl := slog.LevelDebug
		if ww.Status() >= 500 {
			lvl = slog.LevelError
		} else if ww.Status() >= 400 {
			lvl = slog.LevelInfo
		}
		s.log.Log(r.Context(), lvl, "http request", "method", r.Method, "path", r.URL.Path, "status", ww.Status(),
			"duration_ms", time.Since(start).Milliseconds(), "request_id", middleware.GetReqID(r.Context()))
	})
}
