package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"webhookd/internal/model"
)

// Claimed is returned by Claim: the delivery is leased to the caller.
type Claimed struct {
	TenantID    string
	EventType   string
	PayloadKey  string
	PayloadSize int
	TraceParent string
	EventTime   time.Time
}

// Claim leases a delivery for one attempt. It succeeds only if the delivery
// is still pending/retrying at exactly `attempt` attempts and not leased by
// another worker — this deduplicates redelivered or reconciled queue messages.
func (s *Store) Claim(ctx context.Context, deliveryID, eventID string, attempt int, lease time.Duration) (*Claimed, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	dt, err := idTime(deliveryID)
	if err != nil {
		return nil, err
	}
	et, err := idTime(eventID)
	if err != nil {
		return nil, err
	}
	var c Claimed
	var tp *string
	err = s.pool.QueryRow(ctx, `
		UPDATE deliveries d SET lease_until = now() + $4 * interval '1 millisecond', updated_at = now()
		FROM events e
		WHERE d.id = $1 AND d.created_at = $2 AND d.attempt = $3
		  AND d.status IN ('pending', 'retrying')
		  AND (d.lease_until IS NULL OR d.lease_until < now())
		  AND e.id = d.event_id AND e.created_at = $5 AND e.id = $6
		RETURNING d.tenant_id, e.event_type, e.payload_key, e.payload_size, e.trace_parent, e.created_at`,
		deliveryID, dt, attempt, lease.Milliseconds(), et, eventID).
		Scan(&c.TenantID, &c.EventType, &c.PayloadKey, &c.PayloadSize, &tp, &c.EventTime)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrStale
		}
		return nil, err
	}
	if tp != nil {
		c.TraceParent = *tp
	}
	return &c, nil
}

// Release drops a lease without consuming an attempt (e.g. transient payload
// fetch failure).
func (s *Store) Release(ctx context.Context, deliveryID string, attempt int) error {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	dt, err := idTime(deliveryID)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `UPDATE deliveries SET lease_until = NULL
		WHERE id = $1 AND created_at = $2 AND attempt = $3`, deliveryID, dt, attempt)
	return err
}

// Defer moves next_attempt_at without consuming an attempt (breaker open), so
// the reconciler does not treat a long deferral as a lost message.
func (s *Store) Defer(ctx context.Context, deliveryID string, attempt int, at time.Time) error {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	dt, err := idTime(deliveryID)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `UPDATE deliveries SET next_attempt_at = $4, updated_at = now()
		WHERE id = $1 AND created_at = $2 AND attempt = $3 AND status IN ('pending', 'retrying')
		  AND (lease_until IS NULL OR lease_until < now())`, deliveryID, dt, attempt, at)
	return err
}

// Outcome is the result of processing a delivery at a given attempt count.
type Outcome struct {
	DeliveryID    string
	PrevAttempt   int  // attempt count the caller claimed at (guard)
	Attempted     bool // an HTTP attempt was made (increments attempt)
	Status        model.DeliveryStatus
	NextAttemptAt time.Time // for retrying
	StatusCode    *int
	Error         string
	DeadReason    model.DeadReason
	Record        *model.Attempt // attempt history row, if Attempted
}

// Complete records an attempt and transitions the delivery atomically.
// Returns ErrStale if the delivery no longer matches PrevAttempt.
func (s *Store) Complete(ctx context.Context, o Outcome) error {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	dt, err := idTime(o.DeliveryID)
	if err != nil {
		return err
	}
	inc := 0
	if o.Attempted {
		inc = 1
	}
	var next *time.Time
	if o.Status == model.StatusRetrying {
		next = &o.NextAttemptAt
	}
	var reason *string
	if o.DeadReason != "" {
		r := string(o.DeadReason)
		reason = &r
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE deliveries SET
				status = $3, attempt = attempt + $4,
				next_attempt_at = COALESCE($5, next_attempt_at),
				lease_until = NULL, last_status_code = COALESCE($6, last_status_code),
				last_error = $7, dead_reason = $8, updated_at = now(),
				completed_at = CASE WHEN $3 IN ('succeeded', 'dead') THEN now() END
			WHERE id = $1 AND created_at = $2 AND attempt = $9 AND status IN ('pending', 'retrying')`,
			o.DeliveryID, dt, string(o.Status), inc, next, o.StatusCode, nullStr(o.Error), reason, o.PrevAttempt)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrStale
		}
		if r := o.Record; r != nil {
			_, err = tx.Exec(ctx, `
				INSERT INTO delivery_attempts (delivery_id, attempt, started_at, duration_ms, status_code, error, response_body, redirects)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				r.DeliveryID, r.Attempt, r.StartedAt, r.DurationMS, r.StatusCode, nullStr(r.Error), nullStr(r.ResponseBody), r.Redirects)
		}
		return err
	})
}

// Stale is a delivery the reconciler re-enqueues.
type Stale struct {
	DeliveryID, EndpointID, EventID string
	Attempt                         int
}

// Reconcile finds pending/retrying deliveries scheduled before cutoff that
// hold no live lease (presumed lost queue message), bumps their
// next_attempt_at to now so they are not picked again immediately, and
// returns them for re-enqueue. The caller chooses cutoff so that deliveries
// which may still be waiting in a queue backlog are excluded. Runs on one
// replica at a time (advisory lock).
func (s *Store) Reconcile(ctx context.Context, cutoff time.Time, lookback time.Duration, limit int) ([]Stale, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	var out []Stale
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var ok bool
		if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", lockReconcile).Scan(&ok); err != nil || !ok {
			return err
		}
		rows, err := tx.Query(ctx, `
			UPDATE deliveries d SET next_attempt_at = now()
			FROM (
				SELECT id, created_at FROM deliveries
				WHERE status IN ('pending', 'retrying')
				  AND next_attempt_at < $1
				  AND (lease_until IS NULL OR lease_until < now())
				  AND created_at > now() - $2 * interval '1 millisecond'
				ORDER BY next_attempt_at LIMIT $3
				FOR UPDATE SKIP LOCKED
			) s
			WHERE d.id = s.id AND d.created_at = s.created_at
			RETURNING d.id, d.endpoint_id, d.event_id, d.attempt`,
			cutoff, lookback.Milliseconds(), limit)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (Stale, error) {
			var st Stale
			return st, r.Scan(&st.DeliveryID, &st.EndpointID, &st.EventID, &st.Attempt)
		})
		return err
	})
	return out, err
}

const deliveryCols = `id, event_id, endpoint_id, tenant_id, status, attempt, next_attempt_at,
	last_status_code, last_error, dead_reason, created_at, updated_at, completed_at`

func scanDelivery(r pgx.Row) (*model.Delivery, error) {
	var d model.Delivery
	var next time.Time
	var lastErr, reason *string
	if err := r.Scan(&d.ID, &d.EventID, &d.EndpointID, &d.TenantID, &d.Status, &d.Attempt, &next,
		&d.LastStatusCode, &lastErr, &reason, &d.CreatedAt, &d.UpdatedAt, &d.CompletedAt); err != nil {
		return nil, notFound(err)
	}
	if !d.Status.Terminal() {
		d.NextAttemptAt = &next
	}
	if lastErr != nil {
		d.LastError = *lastErr
	}
	if reason != nil {
		d.DeadReason = model.DeadReason(*reason)
	}
	return &d, nil
}

func (s *Store) GetDelivery(ctx context.Context, tenantID, id string) (*model.Delivery, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	dt, err := idTime(id)
	if err != nil {
		return nil, err
	}
	return scanDelivery(s.pool.QueryRow(ctx, `SELECT `+deliveryCols+` FROM deliveries
		WHERE id = $1 AND created_at = $2 AND tenant_id = $3`, id, dt, tenantID))
}

// GetEvent returns an event and its deliveries.
func (s *Store) GetEvent(ctx context.Context, tenantID, id string) (*model.Event, []*model.Delivery, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	et, err := idTime(id)
	if err != nil {
		return nil, nil, err
	}
	var e model.Event
	var key, tp *string
	err = s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, event_type, payload_key, payload_size, stored_size, idempotency_key, trace_parent, created_at
		FROM events WHERE id = $1 AND created_at = $2 AND tenant_id = $3`, id, et, tenantID).
		Scan(&e.ID, &e.TenantID, &e.EventType, &e.PayloadKey, &e.PayloadSize, &e.StoredSize, &key, &tp, &e.CreatedAt)
	if err != nil {
		return nil, nil, notFound(err)
	}
	if key != nil {
		e.IdempotencyKey = *key
	}
	if tp != nil {
		e.TraceParent = *tp
	}
	// Delivery IDs are minted right after the event ID, so a 1h window
	// keeps the scan to at most two partitions.
	rows, err := s.pool.Query(ctx, `SELECT `+deliveryCols+` FROM deliveries
		WHERE event_id = $1 AND created_at >= $2 AND created_at < $3 ORDER BY id`, id, et, et.Add(time.Hour))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var ds []*model.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, nil, err
		}
		ds = append(ds, d)
	}
	return &e, ds, rows.Err()
}

// DeliveryFilter filters ListDeliveries. Results are newest-first; pass the
// last ID seen as Before to page.
type DeliveryFilter struct {
	TenantID   string
	Status     model.DeliveryStatus // empty = any
	EndpointID string
	Since      time.Time
	Until      time.Time
	Before     string
	Limit      int
}

func (s *Store) ListDeliveries(ctx context.Context, f DeliveryFilter) ([]*model.Delivery, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 100
	}
	if f.Until.IsZero() {
		f.Until = time.Now().Add(time.Minute)
	}
	if f.Since.IsZero() {
		f.Since = f.Until.Add(-31 * 24 * time.Hour)
	}
	rows, err := s.pool.Query(ctx, `SELECT `+deliveryCols+` FROM deliveries
		WHERE tenant_id = $1
		  AND ($2 = '' OR status = $2)
		  AND ($3 = '' OR endpoint_id = $3)
		  AND created_at >= $4 AND created_at < $5
		  AND ($6 = '' OR id < $6)
		ORDER BY id DESC LIMIT $7`,
		f.TenantID, string(f.Status), f.EndpointID, f.Since, f.Until, f.Before, f.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) ListAttempts(ctx context.Context, tenantID, deliveryID string) ([]*model.Attempt, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	d, err := s.GetDelivery(ctx, tenantID, deliveryID)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT delivery_id, attempt, started_at, duration_ms, status_code, COALESCE(error, ''), COALESCE(response_body, ''), redirects
		FROM delivery_attempts WHERE delivery_id = $1 AND started_at >= $2 ORDER BY started_at, attempt`,
		deliveryID, d.CreatedAt)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (*model.Attempt, error) {
		var a model.Attempt
		return &a, r.Scan(&a.DeliveryID, &a.Attempt, &a.StartedAt, &a.DurationMS, &a.StatusCode, &a.Error, &a.ResponseBody, &a.Redirects)
	})
}

// Replay moves dead deliveries back to pending with attempt reset to 0.
// webhook-id (the delivery ID) is unchanged so receivers can deduplicate.
// If ids is non-empty only those are replayed; otherwise f selects (up to f.Limit).
func (s *Store) Replay(ctx context.Context, f DeliveryFilter, ids []string) ([]Stale, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 1000
	}
	if f.Until.IsZero() {
		f.Until = time.Now().Add(time.Minute)
	}
	if f.Since.IsZero() {
		f.Since = f.Until.Add(-31 * 24 * time.Hour)
	}
	var times []time.Time
	for _, id := range ids {
		t, err := idTime(id)
		if err != nil {
			return nil, err
		}
		times = append(times, t)
	}
	rows, err := s.pool.Query(ctx, `
		UPDATE deliveries d SET status = 'pending', attempt = 0, next_attempt_at = now(), lease_until = NULL,
			last_error = NULL, dead_reason = NULL, completed_at = NULL, updated_at = now()
		FROM (
			SELECT id, created_at FROM deliveries
			WHERE tenant_id = $1 AND status = 'dead'
			  AND ($2 = '' OR endpoint_id = $2)
			  AND created_at >= $3 AND created_at < $4
			  AND (COALESCE(cardinality($5::text[]), 0) = 0 OR (id, created_at) IN (SELECT * FROM unnest($5::text[], $6::timestamptz[])))
			ORDER BY id LIMIT $7
			FOR UPDATE SKIP LOCKED
		) s
		WHERE d.id = s.id AND d.created_at = s.created_at
		RETURNING d.id, d.endpoint_id, d.event_id, d.attempt`,
		f.TenantID, f.EndpointID, f.Since, f.Until, ids, times, f.Limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Stale, error) {
		var st Stale
		return st, r.Scan(&st.DeliveryID, &st.EndpointID, &st.EventID, &st.Attempt)
	})
}

// DeleteDead permanently removes a dead delivery from the DLQ.
func (s *Store) DeleteDead(ctx context.Context, tenantID, id string) error {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	dt, err := idTime(id)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM deliveries WHERE id = $1 AND created_at = $2 AND tenant_id = $3 AND status = 'dead'`,
		id, dt, tenantID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
