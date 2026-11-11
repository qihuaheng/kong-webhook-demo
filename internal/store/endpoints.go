package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"webhookd/internal/model"
	"webhookd/pkg/webhook"
)

const endpointCols = `id, tenant_id, url, description, event_types, status, sign_alg, compression,
	retry_policy, max_concurrency, created_at, updated_at`

func scanEndpoint(row pgx.Row, extra ...any) (*model.Endpoint, error) {
	var e model.Endpoint
	var pj []byte
	dst := append([]any{&e.ID, &e.TenantID, &e.URL, &e.Description, &e.EventTypes, &e.Status, &e.SignAlg,
		&e.Compression, &pj, &e.MaxConcurrency, &e.CreatedAt, &e.UpdatedAt}, extra...)
	if err := row.Scan(dst...); err != nil {
		return nil, notFound(err)
	}
	var err error
	e.RetryPolicy, err = unmarshalPolicy(pj)
	return &e, err
}

// CreateEndpoint stores e (ID and timestamps are assigned) and returns its secret.
func (s *Store) CreateEndpoint(ctx context.Context, e *model.Endpoint) (string, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	e.ID = model.NewID("ep")
	e.Status = model.EndpointActive
	if e.SignAlg == "" {
		e.SignAlg = model.SignHMACSHA256
	}
	if e.Compression == "" {
		e.Compression = model.CompressionNone
	}
	secret := webhook.NewSecret()
	pj, err := marshalPolicy(e.RetryPolicy)
	if err != nil {
		return "", err
	}
	err = s.pool.QueryRow(ctx, `
		INSERT INTO endpoints (id, tenant_id, url, description, event_types, status, sign_alg, compression,
			secret_enc, retry_policy, max_concurrency)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING created_at, updated_at`,
		e.ID, e.TenantID, e.URL, e.Description, e.EventTypes, e.Status, e.SignAlg, e.Compression,
		s.box.Seal([]byte(secret), []byte(e.ID)), pj, e.MaxConcurrency).Scan(&e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		return "", err
	}
	return secret, nil
}

func (s *Store) GetEndpoint(ctx context.Context, tenantID, id string) (*model.Endpoint, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	return scanEndpoint(s.pool.QueryRow(ctx, `SELECT `+endpointCols+` FROM endpoints
		WHERE id = $1 AND tenant_id = $2 AND status <> 'deleted'`, id, tenantID))
}

// ListEndpoints returns a tenant's non-deleted endpoints. When activeOnly is
// set it returns only active ones (used for subscription matching at ingest).
func (s *Store) ListEndpoints(ctx context.Context, tenantID string, activeOnly bool) ([]*model.Endpoint, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	q := `SELECT ` + endpointCols + ` FROM endpoints WHERE tenant_id = $1 AND status <> 'deleted'`
	if activeOnly {
		q += ` AND status = 'active'`
	}
	rows, err := s.pool.Query(ctx, q+` ORDER BY id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.Endpoint
	for rows.Next() {
		e, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// EndpointPatch holds optional updates; nil fields are unchanged.
type EndpointPatch struct {
	URL            *string
	Description    *string
	EventTypes     []string
	Status         *model.EndpointStatus
	Compression    *model.Compression
	RetryPolicy    *model.RetryPolicy
	ClearPolicy    bool
	MaxConcurrency *int
}

func (s *Store) UpdateEndpoint(ctx context.Context, tenantID, id string, p EndpointPatch) (*model.Endpoint, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	var pj []byte
	var err error
	if p.RetryPolicy != nil {
		if pj, err = marshalPolicy(p.RetryPolicy); err != nil {
			return nil, err
		}
	}
	var types []string
	if p.EventTypes != nil {
		types = p.EventTypes
	}
	return scanEndpoint(s.pool.QueryRow(ctx, `
		UPDATE endpoints SET
			url             = COALESCE($3, url),
			description     = COALESCE($4, description),
			event_types     = COALESCE($5, event_types),
			status          = COALESCE($6, status),
			compression     = COALESCE($7, compression),
			retry_policy    = CASE WHEN $9 THEN NULL ELSE COALESCE($8, retry_policy) END,
			max_concurrency = COALESCE($10, max_concurrency),
			updated_at      = now()
		WHERE id = $1 AND tenant_id = $2 AND status <> 'deleted'
		RETURNING `+endpointCols,
		id, tenantID, p.URL, p.Description, types, p.Status, p.Compression, pj, p.ClearPolicy, p.MaxConcurrency))
}

// DeleteEndpoint soft-deletes so delivery history stays queryable. In-flight
// deliveries are moved to dead lazily by workers (endpoint_disabled).
func (s *Store) DeleteEndpoint(ctx context.Context, tenantID, id string) error {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `UPDATE endpoints SET status = 'deleted', updated_at = now()
		WHERE id = $1 AND tenant_id = $2 AND status <> 'deleted'`, id, tenantID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// RotateSecret issues a new secret; the old one stays valid for grace.
func (s *Store) RotateSecret(ctx context.Context, tenantID, id string, grace time.Duration) (string, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	secret := webhook.NewSecret()
	tag, err := s.pool.Exec(ctx, `
		UPDATE endpoints SET prev_secret_enc = secret_enc, prev_secret_expires_at = now() + $4 * interval '1 second',
			secret_enc = $3, updated_at = now()
		WHERE id = $1 AND tenant_id = $2 AND status <> 'deleted'`,
		id, tenantID, s.box.Seal([]byte(secret), []byte(id)), int64(grace.Seconds()))
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "", ErrNotFound
	}
	return secret, nil
}

// DispatchTarget is everything a worker needs about an endpoint.
type DispatchTarget struct {
	Endpoint     *model.Endpoint // includes decrypted secrets
	TenantPolicy *model.RetryPolicy
}

// GetDispatchTarget loads an endpoint (any status, so workers can see
// disabled/deleted) with decrypted secrets and the tenant's retry policy.
func (s *Store) GetDispatchTarget(ctx context.Context, id string) (*DispatchTarget, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	var enc, prevEnc, tpj []byte
	var prevUntil *time.Time
	e, err := scanEndpoint(s.pool.QueryRow(ctx, `
		SELECT e.id, e.tenant_id, e.url, e.description, e.event_types, e.status, e.sign_alg, e.compression,
			e.retry_policy, e.max_concurrency, e.created_at, e.updated_at,
			e.secret_enc, e.prev_secret_enc, e.prev_secret_expires_at, t.retry_policy
		FROM endpoints e JOIN tenants t ON t.id = e.tenant_id WHERE e.id = $1`, id),
		&enc, &prevEnc, &prevUntil, &tpj)
	if err != nil {
		return nil, err
	}
	pt, err := s.box.Open(enc, []byte(e.ID))
	if err != nil {
		return nil, err
	}
	e.Secret = string(pt)
	if prevEnc != nil && prevUntil != nil {
		if pt, err := s.box.Open(prevEnc, []byte(e.ID)); err == nil {
			e.PrevSecret, e.PrevSecretUntil = string(pt), prevUntil
		}
	}
	tp, err := unmarshalPolicy(tpj)
	if err != nil {
		return nil, err
	}
	return &DispatchTarget{Endpoint: e, TenantPolicy: tp}, nil
}
