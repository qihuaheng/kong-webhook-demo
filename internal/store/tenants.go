package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"

	"webhookd/internal/model"
	"webhookd/pkg/webhook"
)

// HashAPIKey returns the stored form of an API key.
func HashAPIKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

func newAPIKey() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "whk_" + base64.RawURLEncoding.EncodeToString(b)
}

// CreateTenant creates a tenant and returns its API key (shown only once).
func (s *Store) CreateTenant(ctx context.Context, name string, policy *model.RetryPolicy) (*model.Tenant, string, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	t := &model.Tenant{ID: model.NewID("ten"), Name: name, RetryPolicy: policy}
	key := newAPIKey()
	pj, err := marshalPolicy(policy)
	if err != nil {
		return nil, "", err
	}
	err = s.pool.QueryRow(ctx, `
		INSERT INTO tenants (id, name, api_key_hash, retry_policy) VALUES ($1, $2, $3, $4)
		RETURNING created_at`, t.ID, name, HashAPIKey(key), pj).Scan(&t.CreatedAt)
	if err != nil {
		return nil, "", err
	}
	return t, key, nil
}

// TenantByAPIKey authenticates a tenant by API key.
func (s *Store) TenantByAPIKey(ctx context.Context, key string) (*model.Tenant, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	return s.scanTenant(ctx, "api_key_hash = $1", HashAPIKey(key))
}

func (s *Store) GetTenant(ctx context.Context, id string) (*model.Tenant, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	return s.scanTenant(ctx, "id = $1", id)
}

func (s *Store) scanTenant(ctx context.Context, where string, arg any) (*model.Tenant, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	var t model.Tenant
	var url *string
	var pj []byte
	err := s.pool.QueryRow(ctx, `SELECT id, name, notify_url, retry_policy, created_at FROM tenants WHERE `+where, arg).
		Scan(&t.ID, &t.Name, &url, &pj, &t.CreatedAt)
	if err != nil {
		return nil, notFound(err)
	}
	if url != nil {
		t.NotifyURL = *url
	}
	if t.RetryPolicy, err = unmarshalPolicy(pj); err != nil {
		return nil, err
	}
	return &t, nil
}

// SetNotify sets the DLQ notification URL and returns a fresh signing secret
// for it. An empty url disables notifications.
func (s *Store) SetNotify(ctx context.Context, tenantID, url string) (string, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	if url == "" {
		_, err := s.pool.Exec(ctx, `UPDATE tenants SET notify_url = NULL, notify_secret_enc = NULL WHERE id = $1`, tenantID)
		return "", err
	}
	secret := webhook.NewSecret()
	enc := s.box.Seal([]byte(secret), []byte(tenantID))
	tag, err := s.pool.Exec(ctx, `UPDATE tenants SET notify_url = $2, notify_secret_enc = $3 WHERE id = $1`, tenantID, url, enc)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "", ErrNotFound
	}
	return secret, nil
}

// TenantNotify returns the notification URL and secret; url is empty if unset.
func (s *Store) TenantNotify(ctx context.Context, tenantID string) (url, secret string, err error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	var u *string
	var enc []byte
	if err := s.pool.QueryRow(ctx, `SELECT notify_url, notify_secret_enc FROM tenants WHERE id = $1`, tenantID).Scan(&u, &enc); err != nil {
		return "", "", notFound(err)
	}
	if u == nil || enc == nil {
		return "", "", nil
	}
	pt, err := s.box.Open(enc, []byte(tenantID))
	if err != nil {
		return "", "", err
	}
	return *u, string(pt), nil
}

func marshalPolicy(p *model.RetryPolicy) ([]byte, error) {
	if p == nil {
		return nil, nil
	}
	return json.Marshal(p)
}

func unmarshalPolicy(b []byte) (*model.RetryPolicy, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var p model.RetryPolicy
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	return &p, nil
}
