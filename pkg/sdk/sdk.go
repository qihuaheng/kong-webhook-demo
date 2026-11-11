// Package sdk is the Go client for the webhookd API.
//
// Producers:
//
//	c := sdk.New("https://webhooks.example.com", apiKey)
//	res, err := c.SendEvent(ctx, "payment.succeeded", map[string]any{"id": "pay_1"}, nil)
//
// Receivers verify requests with webhook.VerifyRequest (re-exported as Verify*).
package sdk

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"webhookd/pkg/webhook"
)

// Re-exports for receivers.
var (
	Verify        = webhook.Verify
	VerifyRequest = webhook.VerifyRequest
)

type Client struct {
	BaseURL    string
	APIKey     string
	HTTP       *http.Client
	MaxRetries int // for SendEvent; default 3
}

func New(baseURL, apiKey string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), APIKey: apiKey,
		HTTP: &http.Client{Timeout: 15 * time.Second}, MaxRetries: 3}
}

// APIError is a non-2xx response.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("webhookd: HTTP %d: %s", e.StatusCode, e.Message)
}

// IsNotFound reports whether err is a 404 from the API.
func IsNotFound(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.StatusCode == http.StatusNotFound
}

func (c *Client) do(ctx context.Context, method, path string, in, out any, hdr http.Header) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return err
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		json.Unmarshal(data, &e)
		if e.Error == "" {
			e.Error = strings.TrimSpace(string(data))
		}
		return &APIError{StatusCode: resp.StatusCode, Message: e.Error}
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

// --- events ---

type SendOptions struct {
	// IdempotencyKey deduplicates within 24h. If empty, a random key is
	// generated so internal retries never create duplicate events.
	IdempotencyKey string
}

type SendResult struct {
	EventID     string   `json:"event_id"`
	DeliveryIDs []string `json:"delivery_ids"`
	Duplicate   bool     `json:"duplicate"`
}

// SendEvent publishes an event, retrying transient failures (network, 429,
// 5xx) with backoff. data must marshal to JSON.
func (c *Client) SendEvent(ctx context.Context, eventType string, data any, opts *SendOptions) (*SendResult, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshal data: %w", err)
	}
	key := ""
	if opts != nil {
		key = opts.IdempotencyKey
	}
	if key == "" {
		b := make([]byte, 16)
		if _, err := cryptoRead(b); err != nil {
			return nil, err
		}
		key = "sdk_" + hex.EncodeToString(b)
	}
	in := map[string]any{"event_type": eventType, "data": json.RawMessage(raw)}
	hdr := http.Header{"Idempotency-Key": {key}}
	var out SendResult
	for attempt := 0; ; attempt++ {
		err = c.do(ctx, http.MethodPost, "/v1/events", in, &out, hdr)
		if err == nil || attempt >= c.MaxRetries || !retryable(err) {
			if err != nil {
				return nil, err
			}
			return &out, nil
		}
		d := time.Duration(200<<attempt) * time.Millisecond
		d = d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d):
		}
	}
}

func retryable(err error) bool {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae.StatusCode == 429 || ae.StatusCode >= 500
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

var cryptoRead = crand.Read

type Event struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id"`
	EventType   string    `json:"event_type"`
	PayloadSize int       `json:"payload_size"`
	CreatedAt   time.Time `json:"created_at"`
}

type Delivery struct {
	ID             string     `json:"id"`
	EventID        string     `json:"event_id"`
	EndpointID     string     `json:"endpoint_id"`
	Status         string     `json:"status"`
	Attempt        int        `json:"attempt"`
	NextAttemptAt  *time.Time `json:"next_attempt_at,omitempty"`
	LastStatusCode *int       `json:"last_status_code,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
	DeadReason     string     `json:"dead_reason,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
}

type Attempt struct {
	Attempt      int       `json:"attempt"`
	StartedAt    time.Time `json:"started_at"`
	DurationMS   int       `json:"duration_ms"`
	StatusCode   *int      `json:"status_code,omitempty"`
	Error        string    `json:"error,omitempty"`
	ResponseBody string    `json:"response_body,omitempty"`
	Redirects    int       `json:"redirects"`
}

func (c *Client) GetEvent(ctx context.Context, id string) (*Event, []Delivery, error) {
	var out struct {
		Event      Event      `json:"event"`
		Deliveries []Delivery `json:"deliveries"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/events/"+url.PathEscape(id), nil, &out, nil); err != nil {
		return nil, nil, err
	}
	return &out.Event, out.Deliveries, nil
}

// --- endpoints ---

// RetryPolicy overrides; durations are Go duration strings ("5s", "1h").
type RetryPolicy struct {
	MaxAttempts  *int     `json:"max_attempts,omitempty"`
	BaseDelay    string   `json:"base_delay,omitempty"`
	Multiplier   *float64 `json:"multiplier,omitempty"`
	MaxDelay     string   `json:"max_delay,omitempty"`
	Jitter       string   `json:"jitter,omitempty"`
	Timeout      string   `json:"timeout,omitempty"`
	MaxRedirects *int     `json:"max_redirects,omitempty"`
}

type Endpoint struct {
	ID             string       `json:"id"`
	URL            string       `json:"url"`
	Description    string       `json:"description,omitempty"`
	EventTypes     []string     `json:"event_types"`
	Status         string       `json:"status"`
	Compression    string       `json:"compression"`
	RetryPolicy    *RetryPolicy `json:"retry_policy,omitempty"`
	MaxConcurrency int          `json:"max_concurrency"`
	CreatedAt      time.Time    `json:"created_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
}

// EndpointInput is used for create (URL and EventTypes required) and
// update (only non-nil fields change).
type EndpointInput struct {
	URL              *string      `json:"url,omitempty"`
	Description      *string      `json:"description,omitempty"`
	EventTypes       []string     `json:"event_types,omitempty"`
	Status           *string      `json:"status,omitempty"`
	Compression      *string      `json:"compression,omitempty"`
	RetryPolicy      *RetryPolicy `json:"retry_policy,omitempty"`
	ClearRetryPolicy bool         `json:"clear_retry_policy,omitempty"`
	MaxConcurrency   *int         `json:"max_concurrency,omitempty"`
}

// CreateEndpoint returns the endpoint and its signing secret (shown once).
func (c *Client) CreateEndpoint(ctx context.Context, in EndpointInput) (*Endpoint, string, error) {
	var out struct {
		Endpoint Endpoint `json:"endpoint"`
		Secret   string   `json:"secret"`
	}
	if err := c.do(ctx, http.MethodPost, "/v1/endpoints", in, &out, nil); err != nil {
		return nil, "", err
	}
	return &out.Endpoint, out.Secret, nil
}

func (c *Client) GetEndpoint(ctx context.Context, id string) (*Endpoint, error) {
	var out struct {
		Endpoint Endpoint `json:"endpoint"`
	}
	return &out.Endpoint, c.do(ctx, http.MethodGet, "/v1/endpoints/"+url.PathEscape(id), nil, &out, nil)
}

func (c *Client) ListEndpoints(ctx context.Context) ([]Endpoint, error) {
	var out struct {
		Endpoints []Endpoint `json:"endpoints"`
	}
	return out.Endpoints, c.do(ctx, http.MethodGet, "/v1/endpoints", nil, &out, nil)
}

func (c *Client) UpdateEndpoint(ctx context.Context, id string, in EndpointInput) (*Endpoint, error) {
	var out struct {
		Endpoint Endpoint `json:"endpoint"`
	}
	return &out.Endpoint, c.do(ctx, http.MethodPatch, "/v1/endpoints/"+url.PathEscape(id), in, &out, nil)
}

func (c *Client) DeleteEndpoint(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/endpoints/"+url.PathEscape(id), nil, nil, nil)
}

// RotateSecret returns the new secret; the old one stays valid for a grace period.
func (c *Client) RotateSecret(ctx context.Context, id string) (string, error) {
	var out struct {
		Secret string `json:"secret"`
	}
	return out.Secret, c.do(ctx, http.MethodPost, "/v1/endpoints/"+url.PathEscape(id)+"/secret/rotate", nil, &out, nil)
}

// --- deliveries & DLQ ---

type ListOptions struct {
	Status     string // deliveries only
	EndpointID string
	Since      time.Time
	Until      time.Time
	Before     string // cursor: NextBefore from the previous page
	Limit      int
}

type DeliveryPage struct {
	Deliveries []Delivery `json:"deliveries"`
	NextBefore string     `json:"next_before,omitempty"`
}

func (o *ListOptions) query() string {
	v := url.Values{}
	if o == nil {
		return ""
	}
	set := func(k, s string) {
		if s != "" {
			v.Set(k, s)
		}
	}
	set("status", o.Status)
	set("endpoint_id", o.EndpointID)
	set("before", o.Before)
	if !o.Since.IsZero() {
		v.Set("since", o.Since.UTC().Format(time.RFC3339))
	}
	if !o.Until.IsZero() {
		v.Set("until", o.Until.UTC().Format(time.RFC3339))
	}
	if o.Limit > 0 {
		v.Set("limit", strconv.Itoa(o.Limit))
	}
	if len(v) == 0 {
		return ""
	}
	return "?" + v.Encode()
}

func (c *Client) ListDeliveries(ctx context.Context, o *ListOptions) (*DeliveryPage, error) {
	var out DeliveryPage
	return &out, c.do(ctx, http.MethodGet, "/v1/deliveries"+o.query(), nil, &out, nil)
}

func (c *Client) GetDelivery(ctx context.Context, id string) (*Delivery, error) {
	var out struct {
		Delivery Delivery `json:"delivery"`
	}
	return &out.Delivery, c.do(ctx, http.MethodGet, "/v1/deliveries/"+url.PathEscape(id), nil, &out, nil)
}

func (c *Client) ListAttempts(ctx context.Context, deliveryID string) ([]Attempt, error) {
	var out struct {
		Attempts []Attempt `json:"attempts"`
	}
	return out.Attempts, c.do(ctx, http.MethodGet, "/v1/deliveries/"+url.PathEscape(deliveryID)+"/attempts", nil, &out, nil)
}

func (c *Client) ListDLQ(ctx context.Context, o *ListOptions) (*DeliveryPage, error) {
	var out DeliveryPage
	return &out, c.do(ctx, http.MethodGet, "/v1/dlq"+o.query(), nil, &out, nil)
}

// Replay re-delivers one dead delivery with the same webhook-id.
func (c *Client) Replay(ctx context.Context, deliveryID string) error {
	return c.do(ctx, http.MethodPost, "/v1/dlq/"+url.PathEscape(deliveryID)+"/replay", nil, nil, nil)
}

type ReplayFilter struct {
	EndpointID string     `json:"endpoint_id,omitempty"`
	Since      *time.Time `json:"since,omitempty"`
	Until      *time.Time `json:"until,omitempty"`
	Limit      int        `json:"limit,omitempty"`
}

// ReplayBulk replays up to Limit (max 1000) dead deliveries; returns how
// many were replayed. Call repeatedly until it returns 0.
func (c *Client) ReplayBulk(ctx context.Context, f ReplayFilter) (int, error) {
	var out struct {
		Replayed int `json:"replayed"`
	}
	return out.Replayed, c.do(ctx, http.MethodPost, "/v1/dlq/replay", f, &out, nil)
}

func (c *Client) DeleteDead(ctx context.Context, deliveryID string) error {
	return c.do(ctx, http.MethodDelete, "/v1/dlq/"+url.PathEscape(deliveryID), nil, nil, nil)
}

// SetNotifyURL configures DLQ notifications and returns the secret used to
// sign them. An empty URL disables notifications.
func (c *Client) SetNotifyURL(ctx context.Context, u string) (string, error) {
	var out struct {
		Secret string `json:"secret"`
	}
	return out.Secret, c.do(ctx, http.MethodPut, "/v1/tenant/notify", map[string]string{"url": u}, &out, nil)
}

// --- admin ---

type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateTenant requires a client constructed with the admin key. It returns
// the tenant and its API key (shown once).
func (c *Client) CreateTenant(ctx context.Context, name string, policy *RetryPolicy) (*Tenant, string, error) {
	var out struct {
		Tenant Tenant `json:"tenant"`
		APIKey string `json:"api_key"`
	}
	in := map[string]any{"name": name}
	if policy != nil {
		in["retry_policy"] = policy
	}
	if err := c.do(ctx, http.MethodPost, "/admin/v1/tenants", in, &out, nil); err != nil {
		return nil, "", err
	}
	return &out.Tenant, out.APIKey, nil
}
