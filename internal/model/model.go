// Package model holds the core domain types shared across webhookd components.
package model

import (
	"crypto/rand"
	"time"

	"github.com/oklog/ulid/v2"
)

// DeliveryStatus is the state of a single (event, endpoint) delivery. See design §4.
type DeliveryStatus string

const (
	StatusPending   DeliveryStatus = "pending"
	StatusRetrying  DeliveryStatus = "retrying"
	StatusSucceeded DeliveryStatus = "succeeded"
	StatusDead      DeliveryStatus = "dead"
)

// Terminal reports whether no further delivery attempts will be made.
func (s DeliveryStatus) Terminal() bool { return s == StatusSucceeded || s == StatusDead }

// DeadReason explains why a delivery ended in the DLQ.
type DeadReason string

const (
	DeadExhausted          DeadReason = "exhausted"
	DeadNonRetryableStatus DeadReason = "non_retryable_status"
	DeadTooManyRedirects   DeadReason = "too_many_redirects"
	DeadSSRFBlocked        DeadReason = "ssrf_blocked"
	DeadEndpointDisabled   DeadReason = "endpoint_disabled"
	DeadPayloadExpired     DeadReason = "payload_expired"
)

type EndpointStatus string

const (
	EndpointActive   EndpointStatus = "active"
	EndpointDisabled EndpointStatus = "disabled"
	EndpointDeleted  EndpointStatus = "deleted" // soft delete; hidden from the API
)

type Compression string

const (
	CompressionNone Compression = "none"
	CompressionZstd Compression = "zstd"
)

type SignAlg string

const (
	SignHMACSHA256 SignAlg = "hmac-sha256"
	SignEd25519    SignAlg = "ed25519" // reserved, not implemented (design §6)
)

// RetryPolicy is a partial policy; nil fields inherit from the next level up
// (endpoint → tenant → global). Durations are JSON strings like "5s".
type RetryPolicy struct {
	MaxAttempts  *int      `json:"max_attempts,omitempty"`
	BaseDelay    *Duration `json:"base_delay,omitempty"`
	Multiplier   *float64  `json:"multiplier,omitempty"`
	MaxDelay     *Duration `json:"max_delay,omitempty"`
	Jitter       *string   `json:"jitter,omitempty"` // "full" | "equal"
	Timeout      *Duration `json:"timeout,omitempty"`
	MaxRedirects *int      `json:"max_redirects,omitempty"`
}

type Tenant struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	NotifyURL   string       `json:"notify_url,omitempty"`
	RetryPolicy *RetryPolicy `json:"retry_policy,omitempty"`
	CreatedAt   time.Time    `json:"created_at"`
}

type Endpoint struct {
	ID             string         `json:"id"`
	TenantID       string         `json:"tenant_id"`
	URL            string         `json:"url"`
	Description    string         `json:"description,omitempty"`
	EventTypes     []string       `json:"event_types"`
	Status         EndpointStatus `json:"status"`
	SignAlg        SignAlg        `json:"sign_alg"`
	Compression    Compression    `json:"compression"`
	RetryPolicy    *RetryPolicy   `json:"retry_policy,omitempty"`
	MaxConcurrency int            `json:"max_concurrency"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`

	// Decrypted secrets; never serialized.
	Secret          string     `json:"-"`
	PrevSecret      string     `json:"-"`
	PrevSecretUntil *time.Time `json:"-"`
}

// ActiveSecrets returns the secrets to sign with: current first, then the
// previous one while its rotation grace period lasts.
func (e *Endpoint) ActiveSecrets(now time.Time) []string {
	s := []string{e.Secret}
	if e.PrevSecret != "" && e.PrevSecretUntil != nil && now.Before(*e.PrevSecretUntil) {
		s = append(s, e.PrevSecret)
	}
	return s
}

type Event struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	EventType      string    `json:"event_type"`
	PayloadKey     string    `json:"-"`
	PayloadSize    int       `json:"payload_size"`
	StoredSize     int       `json:"-"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	TraceParent    string    `json:"-"`
	CreatedAt      time.Time `json:"created_at"`
}

type Delivery struct {
	ID             string         `json:"id"`
	EventID        string         `json:"event_id"`
	EndpointID     string         `json:"endpoint_id"`
	TenantID       string         `json:"tenant_id"`
	Status         DeliveryStatus `json:"status"`
	Attempt        int            `json:"attempt"`
	NextAttemptAt  *time.Time     `json:"next_attempt_at,omitempty"`
	LastStatusCode *int           `json:"last_status_code,omitempty"`
	LastError      string         `json:"last_error,omitempty"`
	DeadReason     DeadReason     `json:"dead_reason,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	CompletedAt    *time.Time     `json:"completed_at,omitempty"`
}

type Attempt struct {
	DeliveryID   string    `json:"delivery_id"`
	Attempt      int       `json:"attempt"`
	StartedAt    time.Time `json:"started_at"`
	DurationMS   int       `json:"duration_ms"`
	StatusCode   *int      `json:"status_code,omitempty"`
	Error        string    `json:"error,omitempty"`
	ResponseBody string    `json:"response_body,omitempty"`
	Redirects    int       `json:"redirects"`
}

// NewID returns a prefixed, time-ordered ULID, e.g. "msg_01J...".
func NewID(prefix string) string {
	return prefix + "_" + ulid.MustNew(ulid.Now(), rand.Reader).String()
}

// IDTime extracts the creation time embedded in a prefixed ULID.
func IDTime(id string) (time.Time, bool) {
	for i := 0; i < len(id); i++ {
		if id[i] == '_' {
			u, err := ulid.ParseStrict(id[i+1:])
			if err != nil {
				return time.Time{}, false
			}
			return ulid.Time(u.Time()), true
		}
	}
	return time.Time{}, false
}
