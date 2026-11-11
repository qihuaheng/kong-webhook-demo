// Package webhook implements Standard Webhooks (https://www.standardwebhooks.com)
// signing and verification. It is used by the server to sign outbound requests
// and by receivers (via the SDK) to verify them.
package webhook

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	HeaderID        = "webhook-id"
	HeaderTimestamp = "webhook-timestamp"
	HeaderSignature = "webhook-signature"

	secretPrefix = "whsec_"
	// MaxBodyBytes bounds how much VerifyRequest will read (after decompression).
	MaxBodyBytes = 1 << 20
)

var (
	ErrMissingHeaders   = errors.New("webhook: missing required headers")
	ErrInvalidTimestamp = errors.New("webhook: timestamp outside tolerance")
	ErrNoMatch          = errors.New("webhook: no matching signature")
	ErrInvalidSecret    = errors.New("webhook: invalid secret")
)

// Signer produces one versioned signature, e.g. "v1,<base64>". HMAC-SHA256
// ("v1") is implemented; Ed25519 ("v1a") is reserved for a future Signer.
type Signer interface {
	Sign(msgID string, ts int64, body []byte) string
}

// HMACSigner signs with HMAC-SHA256 ("v1").
type HMACSigner struct{ key []byte }

// NewHMACSigner parses a "whsec_<base64>" secret.
func NewHMACSigner(secret string) (*HMACSigner, error) {
	key, err := decodeSecret(secret)
	if err != nil {
		return nil, err
	}
	return &HMACSigner{key: key}, nil
}

func (s *HMACSigner) Sign(msgID string, ts int64, body []byte) string {
	return "v1," + base64.StdEncoding.EncodeToString(s.mac(msgID, ts, body))
}

func (s *HMACSigner) mac(msgID string, ts int64, body []byte) []byte {
	m := hmac.New(sha256.New, s.key)
	m.Write([]byte(msgID))
	m.Write([]byte{'.'})
	m.Write([]byte(strconv.FormatInt(ts, 10)))
	m.Write([]byte{'.'})
	m.Write(body)
	return m.Sum(nil)
}

// NewSecret generates a random 32-byte secret in "whsec_<base64>" form.
func NewSecret() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	return secretPrefix + base64.StdEncoding.EncodeToString(b)
}

func decodeSecret(secret string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, secretPrefix))
	if err != nil || len(key) == 0 {
		return nil, ErrInvalidSecret
	}
	return key, nil
}

// SignatureHeader signs with every signer and joins the results with spaces,
// which lets receivers verify with either secret during rotation.
func SignatureHeader(signers []Signer, msgID string, ts int64, body []byte) string {
	parts := make([]string, len(signers))
	for i, s := range signers {
		parts[i] = s.Sign(msgID, ts, body)
	}
	return strings.Join(parts, " ")
}

// Verify checks the headers against the (uncompressed) body. tolerance bounds
// clock skew and replay age; use 5 minutes unless you have a reason not to.
func Verify(secret string, h http.Header, body []byte, tolerance time.Duration) error {
	return verifyAt(secret, h, body, tolerance, time.Now())
}

func verifyAt(secret string, h http.Header, body []byte, tolerance time.Duration, now time.Time) error {
	id, tsStr, sigs := h.Get(HeaderID), h.Get(HeaderTimestamp), h.Get(HeaderSignature)
	if id == "" || tsStr == "" || sigs == "" {
		return ErrMissingHeaders
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return ErrInvalidTimestamp
	}
	if d := now.Sub(time.Unix(ts, 0)); d > tolerance || d < -tolerance {
		return ErrInvalidTimestamp
	}
	s, err := NewHMACSigner(secret)
	if err != nil {
		return err
	}
	expected := s.mac(id, ts, body)
	for _, sig := range strings.Fields(sigs) {
		ver, val, ok := strings.Cut(sig, ",")
		if !ok || ver != "v1" {
			continue
		}
		got, err := base64.StdEncoding.DecodeString(val)
		if err == nil && hmac.Equal(got, expected) {
			return nil
		}
	}
	return ErrNoMatch
}

// VerifyRequest reads the request body, transparently decodes
// "Content-Encoding: zstd", verifies the signature, and returns the plain body.
func VerifyRequest(secret string, r *http.Request, tolerance time.Duration) ([]byte, error) {
	var rd io.Reader = r.Body
	switch enc := strings.ToLower(r.Header.Get("Content-Encoding")); enc {
	case "", "identity":
	case "zstd":
		zr, err := zstd.NewReader(r.Body, zstd.WithDecoderMaxMemory(MaxBodyBytes*2))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		rd = zr
	default:
		return nil, fmt.Errorf("webhook: unsupported content-encoding %q", enc)
	}
	body, err := io.ReadAll(io.LimitReader(rd, MaxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > MaxBodyBytes {
		return nil, errors.New("webhook: body too large")
	}
	return body, Verify(secret, r.Header, body, tolerance)
}
