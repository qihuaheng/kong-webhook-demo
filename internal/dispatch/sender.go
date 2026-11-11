// Package dispatch performs webhook HTTP delivery (sender.go) and runs the
// queue-consuming worker (worker.go). See design §3.2 and §5.
package dispatch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"webhookd/internal/model"
	"webhookd/internal/ssrf"
	"webhookd/pkg/webhook"
)

const (
	maxRecordedBody = 4 << 10  // response bytes stored in attempt history
	maxDrainBody    = 64 << 10 // response bytes read to allow connection reuse
	maxErrLen       = 1024
	UserAgent       = "webhookd/1.0"
)

// Verdict is the classification of one delivery attempt.
type Verdict int

const (
	Success Verdict = iota
	Retry           // transient: timeout, network, 408, 429, 5xx
	Fail            // permanent: other 4xx, SSRF, redirect limit
)

func (v Verdict) String() string {
	return [...]string{"success", "retry", "dead"}[v]
}

// Request is one signed delivery attempt.
type Request struct {
	URL             string
	MsgID           string // webhook-id
	Body            []byte // uncompressed body; what is signed
	WireBody        []byte // bytes sent (Body, or zstd-compressed Body)
	ContentEncoding string // "" or "zstd"
	Signers         []webhook.Signer
	Timeout         time.Duration // whole attempt including redirects
	MaxRedirects    int
}

// Result describes the outcome of an attempt.
type Result struct {
	Verdict    Verdict
	Reason     model.DeadReason // set when Verdict == Fail
	StatusCode int              // 0 if no response
	Err        string           // human-readable failure description
	RespBody   string           // truncated, sanitized
	RetryAfter time.Duration
	Redirects  int
	Duration   time.Duration
}

// Sender sends signed POSTs with SSRF protection and redirect handling.
type Sender struct {
	client    *http.Client
	guard     *ssrf.Guard
	allowHTTP bool
}

func NewSender(guard *ssrf.Guard, allowHTTP bool, maxConnsPerHost int) *Sender {
	tr := &http.Transport{
		// No proxy: a proxy would make the dial-time SSRF check meaningless.
		Proxy:                 nil,
		DialContext:           guard.Dialer(net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          4096,
		MaxIdleConnsPerHost:   maxConnsPerHost,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 0, // bounded by the per-attempt context
		DisableCompression:    true,
	}
	return &Sender{
		client: &http.Client{
			Transport: tr,
			// Redirects are followed manually so POST + body are preserved for
			// every 3xx (Go's client would switch 301/302/303 to GET).
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		guard:     guard,
		allowHTTP: allowHTTP,
	}
}

// Send performs one attempt: sign once, then POST, following redirects.
func (s *Sender) Send(ctx context.Context, r Request) Result {
	start := time.Now()
	res := s.send(ctx, r, start)
	res.Duration = time.Since(start)
	return res
}

func (s *Sender) send(ctx context.Context, r Request, start time.Time) Result {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()

	ts := start.Unix()
	sig := webhook.SignatureHeader(r.Signers, r.MsgID, ts, r.Body)
	target := r.URL
	var res Result
	for {
		if err := s.guard.ValidateURL(target, s.allowHTTP); err != nil {
			if errors.Is(err, ssrf.ErrBlocked) {
				return fail(res, model.DeadSSRFBlocked, "blocked destination: "+target)
			}
			return fail(res, model.DeadNonRetryableStatus, "invalid destination: "+err.Error())
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(r.WireBody))
		if err != nil {
			return fail(res, model.DeadNonRetryableStatus, err.Error())
		}
		h := req.Header
		h.Set("Content-Type", "application/json")
		h.Set("User-Agent", UserAgent)
		h.Set(webhook.HeaderID, r.MsgID)
		h.Set(webhook.HeaderTimestamp, strconv.FormatInt(ts, 10))
		h.Set(webhook.HeaderSignature, sig)
		if r.ContentEncoding != "" {
			h.Set("Content-Encoding", r.ContentEncoding)
		}

		resp, err := s.client.Do(req)
		if err != nil {
			if errors.Is(err, ssrf.ErrBlocked) {
				return fail(res, model.DeadSSRFBlocked, truncate(err.Error(), maxErrLen))
			}
			res.Verdict, res.Err = Retry, describeNetErr(ctx, err)
			return res
		}
		res.StatusCode = resp.StatusCode
		res.RespBody = readBody(resp.Body)

		if isRedirect(resp.StatusCode) {
			loc := resp.Header.Get("Location")
			if loc == "" {
				return fail(res, model.DeadNonRetryableStatus, fmt.Sprintf("HTTP %d without Location", resp.StatusCode))
			}
			if res.Redirects >= r.MaxRedirects {
				return fail(res, model.DeadTooManyRedirects, fmt.Sprintf("exceeded %d redirects", r.MaxRedirects))
			}
			next, err := req.URL.Parse(loc)
			if err != nil {
				return fail(res, model.DeadNonRetryableStatus, "invalid redirect Location: "+truncate(loc, 200))
			}
			if req.URL.Scheme == "https" && next.Scheme != "https" {
				return fail(res, model.DeadNonRetryableStatus, "redirect from https to "+next.Scheme+" refused")
			}
			res.Redirects++
			target = next.String()
			continue
		}
		return classify(res, resp)
	}
}

func classify(res Result, resp *http.Response) Result {
	c := resp.StatusCode
	switch {
	case c >= 200 && c < 300:
		res.Verdict = Success
	case c == http.StatusRequestTimeout || c == http.StatusTooManyRequests || c >= 500:
		res.Verdict, res.Err = Retry, "HTTP "+strconv.Itoa(c)
		if c == http.StatusTooManyRequests || c == http.StatusServiceUnavailable {
			res.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
		}
	default:
		return fail(res, model.DeadNonRetryableStatus, "HTTP "+strconv.Itoa(c))
	}
	return res
}

func isRedirect(c int) bool {
	switch c {
	case 301, 302, 303, 307, 308:
		return true
	}
	return false
}

func fail(res Result, reason model.DeadReason, msg string) Result {
	res.Verdict, res.Reason, res.Err = Fail, reason, msg
	return res
}

// parseRetryAfter accepts delta-seconds or an HTTP-date.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v = strings.TrimSpace(v); v == "" {
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

func describeNetErr(ctx context.Context, err error) string {
	if ctx.Err() == context.DeadlineExceeded {
		return "timeout"
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err
	}
	return truncate(err.Error(), maxErrLen)
}

// readBody records up to maxRecordedBody bytes and drains a bit more so the
// connection can be reused; then closes.
func readBody(rc io.ReadCloser) string {
	defer rc.Close()
	b, _ := io.ReadAll(io.LimitReader(rc, maxRecordedBody))
	io.Copy(io.Discard, io.LimitReader(rc, maxDrainBody))
	return sanitize(b)
}

// sanitize makes arbitrary bytes safe for a PostgreSQL text column.
func sanitize(b []byte) string {
	s := strings.ReplaceAll(string(b), "\x00", "")
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "\uFFFD")
	}
	return s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "")
}
