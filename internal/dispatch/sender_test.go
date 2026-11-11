package dispatch

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"webhookd/internal/model"
	"webhookd/internal/payload"
	"webhookd/internal/ssrf"
	"webhookd/pkg/webhook"
)

var loopback = &ssrf.Guard{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128")}}

func newReq(t *testing.T, url string, body []byte) (Request, string) {
	secret := webhook.NewSecret()
	s, _ := webhook.NewHMACSigner(secret)
	return Request{URL: url, MsgID: "msg_test", Body: body, WireBody: body, Signers: []webhook.Signer{s},
		Timeout: 2 * time.Second, MaxRedirects: 3}, secret
}

func TestSendSignedSuccess(t *testing.T) {
	body := []byte(`{"type":"payment.succeeded"}`)
	var secret string
	var verifyErr error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("User-Agent") != UserAgent {
			w.WriteHeader(400)
			return
		}
		_, verifyErr = webhook.VerifyRequest(secret, r, time.Minute)
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	req, sec := newReq(t, srv.URL, body)
	secret = sec
	res := NewSender(loopback, true, 10).Send(context.Background(), req)
	if res.Verdict != Success || res.StatusCode != 200 || res.RespBody != "ok" {
		t.Fatalf("unexpected %+v", res)
	}
	if verifyErr != nil {
		t.Fatalf("receiver verify: %v", verifyErr)
	}
}

func TestClassification(t *testing.T) {
	cases := []struct {
		code   int
		want   Verdict
		reason model.DeadReason
	}{
		{200, Success, ""}, {204, Success, ""},
		{408, Retry, ""}, {429, Retry, ""}, {500, Retry, ""}, {502, Retry, ""}, {503, Retry, ""},
		{400, Fail, model.DeadNonRetryableStatus}, {401, Fail, model.DeadNonRetryableStatus},
		{404, Fail, model.DeadNonRetryableStatus}, {410, Fail, model.DeadNonRetryableStatus},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(c.code) }))
		req, _ := newReq(t, srv.URL, []byte("{}"))
		res := NewSender(loopback, true, 10).Send(context.Background(), req)
		srv.Close()
		if res.Verdict != c.want || res.Reason != c.reason || res.StatusCode != c.code {
			t.Errorf("%d: got %v/%s want %v/%s", c.code, res.Verdict, res.Reason, c.want, c.reason)
		}
	}
}

func TestRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(429)
	}))
	defer srv.Close()
	req, _ := newReq(t, srv.URL, []byte("{}"))
	if res := NewSender(loopback, true, 10).Send(context.Background(), req); res.RetryAfter != 120*time.Second {
		t.Fatalf("retry-after: %s", res.RetryAfter)
	}
	now := time.Now()
	if d := parseRetryAfter(now.Add(time.Minute).UTC().Format(http.TimeFormat), now); d < 58*time.Second || d > 61*time.Second {
		t.Fatalf("http-date retry-after: %s", d)
	}
}

func TestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(3 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	req, _ := newReq(t, srv.URL, []byte("{}"))
	req.Timeout = 200 * time.Millisecond
	res := NewSender(loopback, true, 10).Send(context.Background(), req)
	if res.Verdict != Retry || res.Err != "timeout" || res.Duration > time.Second {
		t.Fatalf("unexpected %+v", res)
	}
}

func TestConnectionRefusedRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()
	req, _ := newReq(t, url, []byte("{}"))
	if res := NewSender(loopback, true, 10).Send(context.Background(), req); res.Verdict != Retry || res.StatusCode != 0 {
		t.Fatalf("unexpected %+v", res)
	}
}

// Every 3xx keeps POST, body, and signature headers (design §15 decision 1).
func TestRedirectsPreservePOST(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		body := []byte(`{"n":` + strconv.Itoa(code) + `}`)
		var secret string
		var got []byte
		var method string
		var verr error
		mux := http.NewServeMux()
		mux.HandleFunc("/a", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/b", code) })
		mux.HandleFunc("/b", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/final", code) })
		mux.HandleFunc("/final", func(w http.ResponseWriter, r *http.Request) {
			method = r.Method
			got, verr = webhook.VerifyRequest(secret, r, time.Minute)
		})
		srv := httptest.NewServer(mux)
		req, sec := newReq(t, srv.URL+"/a", body)
		secret = sec
		res := NewSender(loopback, true, 10).Send(context.Background(), req)
		srv.Close()
		if res.Verdict != Success || res.Redirects != 2 || method != http.MethodPost || !bytes.Equal(got, body) || verr != nil {
			t.Errorf("%d: verdict=%v redirects=%d method=%s body=%q verify=%v", code, res.Verdict, res.Redirects, method, got, verr)
		}
	}
}

func TestRedirectLimit(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Redirect(w, r, "/loop", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	req, _ := newReq(t, srv.URL, []byte("{}"))
	req.MaxRedirects = 2
	res := NewSender(loopback, true, 10).Send(context.Background(), req)
	if res.Verdict != Fail || res.Reason != model.DeadTooManyRedirects || hits.Load() != 3 {
		t.Fatalf("unexpected %+v hits=%d", res, hits.Load())
	}
}

func TestRedirectToBlockedAddress(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data", http.StatusFound)
	}))
	defer srv.Close()
	req, _ := newReq(t, srv.URL, []byte("{}"))
	res := NewSender(loopback, true, 10).Send(context.Background(), req)
	if res.Verdict != Fail || res.Reason != model.DeadSSRFBlocked {
		t.Fatalf("unexpected %+v", res)
	}
}

func TestDialBlocked(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	_, port, _ := netSplit(srv.URL)
	req, _ := newReq(t, "http://localhost.:"+port, []byte("{}")) // resolves to loopback
	res := NewSender(&ssrf.Guard{}, true, 10).Send(context.Background(), req)
	if res.Verdict != Fail || res.Reason != model.DeadSSRFBlocked {
		t.Fatalf("unexpected %+v", res)
	}
}

func TestZstdWire(t *testing.T) {
	body := bytes.Repeat([]byte(`{"x":1}`), 500)
	var secret string
	var got []byte
	var raw int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, _ := io.ReadAll(r.Body)
		raw = len(buf)
		r.Body = io.NopCloser(bytes.NewReader(buf))
		got, _ = webhook.VerifyRequest(secret, r, time.Minute)
	}))
	defer srv.Close()
	req, sec := newReq(t, srv.URL, body)
	secret = sec
	req.WireBody, req.ContentEncoding = payload.Compress(body), "zstd"
	if res := NewSender(loopback, true, 10).Send(context.Background(), req); res.Verdict != Success {
		t.Fatalf("unexpected %+v", res)
	}
	if !bytes.Equal(got, body) || raw >= len(body) {
		t.Fatalf("zstd wire: raw=%d plain=%d match=%v", raw, len(body), bytes.Equal(got, body))
	}
}

func TestHTTPSDowngradeRefused(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://example.com/", http.StatusFound)
	}))
	defer srv.Close()
	s := NewSender(loopback, true, 10)
	s.client.Transport.(*http.Transport).TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig
	req, _ := newReq(t, srv.URL, []byte("{}"))
	res := s.Send(context.Background(), req)
	if res.Verdict != Fail || res.Reason != model.DeadNonRetryableStatus {
		t.Fatalf("unexpected %+v", res)
	}
}

func netSplit(u string) (string, string, error) {
	h := u[len("http://"):]
	for i := len(h) - 1; i >= 0; i-- {
		if h[i] == ':' {
			return h[:i], h[i+1:], nil
		}
	}
	return h, "", nil
}
