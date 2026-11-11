package ssrf

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"
)

func TestBlocked(t *testing.T) {
	g := &Guard{}
	blocked := []string{
		"127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.1.1", "169.254.169.254",
		"100.64.0.1", "0.0.0.0", "224.0.0.1", "255.255.255.255", "198.18.0.1",
		"::1", "::", "fc00::1", "fd12::1", "fe80::1", "ff02::1",
		"::ffff:127.0.0.1", "::ffff:10.0.0.1", "64:ff9b::a00:1",
	}
	for _, s := range blocked {
		if !g.Blocked(netip.MustParseAddr(s)) {
			t.Errorf("%s should be blocked", s)
		}
	}
	for _, s := range []string{"8.8.8.8", "1.1.1.1", "2606:4700:4700::1111", "93.184.216.34"} {
		if g.Blocked(netip.MustParseAddr(s)) {
			t.Errorf("%s should be allowed", s)
		}
	}
}

func TestAllowlist(t *testing.T) {
	g := &Guard{Allow: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/24")}}
	if g.Blocked(netip.MustParseAddr("10.0.0.5")) {
		t.Fatal("allowlisted address blocked")
	}
	if !g.Blocked(netip.MustParseAddr("10.0.1.5")) {
		t.Fatal("non-allowlisted private address allowed")
	}
}

func TestValidateURL(t *testing.T) {
	g := &Guard{}
	bad := []string{
		"ftp://example.com", "http://example.com", "https://user:pw@example.com",
		"https://127.0.0.1/x", "https://[::1]/", "https://localhost/", "https://a.localhost/",
		"https://169.254.169.254/latest", "https:///nohost", "://bad",
	}
	for _, u := range bad {
		if err := g.ValidateURL(u, false); err == nil {
			t.Errorf("%s should be rejected", u)
		}
	}
	for _, u := range []string{"https://example.com/hook", "https://example.com:8443/h?q=1"} {
		if err := g.ValidateURL(u, false); err != nil {
			t.Errorf("%s: %v", u, err)
		}
	}
	if err := g.ValidateURL("http://example.com", true); err != nil {
		t.Errorf("http with allowHTTP: %v", err)
	}
}

// The dial-time check must catch hostnames that resolve to blocked IPs.
func TestDialControlBlocksResolvedLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer srv.Close()
	_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())

	g := &Guard{}
	tr := &http.Transport{DialContext: g.Dialer(net.Dialer{Timeout: time.Second}).DialContext}
	c := &http.Client{Transport: tr}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://localhost:"+port, nil)
	_, err := c.Do(req)
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("expected ErrBlocked, got %v", err)
	}

	g.Allow = []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128")}
	resp, err := c.Do(req.Clone(context.Background()))
	if err != nil {
		t.Fatalf("allowlisted dial failed: %v", err)
	}
	resp.Body.Close()
}
