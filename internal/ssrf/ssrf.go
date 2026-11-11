// Package ssrf prevents webhook delivery to internal networks (design §7).
// Checks happen both at URL registration and at dial time; the dial-time
// check sees the final resolved IP, which defeats DNS rebinding.
package ssrf

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
)

var ErrBlocked = errors.New("ssrf: destination address is not allowed")

var blockedPrefixes = mustPrefixes(
	"0.0.0.0/8",       // "this" network
	"100.64.0.0/10",   // CGNAT
	"192.0.0.0/24",    // IETF protocol assignments
	"192.0.2.0/24",    // TEST-NET-1
	"198.18.0.0/15",   // benchmarking
	"198.51.100.0/24", // TEST-NET-2
	"203.0.113.0/24",  // TEST-NET-3
	"240.0.0.0/4",     // reserved + broadcast
	"64:ff9b::/96",    // NAT64 (can reach embedded IPv4)
	"64:ff9b:1::/48",  // local-use NAT64
	"2001:db8::/32",   // documentation
)

func mustPrefixes(ss ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParsePrefix(s)
	}
	return out
}

// Guard decides whether an address may be contacted.
type Guard struct {
	Allow []netip.Prefix // exemptions, e.g. a test network
}

// Blocked reports whether ip is in a disallowed range.
func (g *Guard) Blocked(ip netip.Addr) bool {
	ip = ip.Unmap()
	for _, p := range g.Allow {
		if p.Contains(ip) {
			return false
		}
	}
	if !ip.IsValid() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return true
	}
	for _, p := range blockedPrefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// Control is a net.Dialer.Control hook that rejects blocked addresses.
func (g *Guard) Control(network, address string, _ syscall.RawConn) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrBlocked, address)
	}
	if g.Blocked(ap.Addr()) {
		return fmt.Errorf("%w: %s", ErrBlocked, ap.Addr())
	}
	return nil
}

// ValidateURL performs static checks on an endpoint URL at registration time
// and on every redirect hop.
func (g *Guard) ValidateURL(raw string, allowHTTP bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !allowHTTP {
			return errors.New("url scheme must be https")
		}
	default:
		return errors.New("url scheme must be https")
	}
	if u.User != nil {
		return errors.New("url must not contain userinfo")
	}
	host := u.Hostname()
	if host == "" {
		return errors.New("url must have a host")
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return ErrBlocked
	}
	if ip, err := netip.ParseAddr(host); err == nil && g.Blocked(ip) {
		return ErrBlocked
	}
	return nil
}

// Dialer returns a net.Dialer with the guard installed.
func (g *Guard) Dialer(base net.Dialer) *net.Dialer {
	base.Control = g.Control
	return &base
}
