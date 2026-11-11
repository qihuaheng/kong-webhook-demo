// Package retry implements the exponential backoff + jitter policy (design §5).
package retry

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"webhookd/internal/model"
)

type Jitter string

const (
	JitterFull  Jitter = "full"
	JitterEqual Jitter = "equal"
)

// MaxWindow bounds the nominal total retry window so every in-flight delivery
// stays within payload retention (design §8.3).
const MaxWindow = 72 * time.Hour

const (
	maxTimeout      = 30 * time.Second
	maxRedirectsCap = 10
	maxAttemptsCap  = 100
)

// Policy is a fully resolved retry policy.
type Policy struct {
	MaxAttempts  int // including the first attempt
	BaseDelay    time.Duration
	Multiplier   float64
	MaxDelay     time.Duration
	Jitter       Jitter
	Timeout      time.Duration
	MaxRedirects int
}

// Default is the global default policy (design §15 decision 5).
var Default = Policy{
	MaxAttempts:  15,
	BaseDelay:    5 * time.Second,
	Multiplier:   2.0,
	MaxDelay:     time.Hour,
	Jitter:       JitterEqual,
	Timeout:      10 * time.Second,
	MaxRedirects: 3,
}

// Resolve applies partial overrides in order (e.g. tenant then endpoint);
// later layers win. The result is validated.
func Resolve(base Policy, layers ...*model.RetryPolicy) (Policy, error) {
	p := base
	for _, l := range layers {
		if l == nil {
			continue
		}
		if l.MaxAttempts != nil {
			p.MaxAttempts = *l.MaxAttempts
		}
		if l.BaseDelay != nil {
			p.BaseDelay = l.BaseDelay.D()
		}
		if l.Multiplier != nil {
			p.Multiplier = *l.Multiplier
		}
		if l.MaxDelay != nil {
			p.MaxDelay = l.MaxDelay.D()
		}
		if l.Jitter != nil {
			p.Jitter = Jitter(*l.Jitter)
		}
		if l.Timeout != nil {
			p.Timeout = l.Timeout.D()
		}
		if l.MaxRedirects != nil {
			p.MaxRedirects = *l.MaxRedirects
		}
	}
	return p, p.Validate()
}

func (p Policy) Validate() error {
	switch {
	case p.MaxAttempts < 1 || p.MaxAttempts > maxAttemptsCap:
		return fmt.Errorf("max_attempts must be in [1,%d]", maxAttemptsCap)
	case p.BaseDelay < time.Second:
		return fmt.Errorf("base_delay must be >= 1s")
	case p.Multiplier < 1 || p.Multiplier > 10:
		return fmt.Errorf("multiplier must be in [1,10]")
	case p.MaxDelay < p.BaseDelay:
		return fmt.Errorf("max_delay must be >= base_delay")
	case p.Jitter != JitterFull && p.Jitter != JitterEqual:
		return fmt.Errorf("jitter must be \"full\" or \"equal\"")
	case p.Timeout < time.Second || p.Timeout > maxTimeout:
		return fmt.Errorf("timeout must be in [1s,%s]", maxTimeout)
	case p.MaxRedirects < 0 || p.MaxRedirects > maxRedirectsCap:
		return fmt.Errorf("max_redirects must be in [0,%d]", maxRedirectsCap)
	}
	if w := p.Window(); w > MaxWindow {
		return fmt.Errorf("nominal retry window %s exceeds %s", w, MaxWindow)
	}
	return nil
}

// Backoff is the nominal (pre-jitter) delay before retry n (1-based).
func (p Policy) Backoff(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	d := float64(p.BaseDelay) * math.Pow(p.Multiplier, float64(n-1))
	if d >= float64(p.MaxDelay) || math.IsInf(d, 0) {
		return p.MaxDelay
	}
	return time.Duration(d)
}

// Window is the nominal total time from first attempt to last attempt.
func (p Policy) Window() time.Duration {
	var w time.Duration
	for n := 1; n < p.MaxAttempts; n++ {
		w += p.Backoff(n)
	}
	return w
}

// Delay returns the jittered delay before retry n. retryAfter (from a 429/503
// response, 0 if absent) acts as a floor, itself capped at MaxDelay.
func (p Policy) Delay(n int, retryAfter time.Duration) time.Duration {
	b := p.Backoff(n)
	var d time.Duration
	switch p.Jitter {
	case JitterFull:
		d = time.Duration(rand.Int64N(int64(b) + 1))
	default: // equal
		half := b / 2
		d = half + time.Duration(rand.Int64N(int64(b-half)+1))
	}
	if retryAfter > 0 {
		d = max(d, min(retryAfter, p.MaxDelay))
	}
	return d
}

// Exhausted reports whether attemptsMade attempts use up the policy.
func (p Policy) Exhausted(attemptsMade int) bool { return attemptsMade >= p.MaxAttempts }
