package retry

import (
	"testing"
	"time"

	"webhookd/internal/model"
)

func TestDefaultValidAndWindow(t *testing.T) {
	if err := Default.Validate(); err != nil {
		t.Fatal(err)
	}
	// 5+10+...+2560s (10 retries) = 5115s, plus 4 x 1h = 14400s.
	want := 5115*time.Second + 4*time.Hour
	if got := Default.Window(); got != want {
		t.Fatalf("window = %s, want %s", got, want)
	}
}

func TestBackoffCap(t *testing.T) {
	cases := map[int]time.Duration{1: 5 * time.Second, 2: 10 * time.Second, 10: 2560 * time.Second, 11: time.Hour, 500: time.Hour}
	for n, want := range cases {
		if got := Default.Backoff(n); got != want {
			t.Errorf("Backoff(%d) = %s, want %s", n, got, want)
		}
	}
}

func TestDelayJitterBounds(t *testing.T) {
	for _, j := range []Jitter{JitterFull, JitterEqual} {
		p := Default
		p.Jitter = j
		for n := 1; n <= 14; n++ {
			b := p.Backoff(n)
			lo := time.Duration(0)
			if j == JitterEqual {
				lo = b / 2
			}
			for range 200 {
				d := p.Delay(n, 0)
				if d < lo || d > b {
					t.Fatalf("%s n=%d: delay %s outside [%s,%s]", j, n, d, lo, b)
				}
			}
		}
	}
}

func TestDelayRetryAfter(t *testing.T) {
	p := Default
	if d := p.Delay(1, 2*time.Minute); d != 2*time.Minute {
		t.Fatalf("retry-after floor not honored: %s", d)
	}
	if d := p.Delay(1, 5*time.Hour); d != time.Hour {
		t.Fatalf("retry-after should be capped at max_delay: %s", d)
	}
}

func TestResolveLayering(t *testing.T) {
	five, three := 5, 3
	ten := model.Duration(10 * time.Second)
	tenant := &model.RetryPolicy{MaxAttempts: &five, BaseDelay: &ten}
	ep := &model.RetryPolicy{MaxAttempts: &three}
	p, err := Resolve(Default, tenant, nil, ep)
	if err != nil {
		t.Fatal(err)
	}
	if p.MaxAttempts != 3 || p.BaseDelay != 10*time.Second || p.MaxDelay != time.Hour {
		t.Fatalf("unexpected policy %+v", p)
	}
}

func TestValidateRejects(t *testing.T) {
	n100, n0 := 100, 0
	big := model.Duration(2 * time.Hour)
	bad := model.Duration(time.Minute)
	j := "none"
	cases := []*model.RetryPolicy{
		{MaxAttempts: &n0},
		{Timeout: &bad},                      // > 30s
		{Jitter: &j},                         // unknown
		{MaxAttempts: &n100, MaxDelay: &big}, // window > 72h
	}
	for i, c := range cases {
		if _, err := Resolve(Default, c); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}

func TestExhausted(t *testing.T) {
	if Default.Exhausted(14) || !Default.Exhausted(15) {
		t.Fatal("exhausted boundary wrong")
	}
}
