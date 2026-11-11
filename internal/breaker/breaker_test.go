package breaker_test

import (
	"context"
	"testing"
	"time"

	"webhookd/internal/breaker"
	"webhookd/internal/testenv"
)

func TestBreakerLifecycle(t *testing.T) {
	env := testenv.New(t)
	ctx := context.Background()
	b := breaker.New(env.Redis, env.Prefix, 3, 200*time.Millisecond, time.Second)
	ep := "ep_1"

	for i := 0; i < 2; i++ {
		if until, _ := b.Failure(ctx, ep); !until.IsZero() {
			t.Fatalf("opened early at failure %d", i+1)
		}
	}
	if d, _ := b.Allow(ctx, ep, time.Second); !d.Allowed || d.Probe {
		t.Fatal("closed breaker should allow")
	}
	until, _ := b.Failure(ctx, ep) // 3rd failure opens
	if until.IsZero() {
		t.Fatal("threshold failure should open")
	}
	if d, _ := b.Allow(ctx, ep, time.Second); d.Allowed || d.Until.IsZero() {
		t.Fatalf("open breaker should deny: %+v", d)
	}

	time.Sleep(250 * time.Millisecond) // half-open
	d, _ := b.Allow(ctx, ep, time.Second)
	if !d.Allowed || !d.Probe {
		t.Fatalf("expected probe: %+v", d)
	}
	if d2, _ := b.Allow(ctx, ep, time.Second); d2.Allowed {
		t.Fatal("only one probe may be in flight")
	}
	// Failed probe re-opens with doubled duration (400ms).
	until, _ = b.Failure(ctx, ep)
	if dur := time.Until(until); dur < 300*time.Millisecond || dur > 450*time.Millisecond {
		t.Fatalf("expected ~400ms re-open, got %s", dur)
	}

	time.Sleep(450 * time.Millisecond)
	if d, _ := b.Allow(ctx, ep, time.Second); !d.Probe {
		t.Fatal("expected second probe")
	}
	if closed, _ := b.Success(ctx, ep); !closed {
		t.Fatal("successful probe should close")
	}
	if d, _ := b.Allow(ctx, ep, time.Second); !d.Allowed || d.Probe {
		t.Fatal("closed after success")
	}
	if fails, ou, _ := b.State(ctx, ep); fails != 0 || !ou.IsZero() {
		t.Fatal("state not reset")
	}
	// Success on a clean endpoint is a no-op.
	if closed, _ := b.Success(ctx, "ep_clean"); closed {
		t.Fatal("clean endpoint reported closed")
	}
}
