package scheduler_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"webhookd/internal/queue"
	"webhookd/internal/scheduler"
	"webhookd/internal/testenv"
)

func TestReconcileCutoff(t *testing.T) {
	env := testenv.New(t)
	ctx := context.Background()
	q := queue.New(env.Redis, 4, env.Prefix)
	q.Init(ctx)
	s := scheduler.New(scheduler.Config{ReconcileStaleAfter: 5 * time.Minute, ReconcileMaxGate: 30 * time.Minute,
		MetadataRetention: 24 * time.Hour}, env.Store, q)
	now := time.Now()

	// Empty queue: plain staleAfter cutoff.
	c, gated, err := s.ReconcileCutoff(ctx, now)
	if err != nil || gated || !c.Equal(now.Add(-5*time.Minute)) {
		t.Fatalf("empty: %s gated=%v err=%v", now.Sub(c), gated, err)
	}

	// Backlog whose oldest entry is 10 min old: cutoff moves before it.
	key := env.Prefix + ":ready:{" + strconv.Itoa(q.Shard("ep_x")) + "}"
	add := func(ms int64) {
		if err := env.Redis.XAdd(ctx, &redis.XAddArgs{Stream: key, ID: strconv.FormatInt(ms, 10) + "-1",
			Values: map[string]any{"d": "msg_x"}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	old := now.Add(-10 * time.Minute).UnixMilli()
	add(old)
	c, gated, _ = s.ReconcileCutoff(ctx, now)
	if !gated || c.After(time.UnixMilli(old)) || now.Sub(c) > 11*time.Minute {
		t.Fatalf("backlog: cutoff %s ago gated=%v", now.Sub(c), gated)
	}

	// A 2h-old stuck entry cannot push the cutoff past the 30 min floor.
	env.Redis.Del(ctx, key)
	q.Init(ctx)
	add(now.Add(-2 * time.Hour).UnixMilli())
	c, _, _ = s.ReconcileCutoff(ctx, now)
	if !c.Equal(now.Add(-30 * time.Minute)) {
		t.Fatalf("floor: cutoff %s ago", now.Sub(c))
	}
}
