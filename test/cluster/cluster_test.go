// Package cluster checks that every Redis access path works on Redis
// Cluster: multi-key commands and Lua scripts must keep their keys in one
// hash slot, or the server rejects them with CROSSSLOT.
//
//	docker compose -f deploy/docker-compose.yml --profile test up -d redis-cluster
//	WEBHOOKD_IT=1 go test ./test/cluster/
package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"webhookd/internal/breaker"
	"webhookd/internal/dispatch"
	"webhookd/internal/notify"
	"webhookd/internal/queue"
	"webhookd/internal/ssrf"
	"webhookd/internal/testenv"
	"webhookd/pkg/webhook"
)

func TestQueueOnCluster(t *testing.T) {
	rdb, prefix := testenv.NewRedisCluster(t)
	ctx := context.Background()
	q := queue.New(rdb, 16, prefix)
	if err := q.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// Spread messages over many shards (and therefore slots) in one pipeline.
	var msgs []queue.Msg
	for i := 0; i < 64; i++ {
		ep := "ep_" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		msgs = append(msgs, queue.Msg{DeliveryID: "msg_" + ep, EndpointID: ep, EventID: "evt_1", Payload: []byte{1, 2}})
	}
	if err := q.Enqueue(ctx, msgs...); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got := 0
	var read []queue.Msg
	for s := 0; s < 16; s++ {
		ms, err := q.Read(ctx, s, "c1", 100, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("read shard %d: %v", s, err)
		}
		got += len(ms)
		read = append(read, ms...)
		if _, _, err := q.Reclaim(ctx, s, "c2", time.Hour, "0-0", 10); err != nil {
			t.Fatalf("reclaim shard %d: %v", s, err)
		}
	}
	if got != len(msgs) {
		t.Fatalf("read %d of %d", got, len(msgs))
	}
	if err := q.Ack(ctx, read...); err != nil {
		t.Fatalf("ack: %v", err)
	}
	m := msgs[0]
	m.Attempt = 1
	if err := q.Schedule(ctx, m, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if n, err := q.MoveDue(ctx, q.Shard(m.EndpointID), time.Now(), 10); err != nil || n != 1 {
		t.Fatalf("move due (Lua, 2 keys): n=%d err=%v", n, err)
	}
	if _, err := q.Stats(ctx, q.Shard(m.EndpointID)); err != nil {
		t.Fatalf("stats: %v", err)
	}
	if _, _, err := q.OldestOutstanding(ctx, q.Shard(m.EndpointID), time.Now()); err != nil {
		t.Fatalf("oldest undelivered: %v", err)
	}
	q.PublishDead(ctx, queue.DeadNotice{TenantID: "t", EndpointID: "e", DeliveryID: "d", Reason: "exhausted"})
	ns, err := q.ReadDead(ctx, "n", 10, 10*time.Millisecond, false)
	if err != nil || len(ns) != 1 {
		t.Fatalf("read dead: %v %d", err, len(ns))
	}
	if err := q.AckDead(ctx, ns[0].StreamID); err != nil {
		t.Fatalf("ack dead: %v", err)
	}
}

func TestBreakerOnCluster(t *testing.T) {
	rdb, prefix := testenv.NewRedisCluster(t)
	ctx := context.Background()
	b := breaker.New(rdb, prefix, 2, 100*time.Millisecond, time.Second)
	// Several endpoints so keys land in different slots.
	for _, ep := range []string{"ep_a", "ep_b", "ep_c", "ep_01M74DNPGN0JP8TAS94S5PZEP7"} {
		if _, err := b.Failure(ctx, ep); err != nil {
			t.Fatalf("%s failure (Lua, 2 keys): %v", ep, err)
		}
		until, err := b.Failure(ctx, ep)
		if err != nil || until.IsZero() {
			t.Fatalf("%s should open: %v", ep, err)
		}
		if d, err := b.Allow(ctx, ep, time.Second); err != nil || d.Allowed {
			t.Fatalf("%s allow while open: %+v %v", ep, d, err)
		}
		time.Sleep(120 * time.Millisecond)
		if d, err := b.Allow(ctx, ep, time.Second); err != nil || !d.Probe {
			t.Fatalf("%s probe: %+v %v", ep, d, err)
		}
		if closed, err := b.Success(ctx, ep); err != nil || !closed {
			t.Fatalf("%s success (Lua, 2 keys): %v %v", ep, closed, err)
		}
	}
}

type tenants struct{ url, secret string }

func (f tenants) TenantNotify(context.Context, string) (string, string, error) {
	return f.url, f.secret, nil
}

func TestNotifierOnCluster(t *testing.T) {
	rdb, prefix := testenv.NewRedisCluster(t)
	ctx := context.Background()
	q := queue.New(rdb, 16, prefix)
	q.Init(ctx)
	secret := webhook.NewSecret()
	var got atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := webhook.VerifyRequest(secret, r, time.Minute); err == nil {
			got.Add(1)
		}
	}))
	defer srv.Close()
	guard := &ssrf.Guard{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}}
	n := notify.New(q, tenants{srv.URL, secret}, dispatch.NewSender(guard, true, 4), 200*time.Millisecond, "n")
	eps := []string{"ep_a", "ep_b", "ep_c", "ep_d", "ep_e"}
	for _, ep := range eps {
		for i := 0; i < 3; i++ { // 1 leading + 2 aggregated per endpoint
			q.PublishDead(ctx, queue.DeadNotice{TenantID: "t", EndpointID: ep, DeliveryID: "d", Reason: "exhausted"})
		}
	}
	ns, err := q.ReadDead(ctx, "n", 100, 10*time.Millisecond, false)
	if err != nil || len(ns) != 15 {
		t.Fatalf("read dead: %v %d", err, len(ns))
	}
	for _, x := range ns {
		if err := n.Handle(ctx, x); err != nil {
			t.Fatalf("handle (Lua): %v", err)
		}
	}
	if got.Load() != int32(len(eps)) {
		t.Fatalf("leading notifications: %d", got.Load())
	}
	time.Sleep(250 * time.Millisecond)
	if err := n.FlushDue(ctx, time.Now()); err != nil {
		t.Fatalf("flush (Lua): %v", err)
	}
	if got.Load() != int32(2*len(eps)) {
		t.Fatalf("trailing notifications: %d", got.Load())
	}
}
