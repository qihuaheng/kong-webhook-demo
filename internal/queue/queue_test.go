package queue_test

import (
	"context"
	"testing"
	"time"

	"webhookd/internal/queue"
	"webhookd/internal/testenv"
)

func TestReadyRetryAckReclaim(t *testing.T) {
	env := testenv.New(t)
	ctx := context.Background()
	q := queue.New(env.Redis, 4, env.Prefix)
	if err := q.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.Init(ctx); err != nil { // idempotent
		t.Fatal(err)
	}
	m := queue.Msg{DeliveryID: "msg_1", EndpointID: "ep_1", EventID: "evt_1", Attempt: 0, Payload: []byte{0, 1, 2, 0xff}}
	if err := q.Enqueue(ctx, m); err != nil {
		t.Fatal(err)
	}
	s := q.Shard("ep_1")
	got, err := q.Read(ctx, s, "c1", 10, 100*time.Millisecond)
	if err != nil || len(got) != 1 {
		t.Fatalf("read: %v %d", err, len(got))
	}
	g := got[0]
	if g.DeliveryID != "msg_1" || g.EventID != "evt_1" || g.Attempt != 0 || string(g.Payload) != string(m.Payload) {
		t.Fatalf("roundtrip mismatch: %+v", g)
	}

	// Unacked → reclaimable by another consumer after idle.
	time.Sleep(30 * time.Millisecond)
	rc, _, err := q.Reclaim(ctx, s, "c2", 20*time.Millisecond, "0-0", 10)
	if err != nil || len(rc) != 1 || rc[0].DeliveryID != "msg_1" {
		t.Fatalf("reclaim: %v %+v", err, rc)
	}
	if err := q.Ack(ctx, rc[0]); err != nil {
		t.Fatal(err)
	}
	st, _ := q.Stats(ctx, s)
	if st.Pending != 0 {
		t.Fatalf("pending after ack: %+v", st)
	}

	// Retry: schedule in the past and future; only past is moved; no payload.
	m.Attempt = 1
	q.Schedule(ctx, m, time.Now().Add(-time.Second))
	q.Schedule(ctx, m, time.Now().Add(-time.Second)) // same member: no duplicate
	future := queue.Msg{DeliveryID: "msg_2", EndpointID: "ep_1", EventID: "evt_2", Attempt: 3}
	q.Schedule(ctx, future, time.Now().Add(time.Hour))
	n, err := q.MoveDue(ctx, s, time.Now(), 100)
	if err != nil || n != 1 {
		t.Fatalf("move due: %d %v", n, err)
	}
	got, _ = q.Read(ctx, s, "c1", 10, 100*time.Millisecond)
	if len(got) != 1 || got[0].Attempt != 1 || got[0].Payload != nil {
		t.Fatalf("moved msg: %+v", got)
	}
	st, _ = q.Stats(ctx, s)
	if st.Retry != 1 {
		t.Fatalf("retry backlog: %+v", st)
	}
}

func TestDeadNotices(t *testing.T) {
	env := testenv.New(t)
	ctx := context.Background()
	q := queue.New(env.Redis, 1, env.Prefix)
	q.Init(ctx)
	q.PublishDead(ctx, queue.DeadNotice{TenantID: "t", EndpointID: "e", DeliveryID: "d", Reason: "exhausted"})
	ns, err := q.ReadDead(ctx, "n1", 10, 100*time.Millisecond, false)
	if err != nil || len(ns) != 1 || ns[0].Reason != "exhausted" {
		t.Fatalf("read dead: %v %+v", err, ns)
	}
	// Unacked notices are re-read via pending=true after restart.
	ns2, _ := q.ReadDead(ctx, "n1", 10, 0, true)
	if len(ns2) != 1 {
		t.Fatalf("pending dead: %+v", ns2)
	}
	q.AckDead(ctx, ns[0].StreamID)
	if ns3, _ := q.ReadDead(ctx, "n1", 10, 0, true); len(ns3) != 0 {
		t.Fatalf("acked notice still pending: %+v", ns3)
	}
}
