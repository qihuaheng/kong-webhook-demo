package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"webhookd/internal/model"
	"webhookd/internal/payload"
	"webhookd/internal/store"
	"webhookd/internal/testenv"
)

func setup(t *testing.T) (*store.Store, *model.Tenant, *model.Endpoint) {
	env := testenv.New(t)
	ctx := context.Background()
	ten, key, err := env.Store.CreateTenant(ctx, "acme", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := env.Store.TenantByAPIKey(ctx, key); err != nil || got.ID != ten.ID {
		t.Fatalf("auth by key: %v", err)
	}
	ep := &model.Endpoint{TenantID: ten.ID, URL: "https://example.com/h", EventTypes: []string{"payment.*"}}
	if _, err := env.Store.CreateEndpoint(ctx, ep); err != nil {
		t.Fatal(err)
	}
	return env.Store, ten, ep
}

func item(ten *model.Tenant, ep *model.Endpoint, idem string) store.IngestItem {
	evID := model.NewID("evt")
	et, _ := model.IDTime(evID)
	dID := model.NewID("msg")
	dt, _ := model.IDTime(dID)
	return store.IngestItem{
		Event: model.Event{ID: evID, TenantID: ten.ID, EventType: "payment.succeeded",
			PayloadKey: payload.Key(ten.ID, evID, et), PayloadSize: 10, StoredSize: 8, IdempotencyKey: idem, CreatedAt: et},
		Deliveries: []model.Delivery{{ID: dID, EventID: evID, EndpointID: ep.ID, TenantID: ten.ID, CreatedAt: dt}},
	}
}

func TestIngestIdempotency(t *testing.T) {
	st, ten, ep := setup(t)
	ctx := context.Background()
	a, b, c := item(ten, ep, "k1"), item(ten, ep, "k1"), item(ten, ep, "")
	res, err := st.InsertBatch(ctx, []store.IngestItem{a, b, c})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Duplicate || !res[1].Duplicate || res[1].EventID != a.Event.ID || res[2].Duplicate {
		t.Fatalf("in-batch dedupe wrong: %+v", res)
	}
	// Across batches.
	d := item(ten, ep, "k1")
	res, err = st.InsertBatch(ctx, []store.IngestItem{d})
	if err != nil || !res[0].Duplicate || res[0].EventID != a.Event.ID {
		t.Fatalf("cross-batch dedupe wrong: %+v %v", res, err)
	}
	ev, ds, err := st.GetEvent(ctx, ten.ID, a.Event.ID)
	if err != nil || ev.ID != a.Event.ID || len(ds) != 1 || ds[0].Status != model.StatusPending {
		t.Fatalf("GetEvent: %v %+v", err, ds)
	}
	if _, _, err := st.GetEvent(ctx, ten.ID, b.Event.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("duplicate event should not exist: %v", err)
	}
}

func TestClaimCompleteLifecycle(t *testing.T) {
	st, ten, ep := setup(t)
	ctx := context.Background()
	it := item(ten, ep, "")
	if _, err := st.InsertBatch(ctx, []store.IngestItem{it}); err != nil {
		t.Fatal(err)
	}
	d := it.Deliveries[0]

	c, err := st.Claim(ctx, d.ID, d.EventID, 0, 30*time.Second)
	if err != nil || c.PayloadKey != it.Event.PayloadKey {
		t.Fatalf("claim: %v", err)
	}
	// Leased: a duplicate message cannot claim concurrently.
	if _, err := st.Claim(ctx, d.ID, d.EventID, 0, 30*time.Second); !errors.Is(err, store.ErrStale) {
		t.Fatalf("second claim should be stale: %v", err)
	}
	code := 503
	next := time.Now().Add(time.Minute)
	err = st.Complete(ctx, store.Outcome{DeliveryID: d.ID, PrevAttempt: 0, Attempted: true, Status: model.StatusRetrying,
		NextAttemptAt: next, StatusCode: &code, Error: "HTTP 503",
		Record: &model.Attempt{DeliveryID: d.ID, Attempt: 1, StartedAt: time.Now(), DurationMS: 5, StatusCode: &code}})
	if err != nil {
		t.Fatal(err)
	}
	// Old attempt number is now stale.
	if _, err := st.Claim(ctx, d.ID, d.EventID, 0, time.Second); !errors.Is(err, store.ErrStale) {
		t.Fatalf("stale attempt claim: %v", err)
	}
	if err := st.Complete(ctx, store.Outcome{DeliveryID: d.ID, PrevAttempt: 0, Status: model.StatusSucceeded}); !errors.Is(err, store.ErrStale) {
		t.Fatalf("stale complete: %v", err)
	}
	if _, err := st.Claim(ctx, d.ID, d.EventID, 1, 30*time.Second); err != nil {
		t.Fatalf("claim attempt 1: %v", err)
	}
	err = st.Complete(ctx, store.Outcome{DeliveryID: d.ID, PrevAttempt: 1, Attempted: true, Status: model.StatusDead,
		DeadReason: model.DeadExhausted, Record: &model.Attempt{DeliveryID: d.ID, Attempt: 2, StartedAt: time.Now()}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := st.GetDelivery(ctx, ten.ID, d.ID)
	if err != nil || got.Status != model.StatusDead || got.Attempt != 2 || got.DeadReason != model.DeadExhausted || got.CompletedAt == nil {
		t.Fatalf("final state: %+v %v", got, err)
	}
	atts, err := st.ListAttempts(ctx, ten.ID, d.ID)
	if err != nil || len(atts) != 2 {
		t.Fatalf("attempts: %d %v", len(atts), err)
	}
	dead, err := st.ListDeliveries(ctx, store.DeliveryFilter{TenantID: ten.ID, Status: model.StatusDead})
	if err != nil || len(dead) != 1 {
		t.Fatalf("list dead: %d %v", len(dead), err)
	}

	// Replay resets to pending/attempt 0 with the same ID.
	rep, err := st.Replay(ctx, store.DeliveryFilter{TenantID: ten.ID}, []string{d.ID})
	if err != nil || len(rep) != 1 || rep[0].DeliveryID != d.ID || rep[0].Attempt != 0 {
		t.Fatalf("replay: %+v %v", rep, err)
	}
	got, _ = st.GetDelivery(ctx, ten.ID, d.ID)
	if got.Status != model.StatusPending || got.Attempt != 0 || got.DeadReason != "" {
		t.Fatalf("after replay: %+v", got)
	}
	// Replaying a non-dead delivery is a no-op.
	if rep, _ := st.Replay(ctx, store.DeliveryFilter{TenantID: ten.ID}, []string{d.ID}); len(rep) != 0 {
		t.Fatal("replay of pending delivery should be a no-op")
	}
}

func TestReconcile(t *testing.T) {
	st, ten, ep := setup(t)
	ctx := context.Background()
	it := item(ten, ep, "")
	st.InsertBatch(ctx, []store.IngestItem{it})
	time.Sleep(50 * time.Millisecond)

	got, err := st.Reconcile(ctx, time.Now().Add(-10*time.Millisecond), 24*time.Hour, 100)
	if err != nil || len(got) != 1 || got[0].DeliveryID != it.Deliveries[0].ID {
		t.Fatalf("reconcile: %+v %v", got, err)
	}
	// next_attempt_at was bumped: not picked again immediately.
	if got, _ := st.Reconcile(ctx, time.Now().Add(-time.Second), 24*time.Hour, 100); len(got) != 0 {
		t.Fatalf("should not re-pick: %+v", got)
	}
	// Leased deliveries are skipped.
	time.Sleep(20 * time.Millisecond)
	st.Claim(ctx, it.Deliveries[0].ID, it.Event.ID, 0, time.Minute)
	if got, _ := st.Reconcile(ctx, time.Now().Add(-10*time.Millisecond), 24*time.Hour, 100); len(got) != 0 {
		t.Fatalf("leased delivery reconciled: %+v", got)
	}
}

func TestEndpointsAndSecrets(t *testing.T) {
	st, ten, ep := setup(t)
	ctx := context.Background()
	dt, err := st.GetDispatchTarget(ctx, ep.ID)
	if err != nil || dt.Endpoint.Secret == "" {
		t.Fatalf("dispatch target: %v", err)
	}
	old := dt.Endpoint.Secret
	newS, err := st.RotateSecret(ctx, ten.ID, ep.ID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dt, _ = st.GetDispatchTarget(ctx, ep.ID)
	if s := dt.Endpoint.ActiveSecrets(time.Now()); len(s) != 2 || s[0] != newS || s[1] != old {
		t.Fatalf("rotation secrets wrong: %v", s)
	}
	dis := model.EndpointDisabled
	if _, err := st.UpdateEndpoint(ctx, ten.ID, ep.ID, store.EndpointPatch{Status: &dis}); err != nil {
		t.Fatal(err)
	}
	if act, _ := st.ListEndpoints(ctx, ten.ID, true); len(act) != 0 {
		t.Fatal("disabled endpoint listed as active")
	}
	if err := st.DeleteEndpoint(ctx, ten.ID, ep.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetEndpoint(ctx, ten.ID, ep.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("deleted endpoint visible: %v", err)
	}
	// Workers still see it, to dead-letter in-flight deliveries.
	if dt, err := st.GetDispatchTarget(ctx, ep.ID); err != nil || dt.Endpoint.Status != model.EndpointDeleted {
		t.Fatalf("worker view: %v", err)
	}
}

func TestPartitions(t *testing.T) {
	st, _, _ := setup(t)
	ctx := context.Background()
	old := time.Now().AddDate(0, 0, -40)
	if err := st.EnsurePartitions(ctx, old, 0); err != nil {
		t.Fatal(err)
	}
	dropped, err := st.DropExpiredPartitions(ctx, time.Now(), 30*24*time.Hour, 2*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for _, tb := range []string{"events", "deliveries", "delivery_attempts", "idempotency_keys"} {
		want[tb+"_p"+old.UTC().Format("20060102")] = true
	}
	for _, d := range dropped {
		delete(want, d)
	}
	if len(want) != 0 {
		t.Fatalf("not dropped: %v (dropped %v)", want, dropped)
	}
	for _, d := range dropped {
		if d == "deliveries_p"+time.Now().UTC().Format("20060102") {
			t.Fatal("dropped today's partition")
		}
	}
}
