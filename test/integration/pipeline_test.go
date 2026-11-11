package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"webhookd/internal/testenv"
	"webhookd/pkg/sdk"
	"webhookd/pkg/webhook"
)

func TestDeliverySuccessSignedEnvelope(t *testing.T) {
	h := newHarness(t, opts{})
	rc := newReceiver(t, nil)
	h.endpoint(rc, "/hook", []string{"payment.*"}, nil)

	res := h.send("payment.succeeded", map[string]any{"id": "pay_1", "amount": 100})
	if len(res.DeliveryIDs) != 1 {
		t.Fatalf("deliveries: %+v", res)
	}
	d := h.waitStatus(res.DeliveryIDs[0], "succeeded", 10*time.Second)
	if d.Attempt != 1 || d.CompletedAt == nil {
		t.Fatalf("delivery: %+v", d)
	}
	reqs := rc.requests()
	if len(reqs) != 1 || !reqs[0].VerifyOK || reqs[0].Method != http.MethodPost || reqs[0].MsgID != d.ID {
		t.Fatalf("receiver saw %+v", reqs)
	}
	var env envelope
	if err := json.Unmarshal(reqs[0].Body, &env); err != nil || env.Type != "payment.succeeded" ||
		!strings.Contains(string(env.Data), `"pay_1"`) || env.Timestamp.IsZero() {
		t.Fatalf("envelope: %s %v", reqs[0].Body, err)
	}
	atts, err := h.client.ListAttempts(context.Background(), d.ID)
	if err != nil || len(atts) != 1 || atts[0].StatusCode == nil || *atts[0].StatusCode != 200 {
		t.Fatalf("attempts: %+v %v", atts, err)
	}
}

func TestSubscriptionMatchingAndFanOut(t *testing.T) {
	h := newHarness(t, opts{})
	pay, mon, all := newReceiver(t, nil), newReceiver(t, nil), newReceiver(t, nil)
	h.endpoint(pay, "/", []string{"payment.*"}, nil)
	h.endpoint(mon, "/", []string{"monitor.alert"}, nil)
	h.endpoint(all, "/", []string{"*"}, nil)
	off := newReceiver(t, nil)
	h.endpoint(off, "/", []string{"*"}, nil, func(in *sdk.EndpointInput) { in.Status = ptr("disabled") })

	res := h.send("payment.succeeded", map[string]int{"n": 1})
	if len(res.DeliveryIDs) != 2 {
		t.Fatalf("expected fan-out to 2 endpoints, got %d", len(res.DeliveryIDs))
	}
	for _, id := range res.DeliveryIDs {
		h.waitStatus(id, "succeeded", 10*time.Second)
	}
	if pay.count() != 1 || all.count() != 1 || mon.count() != 0 || off.count() != 0 {
		t.Fatalf("counts pay=%d all=%d mon=%d off=%d", pay.count(), all.count(), mon.count(), off.count())
	}
	if res := h.send("unrelated.type", map[string]int{}); len(res.DeliveryIDs) != 1 { // only "*"
		t.Fatalf("unrelated fan-out: %+v", res)
	}
}

func TestRetryThenSuccess(t *testing.T) {
	h := newHarness(t, opts{})
	rc := newReceiver(t, func(n int, _ *http.Request) (int, http.Header) {
		if n < 2 {
			return 503, nil
		}
		return 200, nil
	})
	h.endpoint(rc, "/", []string{"*"}, fastPolicy(5))
	res := h.send("payment.succeeded", map[string]int{"n": 1})
	d := h.waitStatus(res.DeliveryIDs[0], "succeeded", 15*time.Second)
	if d.Attempt != 3 {
		t.Fatalf("attempt = %d", d.Attempt)
	}
	reqs := rc.requests()
	for i, r := range reqs {
		if r.MsgID != d.ID || !r.VerifyOK {
			t.Fatalf("request %d: webhook-id %s verify %v", i, r.MsgID, r.VerifyOK)
		}
		if i > 0 && r.At.Sub(reqs[i-1].At) < 400*time.Millisecond {
			t.Fatalf("retry %d too soon: %s", i, r.At.Sub(reqs[i-1].At))
		}
	}
	atts, _ := h.client.ListAttempts(context.Background(), d.ID)
	if len(atts) != 3 || *atts[0].StatusCode != 503 || *atts[2].StatusCode != 200 {
		t.Fatalf("attempt history: %+v", atts)
	}
}

func TestTimeoutExhaustsToDLQWithNotification(t *testing.T) {
	h := newHarness(t, opts{})
	slow := newReceiver(t, func(int, *http.Request) (int, http.Header) {
		time.Sleep(1500 * time.Millisecond)
		return 200, nil
	})
	p := fastPolicy(2)
	p.Timeout = "1s"
	ep := h.endpoint(slow, "/", []string{"*"}, p)

	notified := newReceiver(t, nil)
	nsecret, err := h.client.SetNotifyURL(context.Background(), notified.srv.URL+"/dlq")
	if err != nil {
		t.Fatal(err)
	}
	notified.secret = nsecret

	res := h.send("monitor.alert", map[string]string{"sev": "high"})
	d := h.waitStatus(res.DeliveryIDs[0], "dead", 15*time.Second)
	if d.Attempt != 2 || d.DeadReason != "exhausted" || d.LastError != "timeout" {
		t.Fatalf("dead delivery: %+v", d)
	}
	page, err := h.client.ListDLQ(context.Background(), &sdk.ListOptions{EndpointID: ep.ID})
	if err != nil || len(page.Deliveries) != 1 || page.Deliveries[0].ID != d.ID {
		t.Fatalf("dlq: %+v %v", page, err)
	}
	testenv.Eventually(t, 5*time.Second, "dlq notification", func() bool { return notified.count() == 1 })
	n := notified.requests()[0]
	var env struct {
		Type string `json:"type"`
		Data struct {
			EndpointID       string `json:"endpoint_id"`
			Count            int    `json:"count"`
			SampleDeliveryID string `json:"sample_delivery_id"`
		} `json:"data"`
	}
	json.Unmarshal(n.Body, &env)
	if !n.VerifyOK || env.Type != "webhook.delivery.dead" || env.Data.EndpointID != ep.ID || env.Data.SampleDeliveryID != d.ID {
		t.Fatalf("notification: verify=%v %s", n.VerifyOK, n.Body)
	}
}

func TestNonRetryable4xxGoesStraightToDLQ(t *testing.T) {
	h := newHarness(t, opts{})
	rc := newReceiver(t, func(int, *http.Request) (int, http.Header) { return 400, nil })
	h.endpoint(rc, "/", []string{"*"}, fastPolicy(5))
	d := h.waitStatus(h.send("a.b", 1).DeliveryIDs[0], "dead", 10*time.Second)
	if d.Attempt != 1 || d.DeadReason != "non_retryable_status" || rc.count() != 1 {
		t.Fatalf("delivery %+v count %d", d, rc.count())
	}
}

func TestRetryAfterHonored(t *testing.T) {
	h := newHarness(t, opts{})
	rc := newReceiver(t, func(n int, _ *http.Request) (int, http.Header) {
		if n == 0 {
			return 429, http.Header{"Retry-After": {"3"}}
		}
		return 200, nil
	})
	p := fastPolicy(3)
	p.MaxDelay = "10s"
	h.endpoint(rc, "/", []string{"*"}, p)
	h.waitStatus(h.send("a.b", 1).DeliveryIDs[0], "succeeded", 15*time.Second)
	reqs := rc.requests()
	if gap := reqs[1].At.Sub(reqs[0].At); gap < 2900*time.Millisecond {
		t.Fatalf("Retry-After not honored: gap %s", gap)
	}
}

func TestRedirectsFollowedAndLimited(t *testing.T) {
	h := newHarness(t, opts{})
	rc := newReceiver(t, func(_ int, r *http.Request) (int, http.Header) {
		switch r.URL.Path {
		case "/old":
			return http.StatusFound, http.Header{"Location": {"/new"}}
		case "/loop":
			return http.StatusPermanentRedirect, http.Header{"Location": {"/loop"}}
		}
		return 200, nil
	})
	h.endpoint(rc, "/old", []string{"ok.*"}, nil)
	d := h.waitStatus(h.send("ok.x", 1).DeliveryIDs[0], "succeeded", 10*time.Second)
	reqs := rc.requests()
	if len(reqs) != 2 || reqs[1].Path != "/new" || reqs[1].Method != http.MethodPost || !reqs[1].VerifyOK {
		t.Fatalf("redirect: %+v", reqs)
	}
	atts, _ := h.client.ListAttempts(context.Background(), d.ID)
	if atts[0].Redirects != 1 {
		t.Fatalf("redirects recorded: %+v", atts)
	}

	h.endpoint(rc, "/loop", []string{"loop.*"}, nil)
	d = h.waitStatus(h.send("loop.x", 1).DeliveryIDs[0], "dead", 10*time.Second)
	if d.DeadReason != "too_many_redirects" || d.Attempt != 1 {
		t.Fatalf("loop: %+v", d)
	}
}

func TestLargePayloadFromObjectStoreAndZstd(t *testing.T) {
	h := newHarness(t, opts{})
	plain, zst := newReceiver(t, nil), newReceiver(t, nil)
	h.endpoint(plain, "/", []string{"*"}, nil)
	h.endpoint(zst, "/", []string{"*"}, nil, func(in *sdk.EndpointInput) { in.Compression = ptr("zstd") })

	// ~200KB of hex (compresses to ~100KB: above the 64KB inline threshold,
	// so workers must fetch it from object storage).
	raw := make([]byte, 100<<10)
	rand.Read(raw)
	data := map[string]string{"blob": hex.EncodeToString(raw)}
	res := h.send("big.event", data)
	for _, id := range res.DeliveryIDs {
		h.waitStatus(id, "succeeded", 15*time.Second)
	}
	p, z := plain.requests()[0], zst.requests()[0]
	if !p.VerifyOK || !z.VerifyOK || !bytes.Equal(p.Body, z.Body) {
		t.Fatalf("verify plain=%v zstd=%v equal=%v", p.VerifyOK, z.VerifyOK, bytes.Equal(p.Body, z.Body))
	}
	if p.Encoding != "" || z.Encoding != "zstd" {
		t.Fatalf("encodings: %q %q", p.Encoding, z.Encoding)
	}
	var env envelope
	json.Unmarshal(p.Body, &env)
	var got map[string]string
	json.Unmarshal(env.Data, &got)
	if got["blob"] != data["blob"] {
		t.Fatal("payload corrupted")
	}
}

func TestPayloadLimit(t *testing.T) {
	h := newHarness(t, opts{})
	h.endpoint(newReceiver(t, nil), "/", []string{"*"}, nil)
	_, err := h.client.SendEvent(context.Background(), "a.b", strings.Repeat("x", 300<<10), nil)
	if ae, ok := err.(*sdk.APIError); !ok || ae.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %v", err)
	}
}

func TestIdempotencyKey(t *testing.T) {
	h := newHarness(t, opts{})
	rc := newReceiver(t, nil)
	h.endpoint(rc, "/", []string{"*"}, nil)
	ctx := context.Background()
	a, err := h.client.SendEvent(ctx, "a.b", 1, &sdk.SendOptions{IdempotencyKey: "order-42"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := h.client.SendEvent(ctx, "a.b", 1, &sdk.SendOptions{IdempotencyKey: "order-42"})
	if err != nil || !b.Duplicate || b.EventID != a.EventID || len(b.DeliveryIDs) != 1 || b.DeliveryIDs[0] != a.DeliveryIDs[0] {
		t.Fatalf("duplicate: %+v %v", b, err)
	}
	h.waitStatus(a.DeliveryIDs[0], "succeeded", 10*time.Second)
	time.Sleep(300 * time.Millisecond)
	if rc.count() != 1 {
		t.Fatalf("receiver got %d requests", rc.count())
	}
}

// A consumer reads a message and dies without acking; another worker must
// take it over via XAUTOCLAIM.
func TestCrashedConsumerMessageIsReclaimed(t *testing.T) {
	h := newHarness(t, opts{noWorker: true, claimIdle: time.Second})
	rc := newReceiver(t, nil)
	ep := h.endpoint(rc, "/", []string{"*"}, nil)
	res := h.send("a.b", 1)

	msgs, err := h.q.Read(context.Background(), h.q.Shard(ep.ID), "crashed-consumer", 10, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("ghost read: %v %d", err, len(msgs))
	}
	h.start()
	d := h.waitStatus(res.DeliveryIDs[0], "succeeded", 10*time.Second)
	if d.Attempt != 1 || rc.count() != 1 {
		t.Fatalf("reclaimed delivery %+v, requests %d", d, rc.count())
	}
}

// Losing every Redis queue entry must not lose deliveries: the reconciler
// rebuilds them from PostgreSQL.
func TestRedisQueueLossRecoveredByReconciler(t *testing.T) {
	h := newHarness(t, opts{noWorker: true})
	rc := newReceiver(t, nil)
	h.endpoint(rc, "/", []string{"*"}, nil)
	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, h.send("a.b", i).DeliveryIDs...)
	}
	ctx := context.Background()
	keys := testenv.ScanKeys(ctx, h.env.Redis, h.env.Prefix+":ready:*")
	if len(keys) == 0 {
		t.Fatal("no ready streams")
	}
	for _, k := range keys {
		h.env.Redis.Del(ctx, k) // per key: streams live in different cluster slots
	}
	if err := h.q.Init(ctx); err != nil {
		t.Fatal(err)
	}
	h.start()
	for _, id := range ids {
		h.waitStatus(id, "succeeded", 15*time.Second)
	}
	if rc.count() != 5 {
		t.Fatalf("expected 5 deliveries, got %d", rc.count())
	}
}

// A backlog older than staleAfter must not be re-enqueued by the reconciler
// (design §17.4 P0-3): every delivery is still in the stream, just waiting.
func TestReconcilerDoesNotAmplifyBacklog(t *testing.T) {
	h := newHarness(t, opts{noWorker: true}) // reconcile every 300ms, staleAfter 1s
	rc := newReceiver(t, nil)
	h.endpoint(rc, "/", []string{"*"}, nil)
	const n = 20
	var ids []string
	for i := 0; i < n; i++ {
		ids = append(ids, h.send("a.b", i).DeliveryIDs...)
	}
	ctx := context.Background()
	streamLen := func() int64 {
		var total int64
		for _, k := range testenv.ScanKeys(ctx, h.env.Redis, h.env.Prefix+":ready:*") {
			total += h.env.Redis.XLen(ctx, k).Val()
		}
		return total
	}
	time.Sleep(3 * time.Second) // ~10 reconcile passes, backlog now 3x staleAfter
	if got := streamLen(); got != n {
		t.Fatalf("reconciler amplified the backlog: %d stream entries for %d deliveries", got, n)
	}
	h.start()
	for _, id := range ids {
		h.waitStatus(id, "succeeded", 10*time.Second)
	}
	if rc.count() != n {
		t.Fatalf("receiver got %d requests, want %d", rc.count(), n)
	}
}

func TestReplayKeepsWebhookIDAndExpiredPayload410(t *testing.T) {
	h := newHarness(t, opts{})
	rc := newReceiver(t, func(int, *http.Request) (int, http.Header) { return 410, nil })
	h.endpoint(rc, "/", []string{"*"}, nil)
	ctx := context.Background()

	res := h.send("a.b", 1)
	id := res.DeliveryIDs[0]
	h.waitStatus(id, "dead", 10*time.Second)
	rc.setHandler(nil) // receiver fixed
	if err := h.client.Replay(ctx, id); err != nil {
		t.Fatal(err)
	}
	d := h.waitStatus(id, "succeeded", 10*time.Second)
	reqs := rc.requests()
	if len(reqs) != 2 || reqs[0].MsgID != id || reqs[1].MsgID != id || d.Attempt != 1 {
		t.Fatalf("replay: %+v, attempt %d", reqs, d.Attempt)
	}
	// Replaying a non-dead delivery is a conflict.
	if err := h.client.Replay(ctx, id); err == nil || err.(*sdk.APIError).StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %v", err)
	}

	// Payload expiry → 410.
	rc.setHandler(func(int, *http.Request) (int, http.Header) { return 400, nil })
	res = h.send("a.b", 2)
	h.waitStatus(res.DeliveryIDs[0], "dead", 10*time.Second)
	mc, _ := minio.New(h.env.S3Endpoint, &minio.Options{Creds: credentials.NewStaticV4("minioadmin", "minioadmin", "")})
	for obj := range mc.ListObjects(ctx, h.env.S3Bucket, minio.ListObjectsOptions{Recursive: true}) {
		if strings.Contains(obj.Key, res.EventID) {
			mc.RemoveObject(ctx, h.env.S3Bucket, obj.Key, minio.RemoveObjectOptions{})
		}
	}
	if err := h.client.Replay(ctx, res.DeliveryIDs[0]); err == nil || err.(*sdk.APIError).StatusCode != http.StatusGone {
		t.Fatalf("expected 410, got %v", err)
	}
}

func TestBulkReplayAndDelete(t *testing.T) {
	h := newHarness(t, opts{})
	rc := newReceiver(t, func(int, *http.Request) (int, http.Header) { return 404, nil })
	ep := h.endpoint(rc, "/", []string{"*"}, nil)
	ctx := context.Background()
	var ids []string
	for i := 0; i < 4; i++ {
		ids = append(ids, h.send("a.b", i).DeliveryIDs[0])
	}
	for _, id := range ids {
		h.waitStatus(id, "dead", 10*time.Second)
	}
	if err := h.client.DeleteDead(ctx, ids[0]); err != nil {
		t.Fatal(err)
	}
	rc.setHandler(nil)
	n, err := h.client.ReplayBulk(ctx, sdk.ReplayFilter{EndpointID: ep.ID})
	if err != nil || n != 3 {
		t.Fatalf("bulk replay: %d %v", n, err)
	}
	for _, id := range ids[1:] {
		h.waitStatus(id, "succeeded", 10*time.Second)
	}
	if _, err := h.client.GetDelivery(ctx, ids[0]); !sdk.IsNotFound(err) {
		t.Fatalf("deleted delivery still visible: %v", err)
	}
}

func TestCircuitBreakerStopsRequests(t *testing.T) {
	h := newHarness(t, opts{breakerThreshold: 2, breakerOpen: 3 * time.Second})
	var down atomic.Bool
	down.Store(true)
	rc := newReceiver(t, func(int, *http.Request) (int, http.Header) {
		if down.Load() {
			return 500, nil
		}
		return 200, nil
	})
	h.endpoint(rc, "/", []string{"*"}, fastPolicy(20))
	id := h.send("a.b", 1).DeliveryIDs[0]

	testenv.Eventually(t, 10*time.Second, "two failures", func() bool { return rc.count() >= 2 })
	// Breaker is now open for ~3s: no requests during that window.
	n := rc.count()
	time.Sleep(2 * time.Second)
	if rc.count() != n {
		t.Fatalf("requests sent while breaker open: %d -> %d", n, rc.count())
	}
	d, _ := h.client.GetDelivery(context.Background(), id)
	if d.Status != "retrying" || d.Attempt != 2 {
		t.Fatalf("deferral must not consume attempts: %+v", d)
	}
	down.Store(false)
	d = h.waitStatus(id, "succeeded", 15*time.Second)
	if d.Attempt != 3 {
		t.Fatalf("expected probe to be attempt 3, got %d", d.Attempt)
	}
}

func TestDeletedEndpointDeadLettersInFlight(t *testing.T) {
	h := newHarness(t, opts{})
	rc := newReceiver(t, func(int, *http.Request) (int, http.Header) { return 503, nil })
	p := fastPolicy(10)
	p.BaseDelay, p.MaxDelay = "2s", "2s"
	ep := h.endpoint(rc, "/", []string{"*"}, p)
	id := h.send("a.b", 1).DeliveryIDs[0]
	h.waitStatus(id, "retrying", 10*time.Second)
	if err := h.client.DeleteEndpoint(context.Background(), ep.ID); err != nil {
		t.Fatal(err)
	}
	d := h.waitStatus(id, "dead", 10*time.Second)
	if d.DeadReason != "endpoint_disabled" {
		t.Fatalf("reason %s", d.DeadReason)
	}
}

func TestAuthAndTenantIsolation(t *testing.T) {
	h := newHarness(t, opts{})
	rc := newReceiver(t, nil)
	h.endpoint(rc, "/", []string{"*"}, nil)
	id := h.send("a.b", 1).DeliveryIDs[0]
	ctx := context.Background()

	if _, err := sdk.New(h.api.URL, "whk_bogus").ListEndpoints(ctx); err == nil || err.(*sdk.APIError).StatusCode != 401 {
		t.Fatalf("expected 401, got %v", err)
	}
	if _, _, err := sdk.New(h.api.URL, "wrong-admin").CreateTenant(ctx, "x", nil); err == nil || err.(*sdk.APIError).StatusCode != 401 {
		t.Fatalf("expected admin 401, got %v", err)
	}
	_, otherKey, _ := h.admin.CreateTenant(ctx, "other", nil)
	other := sdk.New(h.api.URL, otherKey)
	if _, err := other.GetDelivery(ctx, id); !sdk.IsNotFound(err) {
		t.Fatalf("cross-tenant read: %v", err)
	}
	if eps, _ := other.ListEndpoints(ctx); len(eps) != 0 {
		t.Fatal("cross-tenant endpoints visible")
	}
}

func TestEndpointValidation(t *testing.T) {
	h := newHarness(t, opts{})
	ctx := context.Background()
	bad := []sdk.EndpointInput{
		{URL: ptr("https://169.254.169.254/x"), EventTypes: []string{"*"}},
		{URL: ptr("ftp://example.com"), EventTypes: []string{"*"}},
		{URL: ptr("https://example.com"), EventTypes: []string{"bad pattern"}},
		{URL: ptr("https://example.com"), EventTypes: []string{"*"}, RetryPolicy: &sdk.RetryPolicy{Timeout: "5m"}},
		{URL: ptr("https://example.com"), EventTypes: []string{"*"}, RetryPolicy: &sdk.RetryPolicy{MaxAttempts: ptr(100), MaxDelay: "2h"}},
		{URL: ptr("https://example.com"), EventTypes: []string{"*"}, Compression: ptr("gzip")},
	}
	for i, in := range bad {
		if _, _, err := h.client.CreateEndpoint(ctx, in); err == nil || err.(*sdk.APIError).StatusCode != 400 {
			t.Errorf("case %d: expected 400, got %v", i, err)
		}
	}
	ep, s1, err := h.client.CreateEndpoint(ctx, sdk.EndpointInput{URL: ptr("https://example.com/h"), EventTypes: []string{"*"}})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := h.client.RotateSecret(ctx, ep.ID)
	if err != nil || s2 == s1 || !strings.HasPrefix(s2, "whsec_") {
		t.Fatalf("rotate: %v", err)
	}
	up, err := h.client.UpdateEndpoint(ctx, ep.ID, sdk.EndpointInput{EventTypes: []string{"payment.*"}, Compression: ptr("zstd")})
	if err != nil || up.EventTypes[0] != "payment.*" || up.Compression != "zstd" || up.URL != "https://example.com/h" {
		t.Fatalf("update: %+v %v", up, err)
	}
}

// Secret rotation: deliveries carry both signatures during the grace period.
func TestRotationDualSignature(t *testing.T) {
	h := newHarness(t, opts{})
	var oldOK, newOK atomic.Bool
	var oldSecret, newSecret atomic.Value
	rc := newReceiver(t, nil)
	ep := h.endpoint(rc, "/", []string{"*"}, nil)
	oldSecret.Store(rc.secret)
	s2, err := h.client.RotateSecret(context.Background(), ep.ID)
	if err != nil {
		t.Fatal(err)
	}
	newSecret.Store(s2)
	rc.setHandler(func(_ int, r *http.Request) (int, http.Header) {
		sig := r.Header.Get(webhook.HeaderSignature)
		oldOK.Store(len(strings.Fields(sig)) == 2)
		newOK.Store(true)
		return 200, nil
	})
	time.Sleep(200 * time.Millisecond) // worker endpoint cache TTL
	h.waitStatus(h.send("a.b", 1).DeliveryIDs[0], "succeeded", 10*time.Second)
	if !oldOK.Load() || !newOK.Load() {
		t.Fatal("expected two signatures during grace period")
	}
	// The receiver's verifier ran with the old secret (rc.secret) and passed.
	if r := rc.requests(); !r[len(r)-1].VerifyOK {
		t.Fatal("old secret should still verify during grace period")
	}
}
