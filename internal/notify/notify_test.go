package notify_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"testing"
	"time"

	"webhookd/internal/dispatch"
	"webhookd/internal/notify"
	"webhookd/internal/queue"
	"webhookd/internal/ssrf"
	"webhookd/internal/testenv"
	"webhookd/pkg/webhook"
)

type fakeTenants struct{ url, secret string }

func (f fakeTenants) TenantNotify(context.Context, string) (string, string, error) {
	return f.url, f.secret, nil
}

func TestLeadingAndTrailingAggregation(t *testing.T) {
	env := testenv.New(t)
	ctx := context.Background()
	q := queue.New(env.Redis, 1, env.Prefix)
	q.Init(ctx)

	secret := webhook.NewSecret()
	var mu sync.Mutex
	var got []notify.Payload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := webhook.VerifyRequest(secret, r, time.Minute)
		if err != nil {
			w.WriteHeader(401)
			return
		}
		var env struct {
			Type string         `json:"type"`
			Data notify.Payload `json:"data"`
		}
		json.Unmarshal(body, &env)
		if env.Type != notify.EventType {
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		got = append(got, env.Data)
		mu.Unlock()
	}))
	defer srv.Close()

	guard := &ssrf.Guard{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}}
	n := notify.New(q, fakeTenants{srv.URL, secret}, dispatch.NewSender(guard, true, 4), 300*time.Millisecond, "n1")
	notice := func(d string) queue.DeadNotice {
		return queue.DeadNotice{TenantID: "ten", EndpointID: "ep_1", DeliveryID: d, Reason: "exhausted"}
	}
	for _, d := range []string{"d1", "d2", "d3", "d4"} {
		n.Handle(ctx, notice(d))
	}
	mu.Lock()
	if len(got) != 1 || got[0].Count != 1 || got[0].SampleDeliveryID != "d1" {
		t.Fatalf("leading notification wrong: %+v", got)
	}
	mu.Unlock()

	time.Sleep(350 * time.Millisecond)
	n.FlushDue(ctx, time.Now())
	mu.Lock()
	if len(got) != 2 || got[1].Count != 3 || got[1].SampleDeliveryID != "d4" {
		t.Fatalf("trailing summary wrong: %+v", got)
	}
	mu.Unlock()

	// A new window was started by the trailing flush; a dead in it is counted.
	n.Handle(ctx, notice("d5"))
	time.Sleep(350 * time.Millisecond)
	n.FlushDue(ctx, time.Now())
	// Then an empty window closes the state, so the next one is leading again.
	time.Sleep(350 * time.Millisecond)
	n.FlushDue(ctx, time.Now())
	n.Handle(ctx, notice("d6"))
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 4 || got[2].Count != 1 || got[3].SampleDeliveryID != "d6" || got[3].Count != 1 {
		t.Fatalf("subsequent windows wrong: %+v", got)
	}
}
