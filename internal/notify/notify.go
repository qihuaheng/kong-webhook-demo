// Package notify sends DLQ notifications to tenants (design §3.5).
//
// Aggregation is per endpoint with a leading + trailing window: the first
// dead delivery in a quiet period is reported immediately; further ones in
// the next window are counted and reported once when it ends. Under a
// sustained failure that is at most one notification per window.
package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"webhookd/internal/dispatch"
	"webhookd/internal/metrics"
	"webhookd/internal/model"
	"webhookd/internal/queue"
	"webhookd/pkg/webhook"
)

const EventType = "webhook.delivery.dead"

// TenantNotifyLookup returns the tenant's notification URL and secret.
type TenantNotifyLookup interface {
	TenantNotify(ctx context.Context, tenantID string) (url, secret string, err error)
}

type Notifier struct {
	q        *queue.Queue
	rdb      redis.UniversalClient
	tenants  TenantNotifyLookup
	send     *dispatch.Sender
	window   time.Duration
	consumer string
	log      *slog.Logger
	// Backoffs between the 3 send attempts.
	Backoff []time.Duration
}

func New(q *queue.Queue, tenants TenantNotifyLookup, send *dispatch.Sender, window time.Duration, consumer string) *Notifier {
	return &Notifier{q: q, rdb: q.Redis(), tenants: tenants, send: send, window: window, consumer: consumer,
		log: slog.Default().With("component", "notifier"), Backoff: []time.Duration{time.Second, 3 * time.Second}}
}

// Notifier state is sharded like the queue: endpoint ep's window hash and
// its shard's due-zset share the hash tag {ntf<shard>}, so the Lua scripts
// stay within one Redis Cluster slot.
func (n *Notifier) tag(ep string) string       { return fmt.Sprintf("{ntf%d}", n.q.Shard(ep)) }
func (n *Notifier) stateKey(ep string) string  { return n.q.Key("notify", n.tag(ep), ep) }
func (n *Notifier) dueKeyFor(ep string) string { return n.q.Key("notify", "due", n.tag(ep)) }
func (n *Notifier) dueKeyShard(shard int) string {
	return n.q.Key("notify", "due", fmt.Sprintf("{ntf%d}", shard))
}

// Payload is the data field of a webhook.delivery.dead notification.
type Payload struct {
	EndpointID       string    `json:"endpoint_id"`
	Count            int       `json:"count"`
	Reason           string    `json:"reason"`
	SampleDeliveryID string    `json:"sample_delivery_id"`
	WindowStart      time.Time `json:"window_start"`
	WindowEnd        time.Time `json:"window_end"`
}

// record returns 1 if the caller should send a leading notification now.
var recordScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('HINCRBY', KEYS[1], 'count', 1)
  redis.call('HSET', KEYS[1], 'reason', ARGV[4], 'sample', ARGV[5])
  return 0
end
local now, win = tonumber(ARGV[1]), tonumber(ARGV[2])
redis.call('HSET', KEYS[1], 'count', 0, 'tenant', ARGV[3], 'start', now, 'reason', ARGV[4], 'sample', ARGV[5])
redis.call('PEXPIRE', KEYS[1], win * 3)
redis.call('ZADD', KEYS[2], now + win, ARGV[6])
return 1
`)

// flushScript closes one due window: returns the state, then either starts a
// new window (count > 0, keeps the rate limit) or deletes the state.
var flushScript = redis.NewScript(`
local s = redis.call('HGETALL', KEYS[1])
if #s == 0 then redis.call('ZREM', KEYS[2], ARGV[3]) return {} end
local h = {}
for i = 1, #s, 2 do h[s[i]] = s[i+1] end
local now, win = tonumber(ARGV[1]), tonumber(ARGV[2])
if tonumber(h['count']) > 0 then
  redis.call('HSET', KEYS[1], 'count', 0, 'start', now)
  redis.call('PEXPIRE', KEYS[1], win * 3)
  redis.call('ZADD', KEYS[2], now + win, ARGV[3])
else
  redis.call('DEL', KEYS[1])
  redis.call('ZREM', KEYS[2], ARGV[3])
end
return s
`)

// Run consumes DLQ notices and flushes windows until ctx is cancelled.
func (n *Notifier) Run(ctx context.Context) error {
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n.FlushDue(ctx, time.Now())
			}
		}
	}()
	pending := true // first drain our own unacked notices from a previous run
	for ctx.Err() == nil {
		ns, err := n.q.ReadDead(ctx, n.consumer, 100, 2*time.Second, pending)
		if err != nil {
			if ctx.Err() == nil {
				n.log.Error("read dlq failed", "err", err)
				time.Sleep(time.Second)
			}
			continue
		}
		if pending && len(ns) == 0 {
			pending = false
			continue
		}
		for _, x := range ns {
			n.Handle(ctx, x)
		}
	}
	return nil
}

// Handle processes one DLQ notice. A returned error means the notice was
// not recorded and not acked; it is re-read on restart.
func (n *Notifier) Handle(ctx context.Context, x queue.DeadNotice) error {
	now := time.Now()
	lead, err := recordScript.Run(ctx, n.rdb, []string{n.stateKey(x.EndpointID), n.dueKeyFor(x.EndpointID)},
		now.UnixMilli(), n.window.Milliseconds(), x.TenantID, x.Reason, x.DeliveryID, x.EndpointID).Int()
	if err != nil {
		n.log.Error("record notice failed", "err", err)
		return err
	}
	if lead == 1 {
		n.deliver(ctx, x.TenantID, Payload{EndpointID: x.EndpointID, Count: 1, Reason: x.Reason,
			SampleDeliveryID: x.DeliveryID, WindowStart: now, WindowEnd: now})
	} else {
		metrics.Notifications.WithLabelValues("suppressed").Inc()
	}
	if err := n.q.AckDead(ctx, x.StreamID); err != nil {
		n.log.Error("ack dlq notice failed", "err", err)
		return err
	}
	return nil
}

// FlushDue sends trailing summaries for windows that ended before now.
// Returns the first error encountered; remaining windows are still processed.
func (n *Notifier) FlushDue(ctx context.Context, now time.Time) error {
	var firstErr error
	fail := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}
	for sh := 0; sh < n.q.Shards(); sh++ {
		eps, err := n.rdb.ZRangeByScore(ctx, n.dueKeyShard(sh), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Count: 500}).Result()
		if err != nil {
			fail(err)
			continue
		}
		n.flushShard(ctx, now, eps, fail)
	}
	return firstErr
}

func (n *Notifier) flushShard(ctx context.Context, now time.Time, eps []string, fail func(error)) {
	for _, ep := range eps {
		raw, err := flushScript.Run(ctx, n.rdb, []string{n.stateKey(ep), n.dueKeyFor(ep)}, now.UnixMilli(), n.window.Milliseconds(), ep).StringSlice()
		if err != nil {
			n.log.Error("flush window failed", "endpoint_id", ep, "err", err)
			fail(err)
			continue
		}
		if len(raw) == 0 {
			continue
		}
		h := map[string]string{}
		for i := 0; i+1 < len(raw); i += 2 {
			h[raw[i]] = raw[i+1]
		}
		count, _ := strconv.Atoi(h["count"])
		if count == 0 {
			continue
		}
		start, _ := strconv.ParseInt(h["start"], 10, 64)
		n.deliver(ctx, h["tenant"], Payload{EndpointID: ep, Count: count, Reason: h["reason"],
			SampleDeliveryID: h["sample"], WindowStart: time.UnixMilli(start), WindowEnd: now})
	}
}

func (n *Notifier) deliver(ctx context.Context, tenantID string, p Payload) {
	url, secret, err := n.tenants.TenantNotify(ctx, tenantID)
	if err != nil {
		n.log.Error("tenant notify lookup failed", "tenant_id", tenantID, "err", err)
		metrics.Notifications.WithLabelValues("failed").Inc()
		return
	}
	if url == "" {
		return // tenant has not configured notifications
	}
	signer, err := webhook.NewHMACSigner(secret)
	if err != nil {
		return
	}
	body, _ := json.Marshal(map[string]any{"type": EventType, "timestamp": time.Now().UTC(), "data": p})
	req := dispatch.Request{URL: url, MsgID: model.NewID("ntf"), Body: body, WireBody: body,
		Signers: []webhook.Signer{signer}, Timeout: 10 * time.Second, MaxRedirects: 3}
	for i := 0; ; i++ {
		res := n.send.Send(ctx, req)
		if res.Verdict == dispatch.Success {
			metrics.Notifications.WithLabelValues("sent").Inc()
			return
		}
		if res.Verdict == dispatch.Fail || i >= len(n.Backoff) {
			// Notifications never go to the DLQ (avoids recursion).
			n.log.Warn("dlq notification failed", "tenant_id", tenantID, "endpoint_id", p.EndpointID, "err", res.Err)
			metrics.Notifications.WithLabelValues("failed").Inc()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(n.Backoff[i]):
		}
	}
}
