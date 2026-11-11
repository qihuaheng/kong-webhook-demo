// Package queue wraps the Redis structures used for scheduling (design §8.2):
// sharded ready streams with a consumer group, delayed-retry sorted sets,
// and a dead-letter notification stream.
package queue

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	GroupWorkers  = "workers"
	GroupNotifier = "notifier"
	dlqMaxLen     = 1_000_000
)

// Msg is one ready-queue entry. Payload is the zstd-compressed envelope when
// inlined (first attempt and small enough), else nil.
type Msg struct {
	StreamID   string
	Shard      int
	DeliveryID string
	EndpointID string
	EventID    string
	Attempt    int
	Payload    []byte
}

type Queue struct {
	rdb    redis.UniversalClient
	shards int
	prefix string
}

// New creates a Queue. prefix namespaces all keys (default "whk").
func New(rdb redis.UniversalClient, shards int, prefix string) *Queue {
	if prefix == "" {
		prefix = "whk"
	}
	return &Queue{rdb: rdb, shards: shards, prefix: prefix}
}

func (q *Queue) Shards() int { return q.shards }

// Keys use a {shard} hash tag so a shard's stream and zset share a cluster slot.
func (q *Queue) readyKey(s int) string { return fmt.Sprintf("%s:ready:{%d}", q.prefix, s) }
func (q *Queue) retryKey(s int) string { return fmt.Sprintf("%s:retry:{%d}", q.prefix, s) }
func (q *Queue) DLQKey() string        { return q.prefix + ":dlq" }
func (q *Queue) Key(parts ...string) string {
	return q.prefix + ":" + strings.Join(parts, ":")
}

// Shard maps an endpoint to a shard.
func (q *Queue) Shard(endpointID string) int {
	h := fnv.New32a()
	h.Write([]byte(endpointID))
	return int(h.Sum32() % uint32(q.shards))
}

// Init creates consumer groups (idempotent).
func (q *Queue) Init(ctx context.Context) error {
	for s := 0; s < q.shards; s++ {
		if err := q.createGroup(ctx, q.readyKey(s), GroupWorkers); err != nil {
			return err
		}
	}
	return q.createGroup(ctx, q.DLQKey(), GroupNotifier)
}

func (q *Queue) createGroup(ctx context.Context, key, group string) error {
	err := q.rdb.XGroupCreateMkStream(ctx, key, group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create group %s on %s: %w", group, key, err)
	}
	return nil
}

func (m *Msg) values() map[string]any {
	v := map[string]any{"d": m.DeliveryID, "e": m.EndpointID, "v": m.EventID, "a": m.Attempt}
	if m.Payload != nil {
		v["p"] = m.Payload
	}
	return v
}

// Enqueue appends messages to their shards' ready streams in one pipeline.
func (q *Queue) Enqueue(ctx context.Context, msgs ...Msg) error {
	if len(msgs) == 0 {
		return nil
	}
	_, err := q.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i := range msgs {
			p.XAdd(ctx, &redis.XAddArgs{Stream: q.readyKey(q.Shard(msgs[i].EndpointID)), Values: msgs[i].values()})
		}
		return nil
	})
	return err
}

// Read blocks up to block for new messages on one shard.
func (q *Queue) Read(ctx context.Context, shard int, consumer string, count int, block time.Duration) ([]Msg, error) {
	res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: GroupWorkers, Consumer: consumer, Streams: []string{q.readyKey(shard), ">"},
		Count: int64(count), Block: block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Msg
	for _, st := range res {
		out = append(out, parse(shard, st.Messages)...)
	}
	return out, nil
}

// Reclaim takes over messages idle longer than minIdle in a shard's pending
// list (crashed or stuck consumers). start is the cursor; pass "0-0" first.
func (q *Queue) Reclaim(ctx context.Context, shard int, consumer string, minIdle time.Duration, start string, count int) ([]Msg, string, error) {
	msgs, next, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: q.readyKey(shard), Group: GroupWorkers, Consumer: consumer,
		MinIdle: minIdle, Start: start, Count: int64(count),
	}).Result()
	if err != nil {
		return nil, "0-0", err
	}
	return parse(shard, msgs), next, nil
}

func parse(shard int, xs []redis.XMessage) []Msg {
	out := make([]Msg, 0, len(xs))
	for _, x := range xs {
		m := Msg{StreamID: x.ID, Shard: shard}
		m.DeliveryID, _ = x.Values["d"].(string)
		m.EndpointID, _ = x.Values["e"].(string)
		m.EventID, _ = x.Values["v"].(string)
		if a, ok := x.Values["a"].(string); ok {
			m.Attempt, _ = strconv.Atoi(a)
		}
		if p, ok := x.Values["p"].(string); ok {
			m.Payload = []byte(p)
		}
		out = append(out, m)
	}
	return out
}

// Ack acknowledges and deletes processed entries so stream memory tracks
// backlog rather than history.
func (q *Queue) Ack(ctx context.Context, msgs ...Msg) error {
	if len(msgs) == 0 {
		return nil
	}
	_, err := q.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, m := range msgs {
			k := q.readyKey(m.Shard)
			p.XAck(ctx, k, GroupWorkers, m.StreamID)
			p.XDel(ctx, k, m.StreamID)
		}
		return nil
	})
	return err
}

func retryMember(m Msg) string {
	return m.DeliveryID + "|" + m.EndpointID + "|" + m.EventID + "|" + strconv.Itoa(m.Attempt)
}

// Schedule puts a delivery into the delayed-retry set. Payloads are never
// stored here. The member encodes the attempt, so re-scheduling the same
// attempt (e.g. repeated breaker deferral) just moves its due time.
func (q *Queue) Schedule(ctx context.Context, m Msg, at time.Time) error {
	return q.rdb.ZAdd(ctx, q.retryKey(q.Shard(m.EndpointID)), redis.Z{Score: float64(at.UnixMilli()), Member: retryMember(m)}).Err()
}

// moveDue atomically moves due members of a retry zset onto the ready stream.
var moveDue = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for _, m in ipairs(due) do
  local d, e, v, a = string.match(m, '^([^|]*)|([^|]*)|([^|]*)|([^|]*)$')
  if d then
    redis.call('XADD', KEYS[2], '*', 'd', d, 'e', e, 'v', v, 'a', a)
  end
  redis.call('ZREM', KEYS[1], m)
end
return #due
`)

// MoveDue moves up to limit due retries of one shard to its ready stream.
func (q *Queue) MoveDue(ctx context.Context, shard int, now time.Time, limit int) (int, error) {
	n, err := moveDue.Run(ctx, q.rdb, []string{q.retryKey(shard), q.readyKey(shard)}, now.UnixMilli(), limit).Int()
	return n, err
}

// ShardStats reports backlog for metrics.
type ShardStats struct {
	Lag, Pending, Retry int64
}

func (q *Queue) Stats(ctx context.Context, shard int) (ShardStats, error) {
	var st ShardStats
	groups, err := q.rdb.XInfoGroups(ctx, q.readyKey(shard)).Result()
	if err != nil {
		return st, err
	}
	for _, g := range groups {
		if g.Name == GroupWorkers {
			st.Lag, st.Pending = g.Lag, g.Pending
		}
	}
	st.Retry, err = q.rdb.ZCard(ctx, q.retryKey(shard)).Result()
	return st, err
}

// OldestOutstanding returns the enqueue time of the oldest work still queued
// for a shard: the oldest pending (delivered, unacked) entry, the oldest
// entry not yet delivered to the group, and the oldest overdue retry. ok is
// false when the shard has nothing outstanding. The reconciler uses this to
// avoid re-enqueuing deliveries that are merely waiting in a backlog.
func (q *Queue) OldestOutstanding(ctx context.Context, shard int, now time.Time) (oldest time.Time, ok bool, err error) {
	key := q.readyKey(shard)
	consider := func(t time.Time) {
		if !ok || t.Before(oldest) {
			oldest, ok = t, true
		}
	}
	pend, err := q.rdb.XPending(ctx, key, GroupWorkers).Result()
	if err != nil {
		return time.Time{}, false, err
	}
	if pend.Count > 0 {
		if t, good := streamIDTime(pend.Lower); good {
			consider(t)
		}
	}
	groups, err := q.rdb.XInfoGroups(ctx, key).Result()
	if err != nil {
		return time.Time{}, false, err
	}
	for _, g := range groups {
		if g.Name != GroupWorkers {
			continue
		}
		next, err := q.rdb.XRangeN(ctx, key, "("+g.LastDeliveredID, "+", 1).Result()
		if err != nil {
			return time.Time{}, false, err
		}
		if len(next) > 0 {
			if t, good := streamIDTime(next[0].ID); good {
				consider(t)
			}
		}
	}
	due, err := q.rdb.ZRangeByScoreWithScores(ctx, q.retryKey(shard), &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Count: 1}).Result()
	if err != nil {
		return time.Time{}, false, err
	}
	if len(due) > 0 {
		consider(time.UnixMilli(int64(due[0].Score)))
	}
	return oldest, ok, nil
}

// streamIDTime extracts the millisecond timestamp of a stream ID "ms-seq".
func streamIDTime(id string) (time.Time, bool) {
	ms, _, found := strings.Cut(id, "-")
	if !found {
		return time.Time{}, false
	}
	v, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(v), true
}

// DeadNotice is published when a delivery reaches the DLQ.
type DeadNotice struct {
	StreamID   string
	TenantID   string
	EndpointID string
	DeliveryID string
	Reason     string
}

func (q *Queue) PublishDead(ctx context.Context, n DeadNotice) error {
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: q.DLQKey(), MaxLen: dlqMaxLen, Approx: true,
		Values: map[string]any{"t": n.TenantID, "e": n.EndpointID, "d": n.DeliveryID, "r": n.Reason},
	}).Err()
}

// ReadDead reads DLQ notices for the notifier group. Pending (unacked)
// entries for this consumer are returned first so restarts don't lose them.
func (q *Queue) ReadDead(ctx context.Context, consumer string, count int, block time.Duration, pending bool) ([]DeadNotice, error) {
	id := ">"
	if pending {
		id = "0"
	}
	res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: GroupNotifier, Consumer: consumer, Streams: []string{q.DLQKey(), id}, Count: int64(count), Block: block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []DeadNotice
	for _, st := range res {
		for _, x := range st.Messages {
			n := DeadNotice{StreamID: x.ID}
			n.TenantID, _ = x.Values["t"].(string)
			n.EndpointID, _ = x.Values["e"].(string)
			n.DeliveryID, _ = x.Values["d"].(string)
			n.Reason, _ = x.Values["r"].(string)
			out = append(out, n)
		}
	}
	return out, nil
}

func (q *Queue) AckDead(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := q.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.XAck(ctx, q.DLQKey(), GroupNotifier, ids...)
		p.XDel(ctx, q.DLQKey(), ids...)
		return nil
	})
	return err
}

// Redis exposes the client for components sharing the connection (breaker, notifier).
func (q *Queue) Redis() redis.UniversalClient { return q.rdb }
