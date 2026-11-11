// Package breaker is a Redis-backed circuit breaker shared by all workers
// (design §9). Only retryable failures (timeouts, connection errors, 408,
// 429, 5xx) count; a 4xx means the endpoint is up.
package breaker

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Breaker struct {
	rdb       redis.UniversalClient
	prefix    string
	threshold int
	baseOpen  time.Duration
	maxOpen   time.Duration
}

func New(rdb redis.UniversalClient, prefix string, threshold int, baseOpen, maxOpen time.Duration) *Breaker {
	if prefix == "" {
		prefix = "whk"
	}
	return &Breaker{rdb: rdb, prefix: prefix, threshold: threshold, baseOpen: baseOpen, maxOpen: maxOpen}
}

// Both keys of an endpoint share the {endpoint_id} hash tag so the Lua
// scripts touch a single Redis Cluster slot.
func (b *Breaker) key(ep string) string      { return b.prefix + ":cb:{" + ep + "}" }
func (b *Breaker) probeKey(ep string) string { return b.prefix + ":cbprobe:{" + ep + "}" }

// Decision is the result of Allow.
type Decision struct {
	Allowed bool
	Probe   bool      // allowed as the single half-open probe
	Until   time.Time // when denied: earliest time to try again
}

// allow returns {state, open_until_ms}: 0 closed, 1 open, 2 half-open probe granted, 3 half-open busy.
var allowScript = redis.NewScript(`
local ou = tonumber(redis.call('HGET', KEYS[1], 'open_until') or '0')
local now = tonumber(ARGV[1])
if ou == 0 then return {0, 0} end
if now < ou then return {1, ou} end
if redis.call('SET', KEYS[2], '1', 'NX', 'PX', tonumber(ARGV[2])) then return {2, ou} end
return {3, ou}
`)

// Allow decides whether a request to endpoint ep may be sent now. probeTTL
// should cover one full attempt so only one probe is in flight.
func (b *Breaker) Allow(ctx context.Context, ep string, probeTTL time.Duration) (Decision, error) {
	now := time.Now()
	r, err := allowScript.Run(ctx, b.rdb, []string{b.key(ep), b.probeKey(ep)}, now.UnixMilli(), probeTTL.Milliseconds()).Int64Slice()
	if err != nil {
		return Decision{Allowed: true}, err // fail open: the breaker is an optimization
	}
	switch r[0] {
	case 0:
		return Decision{Allowed: true}, nil
	case 1:
		return Decision{Until: time.UnixMilli(r[1])}, nil
	case 2:
		return Decision{Allowed: true, Probe: true}, nil
	default:
		return Decision{Until: now.Add(probeTTL)}, nil
	}
}

// failure increments the failure count and opens (or re-opens) the breaker
// when the threshold is reached or a half-open probe fails. Returns the new
// open_until (ms) or 0 if still closed.
var failureScript = redis.NewScript(`
local now, threshold, base, maxo, ttl = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])
local ou = tonumber(redis.call('HGET', KEYS[1], 'open_until') or '0')
local fails = redis.call('HINCRBY', KEYS[1], 'fails', 1)
redis.call('PEXPIRE', KEYS[1], ttl)
local halfopen = ou > 0 and now >= ou
if fails >= threshold or halfopen then
  local opens = redis.call('HINCRBY', KEYS[1], 'opens', 1)
  local d = base * (2 ^ (opens - 1))
  if d > maxo then d = maxo end
  local until_ms = now + d
  redis.call('HSET', KEYS[1], 'open_until', until_ms, 'fails', 0)
  redis.call('DEL', KEYS[2])
  return until_ms
end
return 0
`)

// Failure records a retryable failure. Returns the open-until time if this
// failure opened the breaker.
func (b *Breaker) Failure(ctx context.Context, ep string) (time.Time, error) {
	ttl := 2 * b.maxOpen
	if ttl < time.Hour {
		ttl = time.Hour
	}
	r, err := failureScript.Run(ctx, b.rdb, []string{b.key(ep), b.probeKey(ep)},
		time.Now().UnixMilli(), b.threshold, b.baseOpen.Milliseconds(), b.maxOpen.Milliseconds(), ttl.Milliseconds()).Int64()
	if err != nil || r == 0 {
		return time.Time{}, err
	}
	return time.UnixMilli(r), nil
}

// success resets state only if there is any, keeping the hot path to one
// round trip with no writes. Returns 1 if the breaker was open/half-open.
var successScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local ou = tonumber(redis.call('HGET', KEYS[1], 'open_until') or '0')
redis.call('DEL', KEYS[1], KEYS[2])
if ou > 0 then return 1 end
return 0
`)

// Success records a successful (or non-retryable, endpoint-is-up) response.
// Returns true if this closed an open breaker.
func (b *Breaker) Success(ctx context.Context, ep string) (bool, error) {
	r, err := successScript.Run(ctx, b.rdb, []string{b.key(ep), b.probeKey(ep)}).Int()
	return r == 1, err
}

// State is for observability/tests.
func (b *Breaker) State(ctx context.Context, ep string) (fails int, openUntil time.Time, err error) {
	m, err := b.rdb.HGetAll(ctx, b.key(ep)).Result()
	if err != nil {
		return 0, time.Time{}, err
	}
	fails, _ = strconv.Atoi(m["fails"])
	if ou, _ := strconv.ParseInt(m["open_until"], 10, 64); ou > 0 {
		openUntil = time.UnixMilli(ou)
	}
	return fails, openUntil, nil
}
