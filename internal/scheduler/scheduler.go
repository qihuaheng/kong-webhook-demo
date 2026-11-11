// Package scheduler runs the background loops of the scheduler role
// (design §3.3, §3.4): delayed-retry mover, reconciler, partition
// maintenance, and queue-depth metrics. All loops are safe to run on
// multiple replicas.
package scheduler

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"webhookd/internal/metrics"
	"webhookd/internal/queue"
	"webhookd/internal/store"
)

type Config struct {
	MoveInterval        time.Duration // default 200ms
	MoveBatch           int           // default 500
	ReconcileInterval   time.Duration
	ReconcileStaleAfter time.Duration
	ReconcileLookback   time.Duration // only scan deliveries created within this window
	ReconcileBatch      int
	// ReconcileMaxGate bounds how far a queue backlog can push the cutoff
	// back (default 30m). Beyond it, backlogged deliveries are re-enqueued
	// at most once per gate interval, so a stuck entry cannot disable
	// reconciliation and amplification stays bounded.
	ReconcileMaxGate  time.Duration
	MetadataRetention time.Duration
	PartitionInterval time.Duration // default 1h
	StatsInterval     time.Duration // default 10s
}

type Scheduler struct {
	cfg Config
	st  *store.Store
	q   *queue.Queue
	log *slog.Logger
}

func New(cfg Config, st *store.Store, q *queue.Queue) *Scheduler {
	if cfg.MoveInterval == 0 {
		cfg.MoveInterval = 200 * time.Millisecond
	}
	if cfg.MoveBatch == 0 {
		cfg.MoveBatch = 500
	}
	if cfg.ReconcileBatch == 0 {
		cfg.ReconcileBatch = 5000
	}
	if cfg.ReconcileMaxGate == 0 {
		cfg.ReconcileMaxGate = 30 * time.Minute
	}
	if cfg.ReconcileLookback == 0 {
		cfg.ReconcileLookback = cfg.MetadataRetention
	}
	if cfg.PartitionInterval == 0 {
		cfg.PartitionInterval = time.Hour
	}
	if cfg.StatsInterval == 0 {
		cfg.StatsInterval = 10 * time.Second
	}
	return &Scheduler{cfg: cfg, st: st, q: q, log: slog.Default().With("component", "scheduler")}
}

func (s *Scheduler) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, loop := range []struct {
		every time.Duration
		fn    func(context.Context)
	}{
		{s.cfg.MoveInterval, s.MoveOnce},
		{s.cfg.ReconcileInterval, s.ReconcileOnce},
		{s.cfg.PartitionInterval, s.PartitionsOnce},
		{s.cfg.StatsInterval, s.StatsOnce},
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			every(ctx, loop.every, loop.fn)
		}()
	}
	wg.Wait()
	return nil
}

func every(ctx context.Context, d time.Duration, fn func(context.Context)) {
	fn(ctx)
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn(ctx)
		}
	}
}

// MoveOnce moves all due retries to the ready streams.
func (s *Scheduler) MoveOnce(ctx context.Context) {
	now := time.Now()
	for sh := 0; sh < s.q.Shards(); sh++ {
		for ctx.Err() == nil {
			n, err := s.q.MoveDue(ctx, sh, now, s.cfg.MoveBatch)
			if err != nil {
				if ctx.Err() == nil {
					s.log.Error("move due failed", "shard", sh, "err", err)
				}
				break
			}
			if n < s.cfg.MoveBatch {
				break
			}
		}
	}
}

// clockMargin absorbs skew between Redis stream IDs (Redis clock) and
// next_attempt_at (application/PostgreSQL clock).
const clockMargin = 30 * time.Second

// ReconcileCutoff returns the next_attempt_at bound below which a pending
// delivery is presumed to have lost its queue message.
//
// Streams are FIFO, so a delivery scheduled before the oldest work still
// outstanding in Redis (oldest pending entry, oldest undelivered entry, or
// oldest overdue retry) cannot still be waiting in the queue. Without this
// gate a backlog older than staleAfter would be re-enqueued wholesale on
// every pass (design §17.4 P0-3). Returns gated=true when a backlog moved the
// cutoff earlier than now-staleAfter.
func (s *Scheduler) ReconcileCutoff(ctx context.Context, now time.Time) (cutoff time.Time, gated bool, err error) {
	cutoff = now.Add(-s.cfg.ReconcileStaleAfter)
	floor := now.Add(-s.cfg.ReconcileMaxGate)
	for sh := 0; sh < s.q.Shards(); sh++ {
		oldest, ok, err := s.q.OldestOutstanding(ctx, sh, now)
		if err != nil {
			return time.Time{}, false, err // can't see the queue: skip this pass
		}
		if !ok {
			continue
		}
		if c := oldest.Add(-clockMargin); c.Before(cutoff) {
			cutoff, gated = c, true
		}
	}
	if cutoff.Before(floor) {
		cutoff = floor
	}
	return cutoff, gated, nil
}

// ReconcileOnce re-enqueues deliveries whose queue message was lost.
func (s *Scheduler) ReconcileOnce(ctx context.Context) {
	cutoff, gated, err := s.ReconcileCutoff(ctx, time.Now())
	if err != nil {
		if ctx.Err() == nil {
			s.log.Error("reconcile skipped: queue state unavailable", "err", err)
		}
		return
	}
	if gated {
		metrics.ReconcileGated.Inc()
	}
	for ctx.Err() == nil {
		stale, err := s.st.Reconcile(ctx, cutoff, s.cfg.ReconcileLookback, s.cfg.ReconcileBatch)
		if err != nil {
			if ctx.Err() == nil {
				s.log.Error("reconcile failed", "err", err)
			}
			return
		}
		if len(stale) == 0 {
			return
		}
		msgs := make([]queue.Msg, len(stale))
		for i, d := range stale {
			msgs[i] = queue.Msg{DeliveryID: d.DeliveryID, EndpointID: d.EndpointID, EventID: d.EventID, Attempt: d.Attempt}
		}
		if err := s.q.Enqueue(ctx, msgs...); err != nil {
			// next_attempt_at was bumped; they'll be found again after staleAfter.
			s.log.Error("reconcile enqueue failed", "err", err, "count", len(msgs))
			return
		}
		metrics.Reconciled.Add(float64(len(msgs)))
		s.log.Warn("re-enqueued stale deliveries", "count", len(msgs))
		if len(stale) < s.cfg.ReconcileBatch {
			return
		}
	}
}

// PartitionsOnce creates upcoming partitions and drops expired ones.
func (s *Scheduler) PartitionsOnce(ctx context.Context) {
	now := time.Now()
	if err := s.st.EnsurePartitions(ctx, now, 3); err != nil {
		s.log.Error("ensure partitions failed", "err", err)
	}
	dropped, err := s.st.DropExpiredPartitions(ctx, now, s.cfg.MetadataRetention, 2*store.IdempotencyWindow)
	if err != nil {
		s.log.Error("drop partitions failed", "err", err)
	}
	if len(dropped) > 0 {
		s.log.Info("dropped expired partitions", "partitions", dropped)
	}
}

// StatsOnce updates queue-depth gauges.
func (s *Scheduler) StatsOnce(ctx context.Context) {
	for sh := 0; sh < s.q.Shards(); sh++ {
		st, err := s.q.Stats(ctx, sh)
		if err != nil {
			continue
		}
		l := strconv.Itoa(sh)
		metrics.QueueLag.WithLabelValues(l).Set(float64(st.Lag + st.Pending))
		metrics.RetryBacklog.WithLabelValues(l).Set(float64(st.Retry))
	}
}
