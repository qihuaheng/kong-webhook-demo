// Package metrics defines the Prometheus metrics listed in design §11.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_events_ingested_total", Help: "Events accepted by the API.",
	}, []string{"tenant"}) // no event type: it is producer-controlled and unbounded

	IngestPayloadBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "webhook_ingest_payload_bytes", Help: "Uncompressed payload size at ingest.",
		Buckets: prometheus.ExponentialBuckets(256, 4, 7), // 256B .. 1MB
	})

	IngestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "webhook_ingest_duration_seconds", Help: "Ingest latency by stage.",
		Buckets: prometheus.DefBuckets,
	}, []string{"stage"}) // s3_put | pg_commit | enqueue | total

	DeliveryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_delivery_attempts_total", Help: "HTTP delivery attempts.",
	}, []string{"result", "status_class"}) // result: success|retry|dead

	DeliveryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "webhook_delivery_duration_seconds", Help: "Duration of one delivery attempt.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	})

	DeliveryE2ELatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "webhook_delivery_e2e_latency_seconds", Help: "Event creation to successful delivery.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 1800, 3600, 21600},
	})

	DeliveriesDead = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_deliveries_dead_total", Help: "Deliveries moved to the DLQ.",
	}, []string{"reason"})

	Deferred = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_deliveries_deferred_total", Help: "Deliveries deferred without consuming an attempt.",
	}, []string{"reason"}) // breaker | concurrency

	QueueLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "webhook_queue_lag", Help: "Ready-stream entries not yet delivered to the group plus pending entries.",
	}, []string{"shard"})

	RetryBacklog = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "webhook_retry_backlog", Help: "Deliveries waiting in the delayed-retry set.",
	}, []string{"shard"})

	PayloadCache = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_payload_cache_total", Help: "Payload cache lookups.",
	}, []string{"result"}) // hit | miss | inline

	PayloadFetchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "webhook_payload_fetch_duration_seconds", Help: "Payload fetch latency (cache miss path).",
		Buckets: prometheus.DefBuckets,
	})

	ObjectStoreErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_objectstore_errors_total", Help: "Object storage errors.",
	}, []string{"op"})

	// No endpoint label: the pod that opens a breaker is usually not the one
	// that closes it, so a per-endpoint gauge would leak stale series. The
	// endpoint ID is in the "circuit opened/closed" log lines.
	CircuitTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_circuit_transitions_total", Help: "Circuit breaker transitions.",
	}, []string{"to"}) // open | closed

	Reconciled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_reconciled_total", Help: "Stale deliveries re-enqueued by the reconciler.",
	})

	ReconcileGated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_reconcile_gated_total", Help: "Reconciler passes whose cutoff was moved back by a queue backlog.",
	})

	Notifications = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_dlq_notifications_total", Help: "DLQ notifications sent to tenants.",
	}, []string{"result"}) // sent | failed | suppressed

	InFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "webhook_worker_inflight", Help: "Deliveries currently in progress on this pod.",
	})
)

// StatusClass maps an HTTP status (0 = no response) to a low-cardinality label.
func StatusClass(code int) string {
	switch {
	case code == 0:
		return "none"
	case code < 200:
		return "1xx"
	case code < 300:
		return "2xx"
	case code < 400:
		return "3xx"
	case code < 500:
		return "4xx"
	default:
		return "5xx"
	}
}
