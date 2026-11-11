package store

import (
	"context"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"webhookd/internal/model"
)

// IdempotencyWindow is how long an Idempotency-Key deduplicates.
const IdempotencyWindow = 24 * time.Hour

// IngestItem is one accepted event and its fan-out deliveries.
type IngestItem struct {
	Event      model.Event
	Deliveries []model.Delivery
}

// IngestResult reports the outcome for the item at the same index.
// Duplicate items were not inserted; EventID is the original event.
type IngestResult struct {
	EventID   string
	Duplicate bool
}

type idemKey struct{ tenant, key string }

// InsertBatch writes many events in one transaction (group commit, design
// §3.1). Idempotency is resolved inside the same transaction.
func (s *Store) InsertBatch(ctx context.Context, items []IngestItem) ([]IngestResult, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	var res []IngestResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		res = make([]IngestResult, len(items))
		for i := range items {
			res[i].EventID = items[i].Event.ID
		}
		if err := resolveIdempotency(ctx, tx, items, res); err != nil {
			return err
		}
		var evRows, dlRows [][]any
		for i, it := range items {
			if res[i].Duplicate {
				continue
			}
			e := it.Event
			var key *string
			if e.IdempotencyKey != "" {
				key = &e.IdempotencyKey
			}
			evRows = append(evRows, []any{e.ID, e.TenantID, e.EventType, e.PayloadKey, e.PayloadSize,
				e.StoredSize, key, e.TraceParent, e.CreatedAt})
			for _, d := range it.Deliveries {
				dlRows = append(dlRows, []any{d.ID, d.EventID, d.EndpointID, d.TenantID, string(model.StatusPending),
					0, d.CreatedAt, d.CreatedAt, d.CreatedAt})
			}
		}
		if len(evRows) > 0 {
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"events"},
				[]string{"id", "tenant_id", "event_type", "payload_key", "payload_size", "stored_size",
					"idempotency_key", "trace_parent", "created_at"}, pgx.CopyFromRows(evRows)); err != nil {
				return err
			}
		}
		if len(dlRows) > 0 {
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"deliveries"},
				[]string{"id", "event_id", "endpoint_id", "tenant_id", "status", "attempt",
					"next_attempt_at", "created_at", "updated_at"}, pgx.CopyFromRows(dlRows)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// keyArrays holds column arrays for unnest() over a set of keyed items.
type keyArrays struct {
	tenants, keys, eventIDs []string
	days, created           []time.Time
}

func buildArrays(items []IngestItem, idx []int, dayOffset int) keyArrays {
	var a keyArrays
	for _, i := range idx {
		e := items[i].Event
		a.tenants = append(a.tenants, e.TenantID)
		a.keys = append(a.keys, e.IdempotencyKey)
		a.eventIDs = append(a.eventIDs, e.ID)
		a.created = append(a.created, e.CreatedAt)
		a.days = append(a.days, e.CreatedAt.UTC().Truncate(24*time.Hour).AddDate(0, 0, dayOffset))
	}
	return a
}

func resolveIdempotency(ctx context.Context, tx pgx.Tx, items []IngestItem, res []IngestResult) error {
	// Dedupe within the batch first; keep the first occurrence.
	first := map[idemKey]int{}
	var keyed []int
	for i, it := range items {
		if it.Event.IdempotencyKey == "" {
			continue
		}
		k := idemKey{it.Event.TenantID, it.Event.IdempotencyKey}
		if j, ok := first[k]; ok {
			res[i] = IngestResult{EventID: items[j].Event.ID, Duplicate: true}
			continue
		}
		first[k] = i
		keyed = append(keyed, i)
	}
	if len(keyed) == 0 {
		return nil
	}
	// Consistent lock order avoids deadlocks between concurrent batches.
	sort.Slice(keyed, func(a, b int) bool {
		ea, eb := items[keyed[a]].Event, items[keyed[b]].Event
		if ea.TenantID != eb.TenantID {
			return ea.TenantID < eb.TenantID
		}
		return ea.IdempotencyKey < eb.IdempotencyKey
	})
	keyOf := func(i int) idemKey { return idemKey{items[i].Event.TenantID, items[i].Event.IdempotencyKey} }

	// 1. Keys in yesterday's partition that are still inside the 24h window.
	prev := buildArrays(items, keyed, -1)
	found, err := lookupKeys(ctx, tx, prev, time.Now().Add(-IdempotencyWindow))
	if err != nil {
		return err
	}
	var todo []int
	for _, i := range keyed {
		if ev, ok := found[keyOf(i)]; ok {
			res[i] = IngestResult{EventID: ev, Duplicate: true}
		} else {
			todo = append(todo, i)
		}
	}
	if len(todo) == 0 {
		return nil
	}

	// 2. Claim today's keys. Conflicts are duplicates; ON CONFLICT waits for a
	// concurrent inserter to commit or roll back.
	cur := buildArrays(items, todo, 0)
	rows, err := tx.Query(ctx, `
		INSERT INTO idempotency_keys (tenant_id, key, day, event_id, created_at)
		SELECT * FROM unnest($1::text[], $2::text[], $3::date[], $4::text[], $5::timestamptz[])
		ON CONFLICT DO NOTHING RETURNING tenant_id, key`, cur.tenants, cur.keys, cur.days, cur.eventIDs, cur.created)
	if err != nil {
		return err
	}
	inserted := map[idemKey]bool{}
	for rows.Next() {
		var k idemKey
		if err := rows.Scan(&k.tenant, &k.key); err != nil {
			rows.Close()
			return err
		}
		inserted[k] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(inserted) == len(todo) {
		return nil
	}
	existing, err := lookupKeys(ctx, tx, cur, time.Time{})
	if err != nil {
		return err
	}
	for _, i := range todo {
		if k := keyOf(i); !inserted[k] {
			res[i] = IngestResult{EventID: existing[k], Duplicate: true}
		}
	}
	return nil
}

func lookupKeys(ctx context.Context, tx pgx.Tx, a keyArrays, after time.Time) (map[idemKey]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT k.tenant_id, k.key, k.event_id FROM idempotency_keys k
		JOIN unnest($1::text[], $2::text[], $3::date[]) AS u(t, k, d)
		  ON k.tenant_id = u.t AND k.key = u.k AND k.day = u.d
		WHERE k.created_at > $4`, a.tenants, a.keys, a.days, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[idemKey]string{}
	for rows.Next() {
		var k idemKey
		var ev string
		if err := rows.Scan(&k.tenant, &k.key, &ev); err != nil {
			return nil, err
		}
		out[k] = ev
	}
	return out, rows.Err()
}
