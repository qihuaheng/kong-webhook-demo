// Package store is the PostgreSQL access layer (design §8.1). PostgreSQL is
// the source of truth for all metadata; payloads live in object storage.
package store

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"webhookd/internal/model"
	"webhookd/internal/secretbox"
)

//go:embed schema.sql
var schemaSQL string

var (
	ErrNotFound = errors.New("store: not found")
	// ErrStale means a guarded update matched no row: the delivery moved on
	// (another worker, a replay, or a duplicate queue message).
	ErrStale = errors.New("store: stale delivery state")
)

// Advisory lock keys for singleton jobs.
const (
	lockMigrate    = 0x77686b01
	lockReconcile  = 0x77686b02
	lockPartitions = 0x77686b03
)

type Store struct {
	pool *pgxpool.Pool
	box  *secretbox.Box
}

func Open(ctx context.Context, dsn string, maxConns int32, box *secretbox.Box) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	if err := haConfig(cfg, dsn); err != nil {
		return nil, err
	}
	tr := &failoverTracer{onReset: func() {
		slog.Warn("postgres: session is read-only (primary demoted?); reconnecting pool to the writable node")
	}}
	cfg.ConnConfig.Tracer = tr
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	tr.pool.Store(pool)
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool, box: box}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	return s.pool.Ping(ctx)
}

// Migrate applies the schema and creates partitions around now. Serialized
// across replicas by an advisory lock.
func (s *Store) Migrate(ctx context.Context) error {
	ctx, cancel := s.boundDDL(ctx)
	defer cancel()
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockMigrate); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, schemaSQL); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
		return ensurePartitions(ctx, tx, time.Now(), 1, 3)
	})
}

// partitioned lists daily-partitioned tables and whether the bound is a date.
var partitioned = []struct {
	table  string
	isDate bool
}{
	{"events", false}, {"deliveries", false}, {"delivery_attempts", false}, {"idempotency_keys", true},
}

// EnsurePartitions creates daily partitions from today-back to today+ahead.
func (s *Store) EnsurePartitions(ctx context.Context, now time.Time, ahead int) error {
	ctx, cancel := s.boundDDL(ctx)
	defer cancel()
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var ok bool
		if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", lockPartitions).Scan(&ok); err != nil || !ok {
			return err
		}
		return ensurePartitions(ctx, tx, now, 0, ahead)
	})
}

func ensurePartitions(ctx context.Context, tx pgx.Tx, now time.Time, back, ahead int) error {
	day := now.UTC().Truncate(24 * time.Hour)
	for i := -back; i <= ahead; i++ {
		from := day.AddDate(0, 0, i)
		to := from.AddDate(0, 0, 1)
		for _, p := range partitioned {
			lo, hi := from.Format(time.RFC3339), to.Format(time.RFC3339)
			if p.isDate {
				lo, hi = from.Format(time.DateOnly), to.Format(time.DateOnly)
			}
			sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_p%s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
				p.table, from.Format("20060102"), p.table, lo, hi)
			if _, err := tx.Exec(ctx, sql); err != nil {
				return fmt.Errorf("create partition %s %s: %w", p.table, lo, err)
			}
		}
	}
	return nil
}

// DropExpiredPartitions drops whole-day partitions older than retention.
// Returns the names of dropped partitions.
func (s *Store) DropExpiredPartitions(ctx context.Context, now time.Time, metadata, idempotency time.Duration) ([]string, error) {
	ctx, cancel := s.boundDDL(ctx)
	defer cancel()
	var dropped []string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var ok bool
		if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", lockPartitions).Scan(&ok); err != nil || !ok {
			return err
		}
		for _, p := range partitioned {
			keep := metadata
			if p.isDate {
				keep = idempotency
			}
			// A partition for day D holds [D, D+1); drop it once D+1 <= now-keep.
			cutoff := now.UTC().Add(-keep).Truncate(24 * time.Hour).Format("20060102")
			rows, err := tx.Query(ctx, `
				SELECT c.relname FROM pg_inherits i
				JOIN pg_class c ON c.oid = i.inhrelid
				JOIN pg_class p ON p.oid = i.inhparent
				WHERE p.relname = $1`, p.table)
			if err != nil {
				return err
			}
			names, err := pgx.CollectRows(rows, pgx.RowTo[string])
			if err != nil {
				return err
			}
			prefix := p.table + "_p"
			for _, n := range names {
				suffix, found := strings.CutPrefix(n, prefix)
				if !found || len(suffix) != 8 || suffix >= cutoff {
					continue
				}
				if _, err := tx.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{n}.Sanitize()); err != nil {
					return err
				}
				dropped = append(dropped, n)
			}
		}
		return nil
	})
	return dropped, err
}

// idTime returns the creation time embedded in an ID or an error.
func idTime(id string) (time.Time, error) {
	t, ok := model.IDTime(id)
	if !ok {
		return time.Time{}, ErrNotFound
	}
	return t, nil
}

func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
