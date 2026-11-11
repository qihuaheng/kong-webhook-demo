package store

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQL HA (one primary, N streaming standbys; e.g. Patroni, RDS/Aurora,
// Cloud SQL HA). The DSN lists every node:
//
//	postgres://user:pass@pg-0:5432,pg-1:5432,pg-2:5432/webhookd?sslmode=verify-full
//
// The store always connects to the writable node, finds the new primary
// after a failover, and does not hang on a dead or partitioned node.

const (
	defaultConnectTimeout = 5 * time.Second
	// opTimeout bounds every store call so a frozen or partitioned primary
	// (process alive, no replies) cannot hang workers; the context deadline
	// makes pgx close the connection.
	opTimeout  = 15 * time.Second
	ddlTimeout = 2 * time.Minute
	// tcpUserTimeout drops connections whose sent data stays unacknowledged
	// (black-holed node) instead of waiting ~15 min for kernel retransmits.
	tcpUserTimeout = 20 * time.Second
	resetMinGap    = time.Second
)

var tsaRe = regexp.MustCompile(`target_session_attrs\s*=\s*([A-Za-z-]+)`)

// haConfig applies the HA connection policy to a parsed pool config.
func haConfig(cfg *pgxpool.Config, dsn string) error {
	tsa := os.Getenv("PGTARGETSESSIONATTRS")
	if m := tsaRe.FindStringSubmatch(dsn); m != nil {
		tsa = m[1]
	}
	switch strings.ToLower(tsa) {
	case "":
		// libpq's default is "any", which may pick a standby when it is
		// listed first (always true for some node after a failover).
		cfg.ConnConfig.ValidateConnect = pgconn.ValidateConnectTargetSessionAttrsReadWrite
	case "read-write", "primary":
		// already enforced by pgx
	default:
		return fmt.Errorf("postgres DSN: target_session_attrs=%s would allow a read-only node; use read-write or omit it", tsa)
	}
	if !strings.Contains(dsn, "connect_timeout") && os.Getenv("PGCONNECT_TIMEOUT") == "" {
		cfg.ConnConfig.ConnectTimeout = defaultConnectTimeout // per host attempt
	}
	d := &net.Dialer{
		KeepAliveConfig: net.KeepAliveConfig{Enable: true, Idle: 10 * time.Second, Interval: 5 * time.Second, Count: 3},
		Control:         setTCPUserTimeout(tcpUserTimeout),
	}
	cfg.ConnConfig.DialFunc = d.DialContext
	// Recycle connections so they rebalance after topology changes.
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnLifetimeJitter = 5 * time.Minute
	return nil
}

// failoverTracer resets the pool when a statement reports that its session
// is read-only. That happens when a primary is demoted without a restart:
// pooled sessions stay connected to it, and pgx would otherwise keep reusing
// them. After the reset, new connections are validated and go to the
// writable node.
type failoverTracer struct {
	pool      atomic.Pointer[pgxpool.Pool]
	lastReset atomic.Int64
	onReset   func()
}

func (t *failoverTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (t *failoverTracer) TraceQueryEnd(_ context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	t.check(d.Err)
}

func (t *failoverTracer) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromStartData) context.Context {
	return ctx
}

func (t *failoverTracer) TraceCopyFromEnd(_ context.Context, _ *pgx.Conn, d pgx.TraceCopyFromEndData) {
	t.check(d.Err)
}

func (t *failoverTracer) check(err error) {
	if !IsReadOnlyErr(err) {
		return
	}
	now := time.Now().UnixNano()
	last := t.lastReset.Load()
	if now-last < int64(resetMinGap) || !t.lastReset.CompareAndSwap(last, now) {
		return
	}
	if p := t.pool.Load(); p != nil {
		go p.Reset() // idle conns close now, in-use ones when released
		if t.onReset != nil {
			t.onReset()
		}
	}
}

// IsReadOnlyErr reports SQLSTATE 25006 (read_only_sql_transaction).
func IsReadOnlyErr(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "25006"
}

func (s *Store) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, opTimeout)
}

func (s *Store) boundDDL(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, ddlTimeout)
}
