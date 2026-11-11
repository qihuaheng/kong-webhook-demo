package integration

// PostgreSQL HA (primary + synchronous streaming standby) tests. They drive
// real failovers of the two-node cluster from the "ha" compose profile, so
// they are opt-in:
//
//	docker compose -f deploy/docker-compose.yml --profile ha up -d pg-a pg-b
//	WEBHOOKD_IT=1 WEBHOOKD_IT_PG_HA=1 go test ./test/integration -run PGHA -v
//
// Each test leaves the cluster healthy (one primary, one streaming
// standby); which node is primary alternates between runs.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"webhookd/internal/model"
	"webhookd/internal/store"
	"webhookd/internal/testenv"
	"webhookd/pkg/sdk"
)

var pgNodes = map[string]string{"pg-a": "127.0.0.1:55433", "pg-b": "127.0.0.1:55434"}

func requireHA(t *testing.T) {
	t.Helper()
	if os.Getenv("WEBHOOKD_IT") != "1" || os.Getenv("WEBHOOKD_IT_PG_HA") != "1" {
		t.Skip("PG HA test: set WEBHOOKD_IT=1 WEBHOOKD_IT_PG_HA=1 and start the compose 'ha' profile")
	}
}

func composeFile() string {
	_, f, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(f), "..", "..", "deploy", "docker-compose.yml")
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose", "-f", composeFile(), "--profile", "ha"}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

// nodeSQL runs statements on one node directly (single-host DSN).
func nodeSQL(ctx context.Context, node string, sql ...string) error {
	c, err := pgx.Connect(ctx, "postgres://webhookd:webhookd@"+pgNodes[node]+"/postgres?sslmode=disable&connect_timeout=2")
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	for _, q := range sql {
		if _, err := c.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %q: %w", node, q, err)
		}
	}
	return nil
}

func isPrimary(ctx context.Context, node string) (primary bool, err error) {
	c, err := pgx.Connect(ctx, "postgres://webhookd:webhookd@"+pgNodes[node]+"/postgres?sslmode=disable&connect_timeout=2")
	if err != nil {
		return false, err
	}
	defer c.Close(ctx)
	var rec bool
	err = c.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&rec)
	return !rec, err
}

// topology waits until exactly one node is primary and the other streams
// from it, and returns (primary, standby).
func topology(t *testing.T) (string, string) {
	t.Helper()
	ctx := context.Background()
	heal(t)
	var primary, standby string
	testenv.Eventually(t, 120*time.Second, "healthy PG HA topology", func() bool {
		a, errA := isPrimary(ctx, "pg-a")
		b, errB := isPrimary(ctx, "pg-b")
		if errA != nil || errB != nil || a == b {
			return false
		}
		primary, standby = "pg-a", "pg-b"
		if b {
			primary, standby = "pg-b", "pg-a"
		}
		c, err := pgx.Connect(ctx, "postgres://webhookd:webhookd@"+pgNodes[primary]+"/postgres?sslmode=disable&connect_timeout=2")
		if err != nil {
			return false
		}
		defer c.Close(ctx)
		var n int
		c.QueryRow(ctx, "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming' AND sync_state = 'sync'").Scan(&n)
		return n == 1
	})
	return primary, standby
}

// heal restarts a missing node as a standby of the surviving primary, so a
// previously aborted run does not leave the cluster degraded.
func heal(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	a, errA := isPrimary(ctx, "pg-a")
	b, errB := isPrimary(ctx, "pg-b")
	switch {
	case errA != nil && errB == nil && b:
		t.Log("pg-a is down; rejoining it as standby of pg-b")
		rejoin(t, "pg-a")
	case errB != nil && errA == nil && a:
		t.Log("pg-b is down; rejoining it as standby of pg-a")
		rejoin(t, "pg-b")
	}
}

// haDSN lists both nodes; first is tried first. No target_session_attrs:
// the store must find the writable node on its own.
func haDSN(first, second string) string {
	return fmt.Sprintf("postgres://webhookd:webhookd@%s,%s/webhookd?sslmode=disable", pgNodes[first], pgNodes[second])
}

// promote makes node the primary. Like Patroni's synchronous_mode, it drops
// the synchronous-standby requirement while the new primary has no standby,
// otherwise every commit would wait forever.
func promote(t *testing.T, node string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := nodeSQL(ctx, node, "SELECT pg_promote(true, 60)",
		"ALTER SYSTEM SET synchronous_standby_names = ''", "SELECT pg_reload_conf()"); err != nil {
		t.Fatalf("promote %s: %v", node, err)
	}
}

// rejoin recreates node (empty tmpfs) so it clones the current primary and
// streams from it, then restores synchronous replication.
func rejoin(t *testing.T, node string) {
	t.Helper()
	compose(t, "rm", "-sf", node)
	compose(t, "up", "-d", node)
	primary, _ := topologyAllowAsync(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := nodeSQL(ctx, primary, "ALTER SYSTEM SET synchronous_standby_names = '*'", "SELECT pg_reload_conf()"); err != nil {
		t.Fatalf("restore sync replication: %v", err)
	}
	topology(t)
}

// topologyAllowAsync is topology() but accepts an async standby (used while
// re-enabling synchronous replication).
func topologyAllowAsync(t *testing.T) (string, string) {
	t.Helper()
	ctx := context.Background()
	var primary, standby string
	testenv.Eventually(t, 120*time.Second, "standby streaming", func() bool {
		a, errA := isPrimary(ctx, "pg-a")
		b, errB := isPrimary(ctx, "pg-b")
		if errA != nil || errB != nil || a == b {
			return false
		}
		primary, standby = "pg-a", "pg-b"
		if b {
			primary, standby = "pg-b", "pg-a"
		}
		c, err := pgx.Connect(ctx, "postgres://webhookd:webhookd@"+pgNodes[primary]+"/postgres?sslmode=disable&connect_timeout=2")
		if err != nil {
			return false
		}
		defer c.Close(ctx)
		var n int
		c.QueryRow(ctx, "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'").Scan(&n)
		return n == 1
	})
	return primary, standby
}

// The DSN lists the standby first (as it will after any failover, since the
// host list is static). The store must still write to the primary.
func TestPGHAStandbyListedFirst(t *testing.T) {
	requireHA(t)
	primary, standby := topology(t)
	t.Setenv("WEBHOOKD_IT_POSTGRES", haDSN(standby, primary))
	env := testenv.New(t) // opens the store and runs Migrate (DDL)
	if _, _, err := env.Store.CreateTenant(context.Background(), "ha", nil); err != nil {
		t.Fatalf("write through standby-first DSN: %v", err)
	}
}

// The old primary stays up but is demoted to read-only without a restart
// (e.g. fenced by an operator or pg_auto_failover). Pooled connections to it
// keep working for reads but every write fails with SQLSTATE 25006. The
// store must abandon those connections and reconnect to the new primary.
func TestPGHADemotedPrimaryWithoutRestart(t *testing.T) {
	requireHA(t)
	oldP, newP := topology(t)
	t.Setenv("WEBHOOKD_IT_POSTGRES", haDSN(oldP, newP))
	env := testenv.New(t)
	ctx := context.Background()
	if _, _, err := env.Store.CreateTenant(ctx, "before", nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rejoin(t, oldP) })

	// Fence the old primary first (so no write can land on it), then promote.
	if err := nodeSQL(ctx, oldP, "ALTER SYSTEM SET default_transaction_read_only = on", "SELECT pg_reload_conf()"); err != nil {
		t.Fatal(err)
	}
	promote(t, newP)

	start := time.Now()
	var firstErr error
	testenv.Eventually(t, 15*time.Second, "writes to reach the new primary", func() bool {
		_, _, err := env.Store.CreateTenant(ctx, "after", nil)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		return err == nil
	})
	if !store.IsReadOnlyErr(firstErr) {
		t.Fatalf("expected the pooled session to hit SQLSTATE 25006 first, got %v", firstErr)
	}
	t.Logf("writes recovered %s after promotion (first error: %v)", time.Since(start).Round(time.Millisecond), firstErr)
}

// Full pipeline across a hard primary crash (SIGKILL) and promotion of the
// synchronous standby: every event the API acknowledged with 202 must be
// delivered.
func TestPGHAFailoverEndToEnd(t *testing.T) {
	requireHA(t)
	oldP, newP := topology(t)
	t.Cleanup(func() { rejoin(t, oldP) }) // runs last: after the harness has shut down
	t.Setenv("WEBHOOKD_IT_POSTGRES", haDSN(oldP, newP))
	h := newHarness(t, opts{})
	rc := newReceiver(t, nil)
	h.endpoint(rc, "/", []string{"*"}, nil)

	producer := sdk.New(h.api.URL, h.client.APIKey)
	producer.MaxRetries = 30 // ~ rides out the failover; idempotency keys prevent duplicates

	var mu sync.Mutex
	var accepted []string
	var lastAccept atomic.Int64
	var maxGap time.Duration
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				res, err := producer.SendEvent(context.Background(), "ha.test", map[string]int{"i": i},
					&sdk.SendOptions{IdempotencyKey: model.NewID("idem")})
				if err != nil {
					continue // never acknowledged: not counted
				}
				now := time.Now().UnixNano()
				mu.Lock()
				accepted = append(accepted, res.DeliveryIDs...)
				if prev := lastAccept.Swap(now); prev != 0 {
					maxGap = max(maxGap, time.Duration(now-prev))
				}
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}

	time.Sleep(2 * time.Second)
	killedAt := time.Now()
	compose(t, "kill", oldP) // hard crash: no shutdown checkpoint, connections reset
	promote(t, newP)
	t.Logf("primary %s killed; %s promoted after %s", oldP, newP, time.Since(killedAt).Round(time.Millisecond))
	time.Sleep(5 * time.Second)
	close(stop)
	wg.Wait()

	mu.Lock()
	ids := append([]string(nil), accepted...)
	gap := maxGap
	mu.Unlock()
	if len(ids) == 0 {
		t.Fatal("nothing was accepted")
	}
	seen := func() map[string]bool {
		m := map[string]bool{}
		for _, r := range rc.requests() {
			m[r.MsgID] = true
		}
		return m
	}
	testenv.Eventually(t, 90*time.Second, "all accepted deliveries to reach the receiver", func() bool {
		s := seen()
		for _, id := range ids {
			if !s[id] {
				return false
			}
		}
		return true
	})
	for _, id := range ids {
		d, err := h.client.GetDelivery(context.Background(), id)
		if err != nil || d.Status != "succeeded" {
			t.Fatalf("delivery %s: %+v %v", id, d, err)
		}
	}
	dups := len(rc.requests()) - len(seen())
	t.Logf("accepted=%d delivered=%d duplicate_requests=%d max_accept_gap=%s", len(ids), len(seen()), dups, gap.Round(time.Millisecond))
}

// The primary freezes (process hung, or network partition) instead of
// dying: TCP connections stay open but nothing answers. Without bounded
// operations every pooled call would hang forever.
func TestPGHAFrozenPrimary(t *testing.T) {
	requireHA(t)
	oldP, newP := topology(t)
	t.Setenv("WEBHOOKD_IT_POSTGRES", haDSN(oldP, newP))
	env := testenv.New(t)
	ctx := context.Background()
	if _, _, err := env.Store.CreateTenant(ctx, "before", nil); err != nil {
		t.Fatal(err)
	}
	// `docker compose (un)pause` is a no-op with the podman provider; use the
	// container directly.
	container := "webhookd-" + oldP + "-1"
	t.Cleanup(func() {
		exec.Command("docker", "unpause", container).Run()
		exec.Command("docker", "kill", container).Run() // never let the frozen old primary resume as a second primary
		rejoin(t, oldP)
	})
	if out, err := exec.Command("docker", "pause", container).CombinedOutput(); err != nil {
		t.Fatalf("pause: %v %s", err, out)
	}
	promote(t, newP)

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		for {
			_, _, err := env.Store.CreateTenant(ctx, "after", nil)
			if err == nil || time.Since(start) > 90*time.Second {
				done <- err
				return
			}
		}
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("writes never recovered: %v", err)
		}
	case <-time.After(95 * time.Second):
		t.Fatal("store call hung on the frozen primary")
	}
	took := time.Since(start)
	t.Logf("writes recovered %s after promotion", took.Round(time.Millisecond))
	if took > 40*time.Second {
		t.Fatalf("recovery took %s; expected about op timeout (15s) + connect timeout (5s)", took)
	}
}
