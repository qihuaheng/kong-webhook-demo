#!/bin/sh
# Runs once when a node initialises as primary (docker-entrypoint initdb).
set -eu
# Allow streaming replication from the compose network.
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
# Synchronous replication: a commit returns only after the standby has
# flushed it, so a committed (202-acknowledged) event survives the loss of
# the primary (RPO 0). Set via ALTER SYSTEM (not -c) so a promoted node can
# relax it while it has no standby, as Patroni's synchronous_mode does.
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres \
  -c "ALTER SYSTEM SET synchronous_standby_names = '*'" \
  -c "ALTER SYSTEM SET synchronous_commit = 'remote_apply'"
# remote_apply (not just "on" = remote flush): the commit is also replayed
# on the standby before returning. Tests create databases and immediately
# connect through a standby-first DSN; with remote flush only, the standby
# may answer "database does not exist", and pgx stops trying other hosts on
# that error.
