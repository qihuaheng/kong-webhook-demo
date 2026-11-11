#!/bin/sh
# Entrypoint for a node of the two-node PostgreSQL HA test cluster
# (docker-compose profile "ha"). TEST ONLY: there is no fencing or
# automatic failover; tests drive promotion explicitly (like Patroni would).
#
# PGDATA is on tmpfs, so every container start begins empty:
#   - if a peer is a primary, clone it (pg_basebackup -R) and run as its
#     streaming standby;
#   - otherwise initialise a fresh primary (docker-entrypoint.sh initdb path).
set -eu
: "${PGDATA:?}" "${PG_PEERS:?}" "${POSTGRES_USER:?}" "${POSTGRES_PASSWORD:?}"
export PGPASSWORD="$POSTGRES_PASSWORD"
self=$(hostname)

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  primary=""
  for p in $PG_PEERS; do
    [ "$p" = "$self" ] && continue
    r=$(psql -h "$p" -U "$POSTGRES_USER" -d postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null || true)
    [ "$r" = "f" ] && primary=$p
  done
  if [ -n "$primary" ]; then
    echo "pg-ha: cloning primary $primary; starting as standby"
    mkdir -p "$PGDATA"
    chown postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"
    gosu postgres pg_basebackup -h "$primary" -U "$POSTGRES_USER" -D "$PGDATA" -R -X stream -c fast
    # Name this standby so the primary's pg_stat_replication shows who it is.
    echo "primary_conninfo = 'host=$primary user=$POSTGRES_USER password=$POSTGRES_PASSWORD application_name=$self'" \
      >> "$PGDATA/postgresql.auto.conf"
  else
    echo "pg-ha: no primary among peers; initialising as primary"
  fi
fi
exec docker-entrypoint.sh "$@"
