-- webhookd schema (design §8.1). Idempotent: safe to run on every startup.
-- High-volume tables are range-partitioned by day (UTC); partitions are
-- created ahead and dropped after retention by the scheduler.

CREATE TABLE IF NOT EXISTS tenants (
    id                text PRIMARY KEY,
    name              text NOT NULL,
    api_key_hash      text NOT NULL UNIQUE,
    notify_url        text,
    notify_secret_enc bytea,
    retry_policy      jsonb,
    created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS endpoints (
    id                     text PRIMARY KEY,
    tenant_id              text NOT NULL REFERENCES tenants (id),
    url                    text NOT NULL,
    description            text NOT NULL DEFAULT '',
    event_types            text[] NOT NULL,
    status                 text NOT NULL CHECK (status IN ('active', 'disabled', 'deleted')),
    sign_alg               text NOT NULL DEFAULT 'hmac-sha256',
    compression            text NOT NULL DEFAULT 'none' CHECK (compression IN ('none', 'zstd')),
    secret_enc             bytea NOT NULL,
    prev_secret_enc        bytea,
    prev_secret_expires_at timestamptz,
    retry_policy           jsonb,
    max_concurrency        int NOT NULL DEFAULT 0, -- 0 = server default
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS endpoints_tenant ON endpoints (tenant_id) WHERE status <> 'deleted';

-- created_at always equals the millisecond timestamp embedded in the ULID id,
-- so point lookups by id can include created_at for partition pruning.
CREATE TABLE IF NOT EXISTS events (
    id              text NOT NULL,
    tenant_id       text NOT NULL,
    event_type      text NOT NULL,
    payload_key     text NOT NULL,
    payload_size    int NOT NULL,
    stored_size     int NOT NULL,
    idempotency_key text,
    trace_parent    text,
    created_at      timestamptz NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE IF NOT EXISTS deliveries (
    id               text NOT NULL,
    event_id         text NOT NULL,
    endpoint_id      text NOT NULL,
    tenant_id        text NOT NULL,
    status           text NOT NULL,
    attempt          int NOT NULL DEFAULT 0,  -- HTTP attempts made so far
    next_attempt_at  timestamptz NOT NULL,
    lease_until      timestamptz,             -- set while a worker is delivering
    last_status_code int,
    last_error       text,
    dead_reason      text,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    completed_at     timestamptz,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries (next_attempt_at) WHERE status IN ('pending', 'retrying');
CREATE INDEX IF NOT EXISTS deliveries_tenant_status ON deliveries (tenant_id, status, id);
CREATE INDEX IF NOT EXISTS deliveries_event ON deliveries (event_id);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    delivery_id   text NOT NULL,
    attempt       int NOT NULL,
    started_at    timestamptz NOT NULL,
    duration_ms   int NOT NULL,
    status_code   int,
    error         text,
    response_body text,
    redirects     int NOT NULL DEFAULT 0
) PARTITION BY RANGE (started_at);
CREATE INDEX IF NOT EXISTS delivery_attempts_delivery ON delivery_attempts (delivery_id, attempt);

-- Idempotency window is 24h; partitioned by day so expiry is a DROP, not a DELETE.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id  text NOT NULL,
    key        text NOT NULL,
    day        date NOT NULL,
    event_id   text NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, key, day)
) PARTITION BY RANGE (day);
