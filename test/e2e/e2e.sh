#!/usr/bin/env bash
# End-to-end demo against the docker-compose stack:
#   docker compose -f deploy/docker-compose.yml up -d --build
#   test/e2e/e2e.sh
# Exercises: tenant + endpoints, success, retry-then-success, 4xx → DLQ,
# DLQ notification, replay with stable webhook-id, redirect, zstd.
set -euo pipefail

API=${API:-http://127.0.0.1:8080}
RECV=${RECV:-http://127.0.0.1:8090}
RECV_INTERNAL=${RECV_INTERNAL:-http://10.89.50.100:8090} # address workers use
ADMIN_KEY=${ADMIN_KEY:-dev-admin-key}

pass=0
fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "  ok: $*"; pass=$((pass + 1)); }
need() { command -v "$1" >/dev/null || fail "missing dependency: $1"; }
need curl; need jq

api() { # method path [json]
  local m=$1 p=$2; shift 2
  curl -fsS -X "$m" "$API$p" -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' "$@"
}

wait_status() { # delivery_id status [timeout_s]
  local id=$1 want=$2 t=${3:-30} s
  for _ in $(seq $((t * 4))); do
    s=$(api GET "/v1/deliveries/$id" | jq -r .delivery.status)
    [[ $s == "$want" ]] && return 0
    sleep 0.25
  done
  fail "delivery $id: status $s, want $want"
}

endpoint() { # name query patterns-json [extra-json]
  local name=$1 q=$2 types=$3 extra=${4:-'{}'} out secret
  out=$(api POST /v1/endpoints -d "$(jq -nc --arg u "$RECV_INTERNAL/hook/$name$q" --argjson t "$types" --argjson x "$extra" \
    '{url:$u, event_types:$t} + $x')")
  secret=$(jq -r .secret <<<"$out")
  curl -fsS -X PUT "$RECV/_secrets/$name" --data "$secret" >/dev/null
  jq -r .endpoint.id <<<"$out"
}

send() { # type data-json → first delivery id
  api POST /v1/events -d "$(jq -nc --arg t "$1" --argjson d "$2" '{event_type:$t, data:$d}')" | jq -r '.delivery_ids[0]'
}

echo "== waiting for API"
for _ in $(seq 60); do curl -fsS "$API/readyz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$API/readyz" >/dev/null || fail "API not ready"
curl -fsS -X POST "$RECV/_reset" >/dev/null

echo "== tenant"
KEY=$(curl -fsS -X POST "$API/admin/v1/tenants" -H "Authorization: Bearer $ADMIN_KEY" \
  -d "{\"name\":\"e2e-$(date +%s)\"}" | jq -r .api_key)
[[ $KEY == whk_* ]] && ok "tenant created"

FAST='{"retry_policy":{"max_attempts":4,"base_delay":"1s","multiplier":1,"max_delay":"1s"}}'

echo "== success"
endpoint pay "" '["payment.*"]' >/dev/null
id=$(send payment.succeeded '{"id":"pay_1","amount":4200}')
wait_status "$id" succeeded
[[ $(curl -fsS "$RECV/_stats/pay" | jq .verified) == 1 ]] && ok "signed delivery verified by receiver"

echo "== retry then success (2 x 503)"
endpoint flaky "?flaky=2" '["alert.*"]' "$FAST" >/dev/null
id=$(send alert.fired '{"sev":"critical"}')
wait_status "$id" succeeded
n=$(api GET "/v1/deliveries/$id/attempts" | jq '.attempts | length')
[[ $n == 3 ]] && ok "3 attempts recorded" || fail "attempts=$n"

echo "== 4xx → DLQ + notification + replay"
curl -fsS -X PUT "$RECV/_secrets/notify" --data "$(api PUT /v1/tenant/notify -d "{\"url\":\"$RECV_INTERNAL/hook/notify\"}" | jq -r .secret)" >/dev/null
epid=$(endpoint broken "?status=400" '["order.*"]')
id=$(send order.created '{"order":7}')
wait_status "$id" dead
[[ $(api GET "/v1/dlq?endpoint_id=$epid" | jq -r '.deliveries[0].dead_reason') == non_retryable_status ]] && ok "in DLQ with reason"
for _ in $(seq 40); do [[ $(curl -fsS "$RECV/_stats/notify" | jq .verified) -ge 1 ]] && break; sleep 0.25; done
[[ $(curl -fsS "$RECV/_stats/notify" | jq .verified) -ge 1 ]] && ok "signed DLQ notification received"
api PATCH "/v1/endpoints/$epid" -d "{\"url\":\"$RECV_INTERNAL/hook/broken\"}" >/dev/null
sleep 6 # worker endpoint cache TTL
api POST "/v1/dlq/$id/replay" >/dev/null
wait_status "$id" succeeded
[[ $(curl -fsS "$RECV/_stats/broken" | jq .unique_ids) == 1 ]] && ok "replay reused webhook-id"

echo "== redirect (307) and zstd"
endpoint redir "?redirect=1" '["misc.redirect"]' >/dev/null
wait_status "$(send misc.redirect '{}')" succeeded && ok "redirect followed"
endpoint zst "" '["misc.zstd"]' '{"compression":"zstd"}' >/dev/null
wait_status "$(send misc.zstd "$(jq -nc '{blob: ("x" * 50000)}')")" succeeded
[[ $(curl -fsS "$RECV/_stats/zst" | jq .verified) == 1 ]] && ok "zstd body verified"

echo "== metrics"
curl -fsS "$API/metrics" | grep -q '^webhook_events_ingested_total' && ok "api metrics exposed"

echo "PASS ($pass checks)"
