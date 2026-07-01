#!/usr/bin/env bash
# Step 8 — run the experiment matrix: {worker replicas} × {concurrent users}.
# Scales the compose worker service, waits for readiness, runs the load harness,
# and collects a metrics folder per cell.
#
#   bash scripts/run-matrix.sh
#
# Env overrides:
#   WORKERS="1 3"          worker replica counts to sweep
#   USERS="10 50 100"      concurrent user counts to sweep
#   RAMP_MS=3000           user ramp window
#   TIMEOUT_MS=180000      per-cell drain timeout
set -euo pipefail
cd "$(dirname "$0")/.."

WORKERS="${WORKERS:-1 3}"
USERS="${USERS:-10 50 100}"
RAMP_MS="${RAMP_MS:-3000}"
TIMEOUT_MS="${TIMEOUT_MS:-180000}"

echo "== matrix: workers=[$WORKERS] users=[$USERS] =="

# Ensure infra is up.
docker compose up -d rabbitmq mongo redis web >/dev/null

wait_workers_ready() {
  local n="$1"
  echo "  scaling worker=$n and waiting for readiness…"
  docker compose up -d --scale worker="$n" >/dev/null
  # Wait until N worker containers report 'up.' in their logs.
  for _ in $(seq 1 30); do
    local ready
    ready=$(docker compose logs worker 2>/dev/null | grep -c "up\." || true)
    # grep counts historical banners; instead count running worker containers.
    local running
    running=$(docker compose ps worker --format '{{.State}}' | grep -c running || true)
    [ "$running" -ge "$n" ] && { sleep 3; return 0; }
    sleep 1
  done
  return 0
}

for w in $WORKERS; do
  wait_workers_ready "$w"
  for u in $USERS; do
    label="w${w}-u${u}"
    echo "== cell $label =="
    npm run loadtest -- --users "$u" --label "$label" --ramp-ms "$RAMP_MS" --timeout-ms "$TIMEOUT_MS" \
      2>&1 | grep -E '\[load\]|index|timeToPickup|duration|Queue|mongoLat|completed|totalRuns' || true
  done
done

echo "== matrix complete. results in loadtest-results/ =="
