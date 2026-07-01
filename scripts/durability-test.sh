#!/usr/bin/env bash
# Step 8 durability — kill a worker mid-run, confirm the run recovers & finishes.
#
#   bash scripts/durability-test.sh
#
# Runs a single dedicated worker with the reaper enabled, publishes one LONG
# run, kills the worker while the run is in flight, starts a fresh worker, and
# verifies the reaper resets + requeues the run and it completes.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== durability test =="
docker compose up -d rabbitmq mongo redis >/dev/null
# Ensure infra healthy.
for _ in $(seq 1 20); do
  h=$(docker inspect --format '{{.State.Health.Status}}' poss-mongo-1 2>/dev/null || echo none)
  [ "$h" = "healthy" ] && break; sleep 1
done

# Long run so we can kill mid-flight.
# avgTurns=15 → run lasts ~15 turns (~15s here): long enough to kill at t=4s
# mid-flight, short enough to finish within the post-recovery wait window.
export MODEL_AVG_TURNS=15 MODEL_MIN_DELAY_MS=500 MODEL_MAX_DELAY_MS=700 \
       TOOL_MIN_DELAY_MS=200 TOOL_MAX_DELAY_MS=400 \
       REAPER_ENABLED=1 REAPER_LEASE_MS=6000

echo "starting victim worker (reaper on, short lease)…"
npm run worker >/tmp/victim.log 2>&1 &
VICTIM=$!
sleep 3

RUN=$(npm run publish:one -- durability-thread 4242 2>&1 | sed -n 's/.*runId=\([^ ]*\).*/\1/p')
echo "published long run: $RUN"

echo "letting it run for 4s, then KILLING the worker mid-run…"
sleep 4
STATUS_BEFORE=$(docker exec poss-mongo-1 mongosh viper --quiet --eval "print(db.runs.findOne({_id:'$RUN'}).status)")
SEQ_BEFORE=$(docker exec poss-mongo-1 mongosh viper --quiet --eval "print(db.runs.findOne({_id:'$RUN'}).lastEventSeq)")
echo "  before kill: status=$STATUS_BEFORE lastEventSeq=$SEQ_BEFORE"
kill -9 "$VICTIM" 2>/dev/null || true
echo "  worker killed (pid $VICTIM)"

echo "starting a fresh worker (also reaper-enabled) to recover…"
npm run worker >/tmp/rescuer.log 2>&1 &
RESCUER=$!

echo "waiting up to 90s for recovery + completion…"
FINAL="?"
for _ in $(seq 1 90); do
  FINAL=$(docker exec poss-mongo-1 mongosh viper --quiet --eval "print(db.runs.findOne({_id:'$RUN'}).status)")
  [ "$FINAL" = "done" ] && break
  sleep 1
done

SEQ_AFTER=$(docker exec poss-mongo-1 mongosh viper --quiet --eval "print(db.runs.findOne({_id:'$RUN'}).lastEventSeq)")
echo "  after recovery: status=$FINAL lastEventSeq=$SEQ_AFTER"
echo "== reaper log lines =="
grep -h reaper /tmp/victim.log /tmp/rescuer.log || echo "(none)"

kill "$RESCUER" 2>/dev/null || true

echo ""
if [ "$FINAL" = "done" ] && [ "$SEQ_AFTER" -ge "$SEQ_BEFORE" ]; then
  echo "DURABILITY: PASS — run crashed at seq=$SEQ_BEFORE, recovered and finished at seq=$SEQ_AFTER"
else
  echo "DURABILITY: FAIL — final status=$FINAL (seq $SEQ_BEFORE → $SEQ_AFTER)"
  exit 1
fi
