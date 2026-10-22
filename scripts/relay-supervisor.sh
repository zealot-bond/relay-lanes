#!/usr/bin/env bash
# Runs one relay generation on a GitHub runner, behind a named Cloudflare Tunnel.
#
#   1. start the relay (src/main.js) and the tunnel connector
#   2. HANDOVER_LEAD_S before the end of RUN_FOR_S, dispatch the next generation
#   3. at RUN_FOR_S, stop both and exit
#
# The successor is queued behind this job by the workflow's concurrency group, so it starts
# only after this one has finished. The tunnel never has two connectors and lanes never split
# between two relays. A generation that dies early dispatches no successor: a broken tunnel
# token would otherwise restart the relay in a loop. The schedule starts the next one within
# ten minutes instead, and the job exits non-zero so the failure is visible.
#
# Everything configurable comes from the environment, so the loop is tested with stub
# commands in test/relay-supervisor.mjs.
set -uo pipefail

: "${CF_TUNNEL_TOKEN:?CF_TUNNEL_TOKEN is not set}"
: "${GH_TOKEN:?GH_TOKEN is not set}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is not set}"

RUN_FOR_S=${RUN_FOR_S:-13800}
HANDOVER_LEAD_S=${HANDOVER_LEAD_S:-180}
POLL_S=${POLL_S:-5}
RELAY_WORKFLOW=${RELAY_WORKFLOW:-relay.yml}
GH_REF=${GH_REF:-main}
GH_API=${GH_API:-https://api.github.com}
RELAY_CMD=${RELAY_CMD:-node src/main.js}
TUNNEL_CMD=${TUNNEL_CMD:-cloudflared tunnel --no-autoupdate run}

dispatched=0
dispatch_successor () {
  [ "$dispatched" = 1 ] && return 0
  dispatched=1
  if curl -fsS -X POST \
      -H "Authorization: Bearer $GH_TOKEN" \
      -H "Accept: application/vnd.github+json" \
      -H "Content-Type: application/json" \
      "$GH_API/repos/$GITHUB_REPOSITORY/actions/workflows/$RELAY_WORKFLOW/dispatches" \
      -d "{\"ref\":\"$GH_REF\"}"; then
    echo "[supervisor] successor generation dispatched; it starts when this one ends"
  else
    echo "::error::could not dispatch the successor relay; the schedule starts one within 10 minutes"
  fi
}

$RELAY_CMD &
relay=$!
TUNNEL_TOKEN="$CF_TUNNEL_TOKEN" $TUNNEL_CMD &
tunnel=$!
echo "[supervisor] relay pid=$relay tunnel pid=$tunnel, serving for ${RUN_FOR_S}s"

status=0
# Milliseconds, not seconds: whole-second truncation ended a generation up to a second early.
deadline=$(( $(date +%s%3N) + RUN_FOR_S * 1000 ))
handover_at=$(( deadline - HANDOVER_LEAD_S * 1000 ))
while :; do
  if ! kill -0 "$relay" 2>/dev/null; then
    echo "::error::the relay exited before its time was up"
    status=1
    break
  fi
  if ! kill -0 "$tunnel" 2>/dev/null; then
    echo "::error::the tunnel connector exited before its time was up"
    status=1
    break
  fi
  now=$(date +%s%3N)
  if [ "$now" -ge "$handover_at" ]; then dispatch_successor; fi
  if [ "$now" -ge "$deadline" ]; then
    echo "[supervisor] generation complete"
    break
  fi
  sleep "$POLL_S"
done

if [ "$status" = 0 ]; then
  dispatch_successor
else
  echo "[supervisor] no successor dispatched after an early death; the schedule will start one"
fi
kill "$relay" "$tunnel" 2>/dev/null
wait "$relay" "$tunnel" 2>/dev/null
exit $status
