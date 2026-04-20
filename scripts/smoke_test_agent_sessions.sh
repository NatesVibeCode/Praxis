#!/bin/bash
# End-to-end smoke test for the agent_sessions service.
# Starts the service if needed, fires one turn, asserts reply mentions /Users/nate/Praxis.

set -euo pipefail

REPO=/Users/nate/Praxis
PORT=8421
BASE="http://127.0.0.1:$PORT"
LOG="$REPO/artifacts/agent_sessions_smoke_$(date -u +%Y%m%d).log"
STARTED_PID=""

mkdir -p "$(dirname "$LOG")"
exec > >(tee -a "$LOG") 2>&1

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { echo "[$(ts)] smoke: $*"; }

cleanup() {
  rc=$?
  if [ -n "$STARTED_PID" ]; then
    log "stopping started service pid=$STARTED_PID"
    kill "$STARTED_PID" 2>/dev/null || true
    wait "$STARTED_PID" 2>/dev/null || true
  fi
  exit "$rc"
}
trap cleanup EXIT

log "starting smoke test"

if ! curl -sf "$BASE/agents" >/dev/null 2>&1; then
  log "service not running — launching in background"
  cd "$REPO"
  PYTHONPATH="$REPO/Code&DBs/Workflow" /opt/homebrew/bin/python3 \
    "$REPO/Code&DBs/Workflow/surfaces/api/agent_sessions.py" \
    >> "$REPO/artifacts/agent_sessions.log" \
    2>> "$REPO/artifacts/agent_sessions.err" &
  STARTED_PID=$!
  for i in $(seq 1 20); do
    if curl -sf "$BASE/agents" >/dev/null 2>&1; then break; fi
    sleep 0.5
  done
  if ! curl -sf "$BASE/agents" >/dev/null 2>&1; then
    log "FAIL: service didn't come up in 10s"
    exit 1
  fi
  log "service up pid=$STARTED_PID"
fi

log "POST /agents with smoke prompt"
resp=$(curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"prompt":"smoke: what directory are you currently running in? answer with the absolute path only."}' \
  "$BASE/agents")
echo "$resp" > "$REPO/artifacts/agent_sessions_smoke_new.json"
agent_id=$(echo "$resp" | /opt/homebrew/bin/python3 -c 'import sys,json;print(json.load(sys.stdin).get("agent_id",""))')
if [ -z "$agent_id" ]; then
  log "FAIL: no agent_id in response: $resp"
  exit 1
fi
log "agent_id=$agent_id"

log "polling GET /agents/$agent_id/messages for up to 60s"
reply=""
for i in $(seq 1 60); do
  events=$(curl -sf "$BASE/agents/$agent_id/messages" || true)
  reply=$(echo "$events" | /opt/homebrew/bin/python3 - <<'PY'
import json, sys
try:
    payload = json.loads(sys.stdin.read())
except Exception:
    sys.exit(0)
events = payload.get("events") if isinstance(payload, dict) else payload
if not isinstance(events, list):
    sys.exit(0)
pieces = []
for ev in events:
    if not isinstance(ev, dict): continue
    t = ev.get("type", "")
    if t == "assistant":
        msg = ev.get("message") or {}
        c = msg.get("content")
        if isinstance(c, str):
            pieces.append(c)
        elif isinstance(c, list):
            for item in c:
                if isinstance(item, dict) and item.get("text"):
                    pieces.append(str(item["text"]))
    elif t == "result":
        r = ev.get("result")
        if isinstance(r, str):
            pieces.append(r)
        elif isinstance(r, dict) and r.get("text"):
            pieces.append(str(r["text"]))
print("".join(pieces))
PY
)
  if echo "$reply" | grep -q '/Users/nate/Praxis'; then
    log "PASS: reply contains /Users/nate/Praxis"
    log "reply excerpt: $(echo "$reply" | head -c 200)"
    curl -sS -X DELETE "$BASE/agents/$agent_id" >/dev/null || true
    exit 0
  fi
  sleep 1
done

log "FAIL: reply did not contain /Users/nate/Praxis within 60s"
log "last reply text: $reply"
curl -sS -X DELETE "$BASE/agents/$agent_id" >/dev/null || true
exit 1
