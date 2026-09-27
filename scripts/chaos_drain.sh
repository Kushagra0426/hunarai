#!/usr/bin/env bash
# Requirement 5: SIGTERM a node and watch it wind down cleanly.
#
# The contrast with chaos_kill.sh is the point. There the node dies and something
# else has to notice; here the node knows it is going away, stops admitting, and
# closes its sessions with 4504 so clients reconnect rather than hang.
set -euo pipefail

VICTIM="${1:-node2}"
LB="${LB:-http://localhost:8090}"
COUNT="${COUNT:-60}"

cd "$(dirname "$0")/.."
PY="${PY:-.venv/bin/python}"

acme() { curl -s "$LB/api/orgs/acme/sessions"; }

echo "=== opening $COUNT sessions across the cluster ==="
$PY scripts/load.py --count "$COUNT" --hold 40 --url "${LB/http/ws}" &
LOAD_PID=$!
sleep 8

echo
echo "before the drain:"
acme | $PY -c "
import sys, json
d = json.load(sys.stdin)
print(f\"  sessions {d['sessions']}/{d['limit']}  by_node={d['by_node']}\")
"

VICTIM_BEFORE=$(acme | $PY -c "import sys,json; print(json.load(sys.stdin)['by_node'].get('$VICTIM',0))")
echo "  $VICTIM is holding $VICTIM_BEFORE"

echo
echo "=== docker stop $VICTIM (SIGTERM, graceful) ==="
STARTED=$(date +%s)
docker compose stop -t 30 "$VICTIM"
echo "  stopped in $(( $(date +%s) - STARTED ))s"

sleep 3
echo
echo "after the drain:"
acme | $PY -c "
import sys, json
d = json.load(sys.stdin)
print(f\"  sessions {d['sessions']}/{d['limit']}  by_node={d['by_node']}\")
print(f\"  {'$VICTIM' not in d['by_node'] and 'no sessions left on $VICTIM' or 'STILL HOLDING SESSIONS'}\")
"

echo
echo "close reasons recorded for $VICTIM:"
docker compose exec -T postgres psql -U connmgr -d connmgr -t -c \
    "select end_reason, count(*) from connsessions_sessionrecord
     where node_id='$VICTIM' group by end_reason order by 2 desc;"
echo "  ('drained' is the clean wind-down; 'node_lost' would mean the reaper had to"
echo "   step in, which is the failure this script is checking does not happen)"

wait $LOAD_PID 2>/dev/null || true

echo
echo "=== reconnecting: clients should land on the surviving nodes ==="
$PY scripts/load.py --count 30 --hold 3 --url "${LB/http/ws}" 2>&1 | tail -8

echo
echo "restoring $VICTIM"
docker compose up -d "$VICTIM" >/dev/null
sleep 5
curl -s "$LB/api/capacity" | $PY -c "
import sys, json
d = json.load(sys.stdin)
print('  live nodes:', [n['node_id'] for n in d['nodes']])
"
