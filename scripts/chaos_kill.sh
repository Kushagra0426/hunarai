#!/usr/bin/env bash
# Requirement 2: SIGKILL a node and watch its orphaned sessions get reaped.
#
# SIGKILL, not SIGTERM: the point is a node that dies without any chance to clean
# up after itself. Nothing is released, no goodbye is sent, and the sessions it
# held are counted against their orgs until something notices the silence.
set -euo pipefail

VICTIM="${1:-node2}"
LB="${LB:-http://localhost:8090}"
HOLD="${HOLD:-45}"
COUNT="${COUNT:-60}"

cd "$(dirname "$0")/.."
PY="${PY:-.venv/bin/python}"

capacity() { curl -s "$LB/api/capacity"; }
acme()     { curl -s "$LB/api/orgs/acme/sessions"; }

echo "=== opening $COUNT sessions across the cluster ==="
$PY scripts/load.py --count "$COUNT" --hold "$HOLD" --url "${LB/http/ws}" &
LOAD_PID=$!
sleep 8

echo
echo "before the kill:"
acme | $PY -m json.tool | grep -E '"sessions"|"by_node"' -A5 | head -8

echo
echo "=== docker kill $VICTIM (SIGKILL, no cleanup) ==="
docker compose kill -s SIGKILL "$VICTIM"
KILLED_AT=$(date +%s)

echo
echo "watching the count recover:"
for i in $(seq 1 30); do
    sleep 1
    COUNT_NOW=$(acme | $PY -c "import sys,json; print(json.load(sys.stdin)['sessions'])" 2>/dev/null || echo "?")
    NODES=$(capacity | $PY -c "import sys,json; print(','.join(n['node_id'] for n in json.load(sys.stdin)['nodes']))" 2>/dev/null || echo "?")
    ELAPSED=$(( $(date +%s) - KILLED_AT ))
    echo "  +${ELAPSED}s  sessions=$COUNT_NOW  live_nodes=[$NODES]"
    if [[ "$NODES" != *"$VICTIM"* ]] && [[ "$NODES" != "?" ]]; then
        echo
        echo "=== $VICTIM reaped after ${ELAPSED}s ==="
        break
    fi
done

echo
echo "final state:"
acme | $PY -m json.tool | head -12

echo
echo "audit rows for the dead node:"
docker compose exec -T postgres psql -U connmgr -d connmgr -t -c \
    "select end_reason, count(*) from connsessions_sessionrecord
     where node_id='$VICTIM' group by end_reason order by 2 desc;"

wait $LOAD_PID 2>/dev/null || true

echo
echo "restoring $VICTIM"
docker compose up -d "$VICTIM" >/dev/null
