# Multi-Node Connection Management System

Connection layer for a multi-tenant platform: thousands of concurrent long-lived
WebSocket sessions, spread across several server nodes behind a load balancer, on
behalf of multiple organizations. No managed connection services — the full
lifecycle is owned here.

**Django Channels on uvicorn** holds the sockets · **Redis** owns live state and
admission · **Postgres** holds org config and the session audit trail.

Architecture and the reasoning behind every choice: **[DESIGN.md](DESIGN.md)**.

---

## Setup

Requires Docker and Python 3.12+. Nothing else — no local Redis or Postgres
needed for the cluster.

```bash
git clone git@github.com:Kushagra0426/hunarai.git
cd hunarai

docker compose up --build -d          # 3 nodes + nginx + redis + postgres
```

First build takes a few minutes. When it settles:

```bash
curl localhost:8090/api/capacity      # should list 3 live nodes
open http://localhost:8090/           # the dashboard
```

Migrations and the demo orgs are seeded automatically by a one-shot `migrate`
service, which the nodes wait on — so three of them cannot race each other
applying the same migrations.

### For the test suite and load scripts

These run on your machine, not in a container, so they need a local virtualenv.
The tests also need a **local Redis** on `6379` (they use database 15 and never
touch the cluster's).

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt

.venv/bin/pytest                      # 75 tests
```

### Seeded organizations

| Org | Limit | Reserved floor |
| --- | --- | --- |
| `acme` | 500 | 100 |
| `globex` | 800 | 50 |
| `initech` | 100 | 25 |

Global platform cap is **1000**, deliberately less than the sum of org limits
(1400) so the global ceiling is reachable and testable.

Cluster timings are tightened for demos (`HEARTBEAT_SEC=2`, `NODE_TIMEOUT_SEC=8`)
so a chaos run finishes in seconds rather than the minute a production timeout
would take.

### Ports

| Port | What |
| --- | --- |
| `8090` | nginx load balancer — **use this for everything** |
| `16379` | Redis, exposed so you can watch live state |

Port 8090 rather than 8080 because 8080 is commonly taken. Change it in
`docker-compose.yml` if it clashes.

---

## Testing every requirement

Each section below is self-contained: what to do on the dashboard, what to check
in Docker, and what you should see. Open `http://localhost:8090/` first and keep
it visible.

Reset between tests with **Close all** on the dashboard, or:

```bash
docker compose exec redis redis-cli flushdb
```

---

### 1 — Connection lifecycle

*Every session is tracked, and cleaned up whether the client leaves politely or not.*

**Dashboard:** pick `acme`, set **5**, press **Open**. The counters go to 5 and
the nodes show the spread. Press **Close all** — back to 0.

**Docker — see the actual records:**

```bash
docker compose exec redis redis-cli get org:acme:count
docker compose exec redis redis-cli smembers org:acme:sessions
docker compose exec redis redis-cli hgetall session:<paste-a-session-id>
```

The session hash holds the org, the node holding it, the client id, and the start
time.

**The harder case — a client that vanishes without saying goodbye.** Open 5, then
kill the browser tab outright rather than pressing Close all. The count still
returns to 0: Channels fires `disconnect` on transport loss, not just on a clean
close.

**Expected:** count rises to 5, falls to 0 either way, and the org set empties.

---

### 2 — A server node goes down

*Sessions on a dead node must not linger as if they were still active.*

**Dashboard:** pick `acme`, set **60**, **Open**. The three node cards fill up —
not evenly, since nginx uses `least_conn` rather than strict round-robin.

**Docker — kill one outright, with no chance to clean up:**

```bash
docker compose kill -s SIGKILL node2
```

**Watch the dashboard.** Within about 10 seconds (`NODE_TIMEOUT_SEC=8` plus poll
lag) the `node2` card turns red and reads **GONE**, the session total drops by
node2's share, and the org's usage bar falls back.

**Docker — confirm the state is genuinely correct, not just repainted:**

```bash
docker compose exec redis redis-cli get org:acme:count          # dropped by node2's share
docker compose exec redis redis-cli scard node:node2:sessions   # 0
docker compose exec redis redis-cli zrange nodes:alive 0 -1     # node2 absent

docker compose exec postgres psql -U connmgr -d connmgr -c \
  "select node_id, end_reason, count(*) from connsessions_sessionrecord
   where node_id='node2' group by 1,2;"
```

**Expected:** sessions released, node gone from the liveness set, and its rows
marked **`node_lost`** — the audit trail records that something noticed the
silence, rather than the node reporting in. A real run of exactly this:

```
before:  acme=60   node2 holds 28
after:   acme=32   node2 set=0   alive=[node3 node1]
         node2 | node_lost | 28
```

Restore it: `docker compose up -d node2`

---

### 3 — Capacity limits

*A per-org cap that holds even when connections arrive simultaneously on different nodes.*

**Dashboard:** pick `initech` (limit **100**), set **150**, press **Open**.

**Expected, in the activity log:**

```
established 100   →   node1: 26   node2: 46   node3: 28
refused 50        code 4429  (org at its limit)
```

The usage bar goes red and full. **Established is always exactly 100; the spread
across nodes varies run to run** — `least_conn` distributes by current load, not
evenly. That is the interesting part: those 150 arrived at three different nodes
simultaneously, in whatever proportion, and the cap still bound to the exact
number.

**Docker:**

```bash
docker compose exec redis redis-cli get org:initech:count       # exactly 100
```

**The same thing from the CLI, which reports more detail:**

```bash
.venv/bin/python scripts/load.py --org initech --count 150 --hold 10
```

Use `--hold` to keep sessions open. Without it each client closes immediately, so
later clients legitimately reuse freed slots and the *cumulative* admitted count
exceeds the limit without it ever being breached — the script reports peak
concurrent for exactly this reason.

**The unit test is the sharpest version of this proof:**

```bash
.venv/bin/pytest tests/test_admission.py -v -k concurrent
```

---

### 4 — Fair resource sharing

*One org must not be able to consume all available capacity.*

This needs the platform genuinely saturated, so it takes two steps. The global cap
is 1000.

**Step 1 — fill the platform** using two terminals, or the dashboard twice:

```bash
.venv/bin/python scripts/load.py --org globex --count 800 --hold 60 &
sleep 10
.venv/bin/python scripts/load.py --org acme --count 200 --hold 45 &
sleep 10
docker compose exec redis redis-cli get global:count      # 1000 — saturated
```

**Step 2 — a quiet org asks for capacity on a full platform.** `initech` holds
nothing and has a reserved floor of 25:

```bash
.venv/bin/python scripts/load.py --org initech --count 60 --hold 5
```

**Expected:**

```
established : 25
refused 35        code 4503  (platform full)
```

**This is the whole point.** The platform had *zero* free capacity, yet initech
still got exactly its guaranteed floor of 25. Without the floor it would have been
locked out entirely by a noisy neighbour — despite paying, and despite being far
below its own limit of 100. Above the floor it competes like everyone else, which
is why the other 35 were refused.

Measured output from a real run of exactly this:

```
platform: global=1000 of 1000  (SATURATED)
  globex=800  acme=200  initech=0
initech asks for 60:
  established : 25        ← its floor, honoured
  refused 35   code 4503  ← above the floor, competing and losing
```

---

### 5 — Graceful draining

*A node shutting down should wind down, not drop everything at once.*

**Dashboard:** pick `acme`, set **60**, **Open**. Wait for all 60.

**Docker — shut one down politely:**

```bash
docker compose stop node2
```

**Watch the dashboard activity log.** It fills with:

```
session closed 4504 — node draining, reconnect elsewhere
```

That is the contrast with test 2. There the node died and something else had to
notice. Here the node *told* its clients to move before going away.

**Docker — the audit trail records the difference:**

```bash
docker compose exec postgres psql -U connmgr -d connmgr -c \
  "select node_id, end_reason, count(*) from connsessions_sessionrecord
   where end_reason in ('drained','node_lost') group by 1,2 order by 1,2;"
```

**Expected:** node2's sessions marked **`drained`**, not `node_lost` — the node
cleaned up after itself rather than the reaper having to. The other 40 sessions on
node1 and node3 are untouched.

**Confirm new connections land elsewhere** while node2 is down — press **Open**
again and the spread covers only node1 and node3.

Restore: `docker compose up -d node2`

**Pacing** is off by default (`DRAIN_BATCH_SIZE=0` closes everything at once). Set
a batch size and `DRAIN_INTERVAL_MS` in `docker-compose.yml` to spread reconnects
so a deploy does not stampede the surviving nodes.

---

### 6 — Distributed state

*Any node, or an external service, must be able to answer questions about any session.*

```bash
curl localhost:8090/api/capacity | python3 -m json.tool
curl localhost:8090/api/orgs/acme/sessions | python3 -m json.tool
curl localhost:8090/api/sessions/<session-id> | python3 -m json.tool
```

Every response carries **`answered_by`** — the node that handled that request.
Open some sessions first, then call the endpoint repeatedly: different nodes
answer, and the numbers are identical every time. That is the requirement — the
answer does not depend on which node you asked.

```bash
.venv/bin/python scripts/load.py --org acme --count 30 --hold 20 &
sleep 8
for i in 1 2 3 4 5 6; do
  curl -s localhost:8090/api/capacity | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(d['answered_by'], '→', d['global']['sessions'])"
done
```

```
node1 → 30      ← different nodes answering
node3 → 30
node1 → 30
node3 → 30
```

Do this on an idle cluster and you will likely see the *same* node every time.
That is nginx `least_conn` working as intended, not a bug: with no connections to
balance it keeps choosing the same upstream. The rotation appears once there is
load to spread — and note it favours the least-loaded nodes, so a node carrying
more WebSocket sessions gets fewer API requests.

`/api/orgs/<slug>/sessions` includes a `by_node` breakdown — that is what shows
the balancer spreading an org across nodes, and what shows a node's sessions
vanishing when it dies.

**Django admin** at `/admin` gives the full searchable session history:

```bash
docker compose exec node1 python manage.py createsuperuser
```

---

## Rejection codes

The distinction matters: some mean *retrying is pointless*, others mean *try again*.

| Code | Meaning | Retry? |
| --- | --- | --- |
| `4404` | Unknown org | No |
| `4403` | Org inactive | No |
| `4429` | Org at its own ceiling | No — buy more capacity |
| `4503` | Platform full, org above its floor | Yes, later |
| `4504` | Node draining or shutting down | Yes, immediately — another node will take it |
| `4409` | Duplicate session id | No |

---

## Connecting a client directly

```
ws://localhost:8090/ws/<org_slug>/?client_id=<opaque-id>
```

The server replies with `session.established` carrying the session id, the node
that took the connection, and the org's usage against its limit. `{"type":
"ping"}` returns a `pong`, which is how the scripts confirm a held connection is
genuinely alive rather than merely unclosed.

---

## Tests

```bash
.venv/bin/pytest                      # 75 tests, needs local Redis on 6379
.venv/bin/pytest tests/test_admission.py -v   # the admission race
.venv/bin/pytest tests/test_reaper.py -v      # node death, fake clock
.venv/bin/pytest tests/test_drain.py -v       # graceful shutdown
```

Real Redis throughout, never a mock — what is under test is that Redis serialises
a script, and a mock would only prove the mock behaves as imagined. Tests use
database 15 and pin their own settings module, so they cannot reach the cluster's
Redis or any configured Postgres.

The scripted equivalents of tests 2 and 5, if you prefer one command:

```bash
./scripts/chaos_kill.sh node2         # SIGKILL and watch the reap
./scripts/chaos_drain.sh node2        # SIGTERM and watch the drain
```

---

## Repairing the audit trail

A node that dies while not yet in `nodes:alive` — killed before its first
heartbeat, or during a Redis outage — is never discovered, so its audit rows stay
open while the live count stays correct.

```bash
docker compose exec node1 python manage.py reconcile --dry-run
docker compose exec node1 python manage.py reconcile
```

Redis is the authority on what is live, so a row with no session in Redis is
finished by definition. This repairs **only** the audit trail and never touches
live counts — guessing at those is the bug this system exists to avoid. It is not
on a timer either: a scheduled job that quietly fixes drift hides whatever causes
it.

---

## Configuration

Everything is environment-driven so one image runs as any node. See
`.env.example` for the annotated list; the two worth understanding:

- **`NODE_TIMEOUT_SEC`** — how long a node must be silent before its sessions are
  reaped. A crashed node and a briefly unreachable one are indistinguishable from
  outside, so this is a tuning knob, not a correct answer.
- **`GLOBAL_MAX_SESSIONS`** — the platform-wide ceiling. Per-org limits and
  reserved floors live on `Organization` rows instead, since they differ per
  tenant, and are editable in Django admin with immediate effect.

---

## Shutting down

```bash
docker compose down            # stop everything
docker compose down -v         # also drop the Postgres volume
```

---

## Layout

```
connsessions/lua/            the correctness core — admission, release, reaping
connsessions/registry.py     Redis client and key layout
connsessions/consumers.py    WebSocket lifecycle
connsessions/tasks.py        heartbeat and reaper loops
connsessions/drain.py        graceful shutdown
connsessions/views.py        query API and dashboard
tests/                       75 tests against real Redis
scripts/                     load generation and chaos
DESIGN.md                    architecture and decisions
```
