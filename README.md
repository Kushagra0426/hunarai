# Multi-Node Connection Management System

Connection layer for a multi-tenant platform: thousands of concurrent
long-lived WebSocket sessions, spread across several server nodes behind a load
balancer, on behalf of multiple organizations. No managed connection services —
the full lifecycle is owned here.

## The shaping idea

Orphan cleanup, quota enforcement, fair sharing and distributed queries all fail
the moment a node trusts its own local tally. So:

> **Nodes host connections. A shared store owns the truth about them.**

Redis is that store — not for speed alone, but because Lua scripts run
atomically. Two connections arriving in the same millisecond on different nodes
get serialized inside one script: one admitted, one rejected. That is the
admission race, solved. The rest of the design follows from it.

## Layout

| Layer | Holds | Why |
| --- | --- | --- |
| Django Channels on uvicorn | The sockets themselves | Consumer `connect`/`disconnect` maps onto the session lifecycle; uvicorn because the background loops need the ASGI lifespan |
| Redis | Live session state, counts, node liveness | Atomic admission in one round-trip; authoritative for any quota decision |
| Postgres | Org config, session audit trail | Durable and queryable — deliberately *not* the quota enforcer, since per-connect row locking contends exactly when load peaks |

## Running the cluster

Three nodes behind nginx, with Redis and Postgres. This is the way to see the
system actually behave — requirements 2, 3 and 5 are multi-machine failures, and
a single process cannot demonstrate them.

```bash
docker compose up --build -d
curl localhost:8090/api/capacity          # three live nodes

# requirement 3 — the per-org cap holds across nodes
.venv/bin/python scripts/load.py --org initech --count 150

# requirement 2 — kill a node, watch its sessions get reaped
./scripts/chaos_kill.sh node2

# requirement 5 — stop a node gracefully, watch clients move
./scripts/chaos_drain.sh node2
```

Three nodes rather than two: with two, a killed node's sessions can only land on
the single obvious survivor, which shows less than seeing them spread.

The chaos scripts need `websockets` locally (`pip install -r requirements-dev.txt`).
Redis is published on `16379` so you can watch live state while they run.

## Running a single node

Requires Redis on `localhost:6379`. Postgres is optional — without
`DATABASE_URL` it falls back to sqlite, which is enough for a smoke test.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
cp .env.example .env

.venv/bin/python manage.py migrate
.venv/bin/python manage.py seed_orgs        # acme, globex, initech
.venv/bin/uvicorn connmgr.asgi:application --lifespan on

curl localhost:8000/health     # {"status": "ok", "node_id": "..."}
```

uvicorn rather than daphne, and `--lifespan on` is required: the heartbeat and
reaper loops start from the ASGI lifespan, and **daphne does not implement the
lifespan protocol at all**, so under daphne they silently never run and dead nodes
are never reaped.

The `node_id` in that response is how the chaos scripts later confirm the load
balancer is genuinely spreading connections across nodes.

### Connecting a session

```
ws://localhost:8000/ws/<org_slug>/?client_id=<opaque-id>
```

The server replies with `session.established` carrying the session id, the node
that took the connection, and the org's current usage against its limit.
`{"type": "ping"}` gets a `pong`, which is how the chaos scripts confirm a held
connection is genuinely alive rather than merely unclosed.

Rejections arrive as application close codes, split by whether retrying helps:

| Code | Meaning | Retry? |
| --- | --- | --- |
| `4404` | Unknown org | No |
| `4403` | Org inactive | No |
| `4429` | Org at its own ceiling | No — buy more capacity |
| `4503` | Platform full, org above its floor | Yes, later |
| `4504` | Node draining or shutting down | Yes, immediately — another node will take it |
| `4409` | Duplicate session id | No |

Watch the live state while a client is connected:

```bash
redis-cli get org:acme:count
redis-cli smembers org:acme:sessions
redis-cli hgetall session:<session-id>
```

## Admission control

Every limit is checked and applied inside one Lua script
(`connsessions/lua/admit.lua`). The reason is the race: two connections arriving
in the same millisecond on different nodes would both read "499 of 500", both
decide there is room, and both be admitted. Redis runs a script start to finish
without interleaving another client, so they are serialised instead.

Measured, not asserted — same logic, same concurrency, only atomicity differs:

```
50 simultaneous admissions against a limit of 10

check-then-increment  admitted = 50   ← limit breached
one atomic script     admitted = 10   ← holds
```

Three rules, in order:

1. **Org ceiling** — `org_count >= max_sessions` → `ORG_LIMIT`. Binds however
   empty the platform is; it is what the tenant pays for.
2. **Fair sharing** — `global_count >= GLOBAL_MAX_SESSIONS` **and**
   `org_count >= reserved_floor` → `GLOBAL_FULL`. Note the `and`: below its floor
   an org is admitted even when the platform is full, which is what stops a noisy
   tenant from locking out a paying one that is well under its own limit. Above
   the floor everyone competes first-come.
3. **Guards** — a duplicate session id and a draining node are both refused here
   rather than in Python, so the check is atomic with the increment.

Limits live on `Organization` rows in Postgres but are cached in
`org:{slug}:cfg` and read from there on every connect; a `post_save` signal
writes through, so an admin edit takes effect immediately.

## Node death and orphan reaping

When a node crashes nobody sends a goodbye. The sessions it held are gone but
still counted, and an org whose quota is full of ghosts cannot connect. Fixing
that is requirement 2.

Each node writes a timestamp to `nodes:alive` every `HEARTBEAT_SEC`. A reaper on
every node looks for entries older than `NODE_TIMEOUT_SEC` and releases those
nodes' sessions via `reap_node.lua`.

Measured, with `NODE_TIMEOUT_SEC=6`:

```
7 sessions across 2 nodes, SIGKILL one holding 4

immediately after kill   org_count = 7    ghosts still counted
4.5s later               org_count = 3    reaped
surviving node           still serving, ping still answered
```

Three things worth knowing:

- **The timeout is a tuning knob, not a correct answer.** A crashed node and a
  briefly unreachable one are indistinguishable from outside. Too low and a
  network blip reaps a healthy node's sessions; too high and dead nodes hold
  quota. There is no value that is right, only one that is tuned.
- **A node never reaps itself.** If its own heartbeat is stale it is still
  demonstrably alive — it is running the reaper — so treating itself as dead would
  drop working connections.
- **The reaper runs everywhere, guarded by a lock.** No singleton service to lose;
  whoever is up does the work. `reap_node.lua` is idempotent, so a lost race is
  harmless, but the lock keeps duplicated work and alarming log lines down.

`reap_node.lua` verifies each session hash rather than trusting the node's session
set. On the ordinary path release already removes the id, so the check looks
redundant — but the reaper reads `SMEMBERS` once and then works the list, so a
release landing in between leaves it holding ids that are already accounted for.
Counting those again drives the count below the truth, and a count that reads low
is free capacity nobody paid for.

## Graceful drain

On SIGTERM a node sets its drain flag in Redis, then closes its live connections
with **4504**, and the load balancer sends the retries to a healthy node.

Order matters and is the whole correctness argument: the flag goes in *first*, so
`admit.lua` is already refusing new sessions for this node before any socket is
closed. The other order leaves a window where a connection lands on a node that
is shutting down.

Measured — SIGTERM a node holding 6 of 6 sessions:

```
all closed in 0.66s with code 4504
org_count       6 -> 0
nodes:alive     drain-n1 removed
6 reconnects    all landed on drain-n2
audit trail     6 rows marked "drained", not "client"
```

**Sessions are not migrated.** The connection genuinely breaks and the client
genuinely reconnects. Moving a live session would mean serialising its state and
handing it to another node — a substantially harder problem, and out of scope
here. Better said plainly than implied away.

Drain is installed as a SIGTERM handler rather than run from lifespan shutdown,
which is too late: uvicorn handles SIGTERM by closing live websockets with `1012`
(Service Restart) and only *then* fires the shutdown event, so a drain there
finds nothing left to close and clients cannot tell a planned drain from a crash.

Pacing is off by default — `DRAIN_BATCH_SIZE=0` closes everything at once. Set a
batch size and `DRAIN_INTERVAL_MS` to spread the reconnects; the surviving nodes
are already absorbing this node's load, so handing them every reconnect in the
same instant is the worst moment to do it.

## Query API

Requirement 6: any node answers, because the answers come from Redis rather than
from the node's own view of its connections.

```bash
curl localhost:8000/api/capacity
curl localhost:8000/api/orgs/acme/sessions
curl localhost:8000/api/sessions/<session-id>
```

Every response carries `answered_by`, so you can see that a session on `node-1`
is reported identically by `node-2`. `/api/orgs/<slug>/sessions` includes a
`by_node` breakdown — that is what shows the balancer spreading an org across
nodes, and what shows a node's sessions vanishing when it dies.

The views are **async**. They share the process's Redis client with the
consumers, so a sync view bridging via `asyncio.run()` creates a second event
loop inside the running server and fails with "Future attached to a different
loop" the moment it touches that client.

`/health` returns **503 while draining**, so the load balancer stops sending new
connections to a node that is on its way out.

### Repairing the audit trail

A node that dies while not in `nodes:alive` at all — killed before its first
heartbeat, or during a Redis outage — is never discovered, so its audit rows stay
open while the live count stays correct.

```bash
.venv/bin/python manage.py reconcile --dry-run
.venv/bin/python manage.py reconcile
```

Redis is the authority on what is live, so a row with no session in Redis is
finished by definition. Only the audit trail is repaired; live counts are never
touched, because guessing at them is the bug this system exists to avoid. Not on a
timer either — a scheduled job that quietly fixes drift hides whatever causes it.

### Tests

```bash
.venv/bin/pytest          # needs a local Redis; uses db 15, ignores DATABASE_URL
```

The one worth reading is `test_concurrent_admissions_never_exceed_the_limit` in
`tests/test_admission.py`. Real Redis throughout, never a mock: what is under
test is that Redis serialises a script, and a mock would only prove the mock
behaves the way I imagined.

## Configuration

All environment-driven so one image runs as any node — see `.env.example` for
the annotated list. The two worth understanding:

- **`NODE_TIMEOUT_SEC`** — how long a node must stay silent before its sessions
  are reaped. A crashed node and a briefly unreachable one are indistinguishable
  from outside, so this is a tuning knob, not a correct answer.
- **`GLOBAL_MAX_SESSIONS`** — the platform-wide ceiling. Per-org limits and
  reserved floors live on `Organization` rows instead, since they differ per
  tenant.

Full architecture, the reasoning behind each choice, and a frank list of known
weaknesses: **[DESIGN.md](DESIGN.md)**.

## Build phases

- [x] **0 — Scaffold.** Project layout, env config, health endpoint, ASGI wiring.
- [x] **1 — Session lifecycle.** Models, consumer, Redis register/release.
- [x] **2 — Atomic admission.** `admit.lua`, per-org caps, reserved floor, race test.
- [x] **3 — Liveness and reaping.** Heartbeats, orphan cleanup on node death.
- [x] **4 — Drain and query API.** SIGTERM wind-down, capacity endpoints.
- [x] **5 — Multi-node deployment.** Compose, nginx, chaos scripts, `DESIGN.md`.

A note on the app name: the app is `connsessions`, not `sessions`, because
`django.contrib.sessions` already claims that label and Django refuses to start
with two apps sharing one. This app is the connection registry, not cookie
sessions.
