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
| Django Channels | The sockets themselves | Consumer `connect`/`disconnect` maps onto the session lifecycle |
| Redis | Live session state, counts, node liveness | Atomic admission in one round-trip; authoritative for any quota decision |
| Postgres | Org config, session audit trail | Durable and queryable — deliberately *not* the quota enforcer, since per-connect row locking contends exactly when load peaks |

## Running it

Requires Redis on `localhost:6379`. Postgres is optional — without
`DATABASE_URL` it falls back to sqlite, which is enough for a smoke test.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
cp .env.example .env

.venv/bin/python manage.py migrate
.venv/bin/python manage.py seed_orgs        # acme, globex, initech
.venv/bin/daphne connmgr.asgi:application   # daphne, not runserver: websockets

curl localhost:8000/health     # {"status": "ok", "node_id": "..."}
```

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
| `4504` | Node draining | Yes, immediately — another node will take it |
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

## Build phases

- [x] **0 — Scaffold.** Project layout, env config, health endpoint, ASGI wiring.
- [x] **1 — Session lifecycle.** Models, consumer, Redis register/release.
- [x] **2 — Atomic admission.** `admit.lua`, per-org caps, reserved floor, race test.
- [ ] **3 — Liveness and reaping.** Heartbeats, orphan cleanup on node death.
- [ ] **4 — Drain and query API.** SIGTERM wind-down, capacity endpoints.
- [ ] **5 — Multi-node deployment.** Compose, nginx, chaos scripts, `DESIGN.md`.

A note on the app name: the app is `connsessions`, not `sessions`, because
`django.contrib.sessions` already claims that label and Django refuses to start
with two apps sharing one. This app is the connection registry, not cookie
sessions.
