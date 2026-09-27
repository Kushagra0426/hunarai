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
.venv/bin/python manage.py runserver

curl localhost:8000/health     # {"status": "ok", "node_id": "..."}
```

The `node_id` in that response is how the chaos scripts later confirm the load
balancer is genuinely spreading connections across nodes.

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
- [ ] **1 — Session lifecycle.** Models, consumer, Redis register/release.
- [ ] **2 — Atomic admission.** `admit.lua`, per-org caps, reserved floor, race test.
- [ ] **3 — Liveness and reaping.** Heartbeats, orphan cleanup on node death.
- [ ] **4 — Drain and query API.** SIGTERM wind-down, capacity endpoints.
- [ ] **5 — Multi-node deployment.** Compose, nginx, chaos scripts, `DESIGN.md`.

A note on the app name: the app is `connsessions`, not `sessions`, because
`django.contrib.sessions` already claims that label and Django refuses to start
with two apps sharing one. This app is the connection registry, not cookie
sessions.
