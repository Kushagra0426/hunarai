# Design

Connection layer for a multi-tenant platform: thousands of concurrent long-lived
WebSocket sessions, spread across several nodes behind a load balancer, on behalf
of multiple organizations.

## The problem, stated plainly

Long-lived connections are difficult in a way ordinary requests are not. A normal
request is over in milliseconds, so the server barely has to remember anything.
A session that lasts from seconds to hours means the server is holding something
on behalf of a user the entire time — and several servers are each holding their
own share, with no single one able to see the whole.

Six requirements follow from that, and four of them turn out to be the same
requirement wearing different clothes.

## The shaping insight

Look at orphan cleanup (2), quota enforcement (3), fair sharing (4) and
distributed queries (6) together. Every one fails the moment a node relies on its
own local tally:

- A node cannot enforce an org's limit, because it only sees its own connections.
- A dead node cannot correct its own count — the thing that would correct it is
  the thing that died.
- No node can answer "how many sessions does Acme have" without asking the others.

So the architecture is one sentence:

> **Nodes host connections. A shared store owns the truth about them.**

Everything else is consequence.

## Why Redis

Speed matters — admission runs on every connect — but that is not the reason.
The reason is that **Redis executes a Lua script atomically**. No other client's
commands interleave with it.

That is what makes requirement 3 solvable. Two connections arriving in the same
millisecond on different nodes both read "499 of 500", both conclude there is
room, and both get admitted. Serialise the check and the increment into one
indivisible step and the race disappears.

Measured, same logic and same concurrency, only atomicity differing:

```
50 simultaneous admissions against a limit of 10

check-then-increment  admitted = 50   ← limit breached
one atomic script     admitted = 10   ← holds
```

And on the full cluster — 150 simultaneous connections through nginx to three
nodes, against `initech`'s limit of 100:

```
peak concurrent   100        limit held
rejected           50        all 4429 ORG_LIMIT
spread             node1: 36   node2: 27   node3: 37
```

That is `tests/test_admission.py::test_concurrent_admissions_never_exceed_the_limit`.
It is the single most load-bearing test here.

## Three stores, three jobs

| Store | Owns | Why it, and not the others |
| --- | --- | --- |
| **Redis** | Live state: counts, session locations, node liveness | Atomic admission in one round-trip. Authoritative for anything a quota decision depends on. |
| **Postgres** | Org configuration, session audit trail | Durable, queryable, survives a Redis restart. |
| **The process** | The actual socket objects | Only the node holding a connection can close it. |

The tempting mistake is enforcing quotas in Postgres — it is the "real" database.
That means a transaction with row locking on every connect, and under
simultaneous arrivals you get lock contention or deadlocks precisely when load is
highest. Postgres is for what must survive forever. Redis is for what must be
correct *right now*. Different jobs.

Limits live on `Organization` rows but are cached in `org:{slug}:cfg` and read
from there on every connect, with a `post_save` signal writing through so an
admin edit takes effect immediately.

## Key layout

```
session:{sid}            hash    org, node, client, started_at
org:{slug}:sessions      set     session ids for this org
node:{node}:sessions     set     session ids here — what to reap when it dies
org:{slug}:count         int     live count, authoritative for limits
org:{slug}:cfg           hash    max_sessions, reserved_floor (cached)
node:{node}:hb           string  heartbeat marker, TTL
node:{node}:draining     flag    set during drain, blocks new admissions
nodes:alive              zset    score = last heartbeat, ms
global:count             int     live count across all orgs
reaper:lock              string  one reaper per round
```

Counts are denormalised from the sets deliberately: admission reads them on every
connect, and an O(1) `GET` beats an O(n) `SCARD` once an org holds thousands.

## The requirements, one by one

### 1 — Connection lifecycle

Channels calls `connect()` on arrival and `disconnect()` on departure, including
abnormal closes. Sessions are registered in Redis and written to the audit trail
on connect, released and closed on disconnect.

Verified with an abrupt transport kill — no close frame — and the slot is still
freed, because Channels calls `disconnect` on transport loss too.

### 2 — Nodes going down

Each node writes a timestamp to `nodes:alive` every `HEARTBEAT_SEC`. A reaper on
every node releases the sessions of nodes silent past `NODE_TIMEOUT_SEC`.

Measured on the compose cluster — 3 nodes, 60 held sessions, `NODE_TIMEOUT_SEC=8`:

```
before          count=60   node2 holding 20
SIGKILL node2   count=60   ghosts still counted, node2 still in nodes:alive
after timeout   count=40   node2 reaped and removed from nodes:alive
audit trail     node2: node_lost = 20
```

The audit trail is the clearest evidence: 20 rows marked `node_lost`, meaning
something noticed the silence and released them. Nothing on the dead node
participated.

Three decisions worth defending:

- **The timeout is a tuning knob, not a correct answer.** A crashed node and a
  briefly unreachable one are indistinguishable from outside. You cannot detect
  death; you can only pick a silence threshold. Too low and a network blip reaps
  a healthy node; too high and ghosts hold quota.
- **A node never reaps itself.** A stale entry for its own id means its heartbeat
  is failing, but it is demonstrably alive — it is running the reaper. Treating
  itself as dead would drop working connections.
- **The reaper runs everywhere, behind a lock.** No singleton service to lose.
  `reap_node.lua` is idempotent, so a lost race is harmless; the lock just keeps
  duplicated work and alarming logs down.

`reap_node.lua` verifies each session hash rather than trusting the node's session
set. On the ordinary path release already removes the id, so the check looks
redundant — but the reaper reads `SMEMBERS` once and then works the list, so a
release landing in between leaves it holding ids already accounted for. Counting
those again drives the count below the truth, and a count that reads low is free
capacity nobody paid for.

### 3 — Capacity limits

Covered above. `admit.lua` applies the org ceiling, the global ceiling and the
reserved floor in one atomic script, returning a typed reason on refusal.

### 4 — Fair resource sharing

Each org has a **hard ceiling** (`max_sessions`) and a **reserved floor**
(`reserved_floor`). The fairness rule is one line, and the conjunction is the
whole guarantee:

```lua
if global_count >= global_max and org_count >= floor then
  return 'GLOBAL_FULL'
end
```

Below its floor an org is admitted **even when the platform is full** —
deliberately overshooting the global cap to honour the promise. That is what stops
a noisy tenant from locking out a paying one that is well under its own limit.
Above the floor, everyone competes first-come for what remains.

Weighted proportional shares would be fairer under genuinely mixed load, but need
continuous recalculation as capacity shifts and are much harder to reason about.
Reserved floors are fifteen lines inside a script that had to exist anyway.

### 5 — Graceful draining

On SIGTERM: set the drain flag in Redis, then close live connections with
**4504**, and the balancer routes retries to a healthy node.

Order is the correctness argument. The flag goes first, so `admit.lua` is already
refusing new sessions for this node before any socket closes. The other order
leaves a window where a connection lands on a node that is shutting down.

Measured on the compose cluster — `docker compose stop node1`, which holds 20 of
60 sessions:

```
before          count=60   node1 holding 20
after stop      count=40   node1 holding 0, removed from nodes:alive
stopped in      under 1s
audit trail     node1: drained = 20
30 reconnects   node2: 15, node3: 15 — none to node1
```

`drained`, not `node_lost`, is the point: the node wound down on its own rather
than the reaper having to clean up after it. The other 40 sessions were
undisturbed.

**Sessions are not migrated.** The connection genuinely breaks and the client
genuinely reconnects. Real migration means serialising live state and handing it
to another node — substantially harder, and out of scope. Stating that beats
implying seamlessness.

Pacing (`DRAIN_BATCH_SIZE`, `DRAIN_INTERVAL_MS`) defaults to closing everything at
once. Setting a batch spreads reconnects so a deploy does not stampede the
surviving nodes, which are already absorbing this node's load.

### 6 — Distributed state

`/api/capacity`, `/api/orgs/<slug>/sessions`, `/api/sessions/<id>`. Any node
answers, because answers come from Redis rather than the node's own view. Every
response carries `answered_by` to make that visible, and the org endpoint includes
a `by_node` breakdown.

## Choices a reviewer might question

**Django Channels.** The consumer lifecycle maps cleanly onto the problem, and
Django brings the ORM and admin for free. It carries more per-connection weight
than bare Starlette or Go — the honest framing is that capacity claims should come
from the load script, not from assertion.

**uvicorn, not daphne.** Not preference. **daphne does not implement the ASGI
lifespan protocol at all**, so the heartbeat and reaper loops silently never
started and dead nodes were never reaped — with no error to show for it. Only the
multi-process chaos test caught this; unit tests drove `reap_once()` directly and
stayed green.

**Async views.** Sync views bridging to the async registry via `asyncio.run()`
create a second event loop inside the running server and fail with "Future
attached to a different loop" the moment they touch the client shared with the
consumers. `/api/capacity` returned 500 on the real server until this was fixed.

**In-process consumer set, not a channel layer.** Drain only ever closes
connections on the node running it, so there is nothing to broadcast. A layer
would add serialisation and group bookkeeping to reach objects already in this
process's memory. The channel layer is `InMemory` for the same reason.

**nginx `least_conn`, not `ip_hash`.** These are long-lived sessions of
unpredictable duration, so counting requests is the wrong measure. And pinning
clients to nodes would hide exactly what the chaos scripts need to show.

**Plain Django views, no DRF.** Three read-only JSON endpoints do not justify a
serialisation framework, a router and a viewset hierarchy.

## Known weaknesses

Named here because a design that hides its limits is harder to trust than one
that states them.

**Redis is a single point of failure.** If it dies, no new connections are
admitted. Existing ones survive — they are just sockets on the nodes. Production
answer: Sentinel or Cluster. Not done here.

**`release.lua` is single-instance only.** It derives org and node key names from
the session hash, so not every touched key is declared in `KEYS`, which Redis
Cluster requires. Upgrade path: hash-tag the keys and pass them explicitly,
accepting an extra `HMGET` in the caller.

**Counts can drift.** A node dying between the increment and the session write
leaks a slot. The heartbeat reaper catches the common case. `manage.py reconcile`
repairs the audit trail but deliberately never touches live counts — guessing at
those is the bug this system exists to avoid. A full count-reconciliation pass is
documented, not built.

**`reap_node.lua` reaps a whole node in one script.** A node holding tens of
thousands of sessions would block Redis for the duration, since scripts are
single-threaded. Upgrade path: pass a batch size, `SPOP` that many per call, loop.

**Rejections write a Postgres row each.** Under a sustained flood that is a write
per refused connect. Wants batching if it becomes real.

**Capacity numbers must be measured.** Any concurrency figure should come from
`scripts/load.py`, not from assertion. No claim is made here that is not in this
document with a number attached.

One measurement subtlety, since it is easy to misread the load script: run
without `--hold`, each client closes as soon as it connects, so later clients
legitimately reuse freed slots and the *cumulative* admitted count can exceed the
limit without it ever having been breached. Only the peak concurrent count
answers "were too many live at once", which is what the script reports and checks.
Run with `--hold` to keep sessions open and watch the cap actually bind.

## Testing approach

74 tests, real Redis throughout — never a mock. What is under test is that Redis
serialises a script and that exactly one concurrent `DEL` wins; a mock would only
prove the mock behaves as imagined.

Two tests were found to be **vacuous and then fixed**, which is worth recording:

1. The reap DEL-gate test passed even with the gate removed, because `release.lua`
   cleans the node set so the dangerous scenario never arose naturally. Rewritten
   to reconstruct the window directly; it now fails when the gate goes.
2. The org-limit cache was written by a signal but never read — enforcement used
   the Postgres row instead. Every unit test was green while an end-to-end run
   admitted 20 against a limit of 5, because the tests called `admit_session`
   directly and never went through the consumer.

Both were caught by end-to-end runs against real processes. The lesson taken:
unit tests verify the pieces, and only the multi-process chaos tests verify that
the pieces are actually wired to each other.

Test settings are pinned in `connmgr/settings_test.py` rather than patched in
`conftest.py`, because pytest-django reads settings before conftest runs — an
earlier version let a run create `test_neondb` on the production Postgres
instance.
