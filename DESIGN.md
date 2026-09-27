# Design

How to run and verify all of this: **[README.md](README.md)**.

## The problem

Long-lived connections are hard in a way ordinary requests are not. A normal
request is over in milliseconds; a session lasting seconds to hours means the
server holds something on a user's behalf the whole time — and several servers
each hold their own share, none able to see the whole.

## The shaping insight

Four of the six requirements — orphan cleanup, quota enforcement, fair sharing,
distributed queries — are the same requirement in different clothes. Each fails
the moment a node trusts its own local tally:

- A node cannot enforce an org's limit; it only sees its own connections.
- A dead node cannot correct its own count — the thing that would correct it is
  the thing that died.
- No node can answer "how many sessions does Acme have" without asking the others.

So the architecture is one sentence:

> **Nodes host connections. A shared store owns the truth about them.**

Everything else follows.

## Why Redis

Speed matters — admission runs on every connect — but that is not the reason. The
reason is that **Redis runs a Lua script atomically**: no other client's commands
interleave with it.

That is what makes the capacity requirement solvable. Two connections arriving in
the same millisecond on different nodes both read "499 of 500", both conclude
there is room, and both get admitted. Collapse the check and the increment into
one indivisible step and the race disappears.

Measured — same logic, same concurrency, only atomicity differing:

```
50 simultaneous admissions against a limit of 10
  check-then-increment   admitted = 50    limit breached
  one atomic script      admitted = 10    holds
```

## Three stores, three jobs

| Store | Owns | Why it and not the others |
| --- | --- | --- |
| **Redis** | Live state: counts, session locations, node liveness | Atomic admission in one round-trip. Authoritative for any quota decision. |
| **Postgres** | Org config, session audit trail | Durable, queryable, survives a Redis restart. |
| **The process** | The socket objects | Only the node holding a connection can close it. |

The tempting mistake is enforcing quotas in Postgres — it is the "real" database.
That means a transaction with row locking on every connect, and under simultaneous
arrivals you get contention or deadlocks precisely when load is highest. Postgres
is for what must survive forever; Redis for what must be correct *right now*.

Limits live on `Organization` rows but are cached in `org:{slug}:cfg` and read from
there on every connect, with a `post_save` signal writing through so an admin edit
takes effect immediately.

## Key layout

```
session:{sid}          hash    org, node, client, started_at
org:{slug}:sessions    set     session ids for this org
node:{node}:sessions   set     what to reap when this node dies
org:{slug}:count       int     live count — authoritative for limits
org:{slug}:cfg         hash    max_sessions, reserved_floor (cached)
nodes:alive            zset    score = last heartbeat, ms
node:{node}:draining   flag    set during drain, blocks new admissions
global:count           int     live count across all orgs
reaper:lock            string  one reaper per round
```

Counts are denormalised from the sets deliberately: admission reads them on every
connect, and an O(1) `GET` beats an O(n) `SCARD` once an org holds thousands.

## Node death

Each node writes a timestamp to `nodes:alive` every `HEARTBEAT_SEC`; a reaper on
every node releases the sessions of nodes silent past `NODE_TIMEOUT_SEC`. Three
decisions worth defending:

- **The timeout is a tuning knob, not a correct answer.** A crashed node and a
  briefly unreachable one are indistinguishable from outside. You cannot detect
  death, only pick a silence threshold. Too low and a network blip reaps a healthy
  node; too high and ghosts hold quota.
- **A node never reaps itself.** A stale entry for its own id means its heartbeat
  is failing, but it is demonstrably alive — it is running the reaper. Treating
  itself as dead would drop working connections.
- **The reaper runs everywhere, behind a lock.** No singleton service to lose.
  `reap_node.lua` is idempotent, so a lost race is harmless; the lock just avoids
  duplicated work and alarming logs.

`reap_node.lua` verifies each session hash rather than trusting the node's session
set. The check looks redundant — release already removes the id — but the reaper
reads `SMEMBERS` once and then works the list, so a release landing in between
leaves it holding ids already accounted for. Counting those again drives the count
below the truth, and a count that reads low is free capacity nobody paid for.

## Fair sharing

Each org has a hard ceiling and a reserved floor. The rule is one line, and the
conjunction is the entire guarantee:

```lua
if global_count >= global_max and org_count >= floor then
  return 'GLOBAL_FULL'
end
```

Below its floor an org is admitted **even when the platform is full**, deliberately
overshooting the global cap to honour the promise. That is what stops a noisy
tenant locking out a paying one well under its own limit. Above the floor everyone
competes first-come.

Weighted proportional shares would be fairer under genuinely mixed load, but need
continuous recalculation as capacity shifts and are much harder to reason about.
Reserved floors are fifteen lines inside a script that had to exist anyway.

## Draining

On SIGTERM: set the drain flag in Redis, *then* close connections with `4504`.
Order is the correctness argument — the flag goes first, so `admit.lua` already
refuses new sessions for this node before any socket closes. The other order leaves
a window where a connection lands on a node that is shutting down.

**Sessions are not migrated.** The connection genuinely breaks and the client
genuinely reconnects. Real migration means serialising live state and handing it to
another node — substantially harder, and out of scope. Stating that beats implying
seamlessness.

## Choices a reviewer might question

**Django Channels.** The consumer lifecycle maps cleanly onto the problem and
Django brings the ORM and admin for free. It carries more per-connection weight
than bare Starlette or Go — so capacity claims here come from the load script, not
from assertion.

**uvicorn, not daphne.** Not preference. **daphne does not implement the ASGI
lifespan protocol at all**, so the heartbeat and reaper loops silently never
started and dead nodes were never reaped — with no error to show for it. Only the
multi-process chaos test caught this; unit tests drove the reaper directly and
stayed green.

**Async views.** Sync views bridging to the async registry via `asyncio.run()`
create a second event loop inside the running server and fail with "Future attached
to a different loop" the moment they touch the client shared with the consumers.
`/api/capacity` returned 500 on the real server until this was fixed.

**In-process consumer set, not a channel layer.** Drain only closes connections on
the node running it, so there is nothing to broadcast. A layer would add
serialisation to reach objects already in this process's memory.

**nginx `least_conn`, not `ip_hash`.** These are long-lived sessions of
unpredictable duration, so counting requests is the wrong measure — and pinning
clients to nodes would hide exactly what the chaos scripts need to show.

**Plain Django views, no DRF.** Three read-only JSON endpoints do not justify a
serialisation framework, a router and a viewset hierarchy.

## Known weaknesses

Named here because a design that hides its limits is harder to trust than one that
states them.

- **Redis is a single point of failure.** If it dies, no new connections are
  admitted; existing ones survive, being just sockets on the nodes. Production
  answer is Sentinel or Cluster. Not done here.
- **`release.lua` is single-instance only.** It derives key names from the session
  hash, so not every touched key is declared in `KEYS`, which Redis Cluster
  requires. Fix: hash-tag the keys and pass them explicitly.
- **Counts can drift.** A node dying between the increment and the session write
  leaks a slot. The reaper catches the common case; `manage.py reconcile` repairs
  the audit trail but deliberately never touches live counts. A full
  count-reconciliation pass is documented, not built.
- **`reap_node.lua` reaps a whole node in one script.** A node holding tens of
  thousands of sessions would block Redis for the duration. Fix: pass a batch size
  and loop.
- **Rejections write a Postgres row each.** Under a sustained flood that is a write
  per refused connect. Wants batching if it becomes real.
- **Capacity figures must be measured**, never asserted. Every number in this
  document came from a run.

## On testing

75 tests, real Redis throughout — what is under test is that Redis serialises a
script and that exactly one concurrent `DEL` wins, and a mock would only prove the
mock behaves as imagined.

Two tests turned out **vacuous and were rewritten**: the reap-gate test passed even
with the gate removed, and the org-limit cache was written by a signal but never
read, so enforcement silently used the Postgres row while every unit test stayed
green. Both were caught by end-to-end runs, not by the suite. Unit tests verify the
pieces; only multi-process runs verify the pieces are wired to each other.
