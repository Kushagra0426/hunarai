"""Redis-backed live session registry and admission control.

This is the authoritative answer to "what is connected right now". Nodes hold the
sockets; this holds the truth about them, because no node can be trusted to report
on sessions it lost when it died, and no node can see the others' tallies.

Admission lives in Lua (lua/admit.lua) because the check and the increment must
not interleave: two connections arriving simultaneously on different nodes would
otherwise both read "499 of 500" and both be admitted. Redis runs a script without
interleaving other clients, so they get serialised.

Key layout
----------
    session:{sid}            hash    org, node, client, started_at
    org:{slug}:sessions      set     session ids for this org
    node:{node}:sessions     set     session ids on this node -- what to reap
    org:{slug}:count         int     live count, authoritative for limits
    org:{slug}:cfg           hash    max_sessions, reserved_floor (cached)
    node:{node}:draining     flag    set during drain; blocks new admissions
    global:count             int     live count across all orgs

Counts are denormalised from the sets on purpose: admission reads them on every
connect, and an O(1) GET beats an O(n) SCARD once an org holds thousands.
"""

import logging
from pathlib import Path

from django.conf import settings
from redis import asyncio as aioredis

log = logging.getLogger(__name__)

LUA_DIR = Path(__file__).resolve().parent / "lua"


class Rejected(Exception):
    """Admission refused. Carries the machine-readable reason for the close code."""

    def __init__(self, reason, org_count=None, global_count=None):
        super().__init__(reason)
        self.reason = reason
        self.org_count = org_count
        self.global_count = global_count


# --- keys --------------------------------------------------------------------
# One place, so the Lua scripts, the consumer and the query API cannot drift.


def session_key(session_id):
    return f"session:{session_id}"


def org_sessions_key(org_slug):
    return f"org:{org_slug}:sessions"


def org_count_key(org_slug):
    return f"org:{org_slug}:count"


def org_cfg_key(org_slug):
    return f"org:{org_slug}:cfg"


def node_sessions_key(node_id):
    return f"node:{node_id}:sessions"


def node_draining_key(node_id):
    return f"node:{node_id}:draining"


def node_heartbeat_key(node_id):
    return f"node:{node_id}:hb"


GLOBAL_COUNT_KEY = "global:count"
NODES_ALIVE_KEY = "nodes:alive"
REAP_LOCK_KEY = "reaper:lock"


# --- client and scripts ------------------------------------------------------

_client = None
_scripts = {}


def get_redis():
    """Process-wide async Redis client.

    decode_responses so callers deal in str rather than bytes -- the Lua scripts
    return strings too, and mixing the two is a reliable source of silent
    comparison bugs.
    """
    global _client
    if _client is None:
        _client = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            health_check_interval=30,
        )
    return _client


def _script(name):
    """Register a Lua file, cached per process.

    redis-py's Script object uses EVALSHA and falls back to EVAL on NOSCRIPT, so
    the body crosses the wire once rather than on every connect -- and it
    recovers by itself if the server is restarted or its script cache flushed.
    """
    if name not in _scripts:
        body = (LUA_DIR / f"{name}.lua").read_text()
        _scripts[name] = get_redis().register_script(body)
    return _scripts[name]


async def close_redis():
    global _client, _scripts
    if _client is not None:
        await _client.aclose()
        _client = None
    # Scripts bind to the client that registered them, so drop them together.
    _scripts = {}


# --- org config --------------------------------------------------------------
# Limits live in Postgres (they are configuration) but admission needs them on
# every connect, so they are cached in Redis. Written through on save by a signal
# in models.py, so a limit change takes effect immediately.


async def cache_org_config(org_slug, max_sessions, reserved_floor):
    await get_redis().hset(
        org_cfg_key(org_slug),
        mapping={"max_sessions": int(max_sessions), "reserved_floor": int(reserved_floor)},
    )


async def get_org_config(org_slug):
    """Cached limits for an org, or None if not cached yet."""
    cfg = await get_redis().hmget(org_cfg_key(org_slug), "max_sessions", "reserved_floor")
    if cfg[0] is None:
        return None
    return {"max_sessions": int(cfg[0]), "reserved_floor": int(cfg[1] or 0)}


async def invalidate_org_config(org_slug):
    await get_redis().delete(org_cfg_key(org_slug))


# --- admission ---------------------------------------------------------------


async def admit_session(
    session_id,
    org_slug,
    node_id,
    client_id,
    started_at,
    max_sessions,
    reserved_floor,
    global_max=None,
):
    """Atomically check every limit and register the session if it fits.

    Raises Rejected with a reason when refused. Returns (org_count, global_count)
    as they stand after admitting.
    """
    sid = str(session_id)
    global_max = settings.GLOBAL_MAX_SESSIONS if global_max is None else global_max

    result = await _script("admit")(
        keys=[
            org_count_key(org_slug),
            GLOBAL_COUNT_KEY,
            org_sessions_key(org_slug),
            node_sessions_key(node_id),
            session_key(sid),
            node_draining_key(node_id),
        ],
        args=[
            sid,
            org_slug,
            node_id,
            client_id or "",
            int(max_sessions),
            int(reserved_floor),
            int(global_max),
            started_at,
        ],
    )

    status = result[0]
    if status == "OK":
        org_count, global_count = int(result[1]), int(result[2])
        log.info(
            "admitted session=%s org=%s node=%s org_count=%s global_count=%s",
            sid, org_slug, node_id, org_count, global_count,
        )
        return org_count, global_count

    reason, org_count, global_count = result[1], int(result[2]), int(result[3])
    log.info(
        "rejected session=%s org=%s reason=%s org_count=%s global_count=%s",
        sid, org_slug, reason, org_count, global_count,
    )
    raise Rejected(reason, org_count, global_count)


async def release_session(session_id, org_slug=None, node_id=None):
    """Release a session and free the slot. Idempotent.

    Returns True if this call was the one that released it. See lua/release.lua
    for why double-release must not double-decrement.
    """
    sid = str(session_id)

    result = await _script("release")(
        keys=[session_key(sid), org_count_key(org_slug or "_"), GLOBAL_COUNT_KEY],
        args=[sid, org_slug or "", node_id or ""],
    )

    if result[0] == "NOOP":
        log.debug("release session=%s: already released or unknown", sid)
        return False

    log.info(
        "released session=%s org=%s node=%s org_count=%s",
        sid, result[1], result[2], result[3],
    )
    return True


# --- liveness ----------------------------------------------------------------
# A node proves it is alive by writing a timestamp. Silence past a threshold is
# the only signal available that a node is gone -- a crashed process sends no
# goodbye, and a crashed node is indistinguishable from a briefly unreachable one
# from the outside. So NODE_TIMEOUT_SEC is a tuning knob, not a correct answer:
# too low and a network blip reaps a healthy node's sessions, too high and dead
# nodes hold quota. See settings.py.
#
# The timestamp is passed in rather than read from redis.call('TIME') inside Lua,
# which keeps the reaper testable against a fake clock.


def _now_ms():
    import time

    return int(time.time() * 1000)


async def heartbeat(node_id, now_ms=None):
    """Record that this node is alive, right now.

    Two writes, because they answer different questions: the sorted set is how
    the reaper finds stale nodes in one query, while the TTL key is a cheap
    per-node liveness check that expires on its own if the process dies.
    """
    now_ms = _now_ms() if now_ms is None else now_ms
    ttl = max(settings.NODE_TIMEOUT_SEC * 2, 10)

    async with get_redis().pipeline(transaction=True) as pipe:
        pipe.zadd(NODES_ALIVE_KEY, {node_id: now_ms})
        pipe.set(node_heartbeat_key(node_id), now_ms, ex=ttl)
        await pipe.execute()


async def alive_nodes():
    """Every node with a heartbeat on record, newest last."""
    return await get_redis().zrange(NODES_ALIVE_KEY, 0, -1)


async def stale_nodes(timeout_sec=None, now_ms=None):
    """Nodes whose last heartbeat is older than the timeout."""
    timeout_sec = settings.NODE_TIMEOUT_SEC if timeout_sec is None else timeout_sec
    now_ms = _now_ms() if now_ms is None else now_ms
    cutoff = now_ms - (timeout_sec * 1000)
    return await get_redis().zrangebyscore(NODES_ALIVE_KEY, "-inf", cutoff)


async def forget_node(node_id):
    """Remove a node's liveness records without touching its sessions.

    For a node that shut down cleanly and released its own sessions. Reaping such
    a node would be a no-op anyway, but leaving it in nodes:alive means the
    reaper keeps rediscovering it.
    """
    async with get_redis().pipeline(transaction=True) as pipe:
        pipe.zrem(NODES_ALIVE_KEY, node_id)
        pipe.delete(node_heartbeat_key(node_id))
        pipe.delete(node_draining_key(node_id))
        await pipe.execute()


# --- reaping -----------------------------------------------------------------


async def reap_node(node_id):
    """Release every session held by a dead node. Returns (total, {org: count}).

    Safe to call on a live node's id, and safe to call twice: sessions already
    released by their own consumer are skipped, because DEL on the session hash
    is the gate.
    """
    result = await _script("reap_node")(
        keys=[node_sessions_key(node_id), GLOBAL_COUNT_KEY, NODES_ALIVE_KEY],
        args=[node_id],
    )

    total = int(result[0])
    per_org = {}
    for i in range(1, len(result), 2):
        per_org[result[i]] = int(result[i + 1])

    if total:
        log.warning(
            "reaped node=%s sessions=%s per_org=%s", node_id, total, per_org
        )
    return total, per_org


async def acquire_reap_lock(ttl_sec=None):
    """Try to become the one node that reaps this round.

    Every node runs a reaper, so without this they would all reap the same dead
    node at once. The script is idempotent, so a lost race is harmless rather
    than corrupting -- but the lock keeps the work (and the log noise) to one
    node.

    The TTL is the safety valve: a node that dies holding the lock releases it by
    expiry rather than blocking reaping forever.
    """
    ttl_sec = ttl_sec or max(settings.REAP_INTERVAL_SEC * 2, 10)
    return bool(
        await get_redis().set(
            REAP_LOCK_KEY, settings.NODE_ID, nx=True, ex=ttl_sec
        )
    )


async def release_reap_lock():
    """Release the lock, but only if this node still holds it.

    Checked rather than deleted blindly: if this node stalled long enough for the
    lock to expire and another node to take it, deleting would hand a third node
    the lock while the second is mid-reap.
    """
    r = get_redis()
    if await r.get(REAP_LOCK_KEY) == settings.NODE_ID:
        await r.delete(REAP_LOCK_KEY)


# --- drain -------------------------------------------------------------------
# The flag is checked inside admit.lua so it is atomic with the increment: a node
# that begins draining mid-handshake cannot still pick up the session. The drain
# loop itself lands in phase 4.


async def mark_draining(node_id):
    await get_redis().set(node_draining_key(node_id), "1")


async def clear_draining(node_id):
    await get_redis().delete(node_draining_key(node_id))


async def is_draining(node_id):
    return bool(await get_redis().exists(node_draining_key(node_id)))


# --- queries -----------------------------------------------------------------
# Requirement 6: any node, or an external service, can answer these.


async def org_session_count(org_slug):
    value = await get_redis().get(org_count_key(org_slug))
    return int(value or 0)


async def global_session_count():
    value = await get_redis().get(GLOBAL_COUNT_KEY)
    return int(value or 0)


async def get_session(session_id):
    """Full detail for one session, including which node holds it."""
    data = await get_redis().hgetall(session_key(str(session_id)))
    return data or None


async def org_session_ids(org_slug):
    return await get_redis().smembers(org_sessions_key(org_slug))


async def node_session_ids(node_id):
    return await get_redis().smembers(node_sessions_key(node_id))
