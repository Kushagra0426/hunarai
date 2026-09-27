"""Redis-backed live session registry.

This is the authoritative answer to "what is connected right now". Nodes hold
the sockets; this holds the truth about them, because no node can be trusted to
report on sessions it lost when it died, and no node can see the others' tallies.

Phase 1 is bookkeeping only: register on connect, release on disconnect, and
answer queries. Admission (per-org caps, the reserved floor, the global ceiling)
arrives in phase 2 as a Lua script so the check and the increment cannot
interleave across nodes.

Key layout
----------
    session:{sid}            hash    org, node, client, started_at
    org:{slug}:sessions      set     session ids for this org
    node:{node}:sessions     set     session ids on this node -- what to reap
    org:{slug}:count         int     live count, authoritative for limits
    global:count             int     live count across all orgs

The counts are denormalised from the sets on purpose: enforcement reads them on
every single connect, and an O(1) GET beats an O(n) SCARD once an org holds
thousands of sessions.
"""

import logging

from django.conf import settings
from redis import asyncio as aioredis

log = logging.getLogger(__name__)

# --- keys --------------------------------------------------------------------
# One place, so the Lua scripts in phase 2 and the query API in phase 4 cannot
# drift from what the consumer writes.


def session_key(session_id):
    return f"session:{session_id}"


def org_sessions_key(org_slug):
    return f"org:{org_slug}:sessions"


def org_count_key(org_slug):
    return f"org:{org_slug}:count"


def node_sessions_key(node_id):
    return f"node:{node_id}:sessions"


GLOBAL_COUNT_KEY = "global:count"


# --- client ------------------------------------------------------------------

_client = None


def get_redis():
    """Process-wide async Redis client.

    One connection pool per process, created lazily. decode_responses so callers
    deal in str rather than bytes -- the Lua scripts return strings too, and
    mixing the two is a reliable source of silent comparison bugs.
    """
    global _client
    if _client is None:
        _client = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            health_check_interval=30,
        )
    return _client


async def close_redis():
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


# --- lifecycle ---------------------------------------------------------------


async def register_session(session_id, org_slug, node_id, client_id, started_at):
    """Record a session as live. Phase 1: unconditional, no limit check.

    Every write goes in one pipeline so a session never exists in the set
    without its hash, or counts without membership. This is atomic in the sense
    that matters here -- Redis runs the whole transaction without interleaving
    another client's commands.

    Returns the org's live count after registering.
    """
    r = get_redis()
    sid = str(session_id)

    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(
            session_key(sid),
            mapping={
                "session_id": sid,
                "org": org_slug,
                "node": node_id,
                "client": client_id or "",
                "started_at": started_at,
            },
        )
        pipe.sadd(org_sessions_key(org_slug), sid)
        pipe.sadd(node_sessions_key(node_id), sid)
        pipe.incr(org_count_key(org_slug))
        pipe.incr(GLOBAL_COUNT_KEY)
        results = await pipe.execute()

    org_count = results[3]
    log.info(
        "registered session=%s org=%s node=%s org_count=%s",
        sid, org_slug, node_id, org_count,
    )
    return org_count


async def release_session(session_id, org_slug=None, node_id=None):
    """Release a session and free the slot it held.

    Idempotent, and that matters more than it looks: a session can plausibly be
    released twice -- the client disconnects while the reaper is already
    cleaning up the node, say -- and a double decrement would corrupt the count
    that phase 2 enforces limits against. Corrupted low, an org gets free
    capacity; corrupted high, a paying org is locked out.

    So deletion of the session hash is the gate. Exactly one caller wins the
    DEL, and only that caller touches the counters. Redis executes this while
    nothing else runs, so two concurrent releases cannot both observe 1.

    Returns True if this call was the one that released it.
    """
    r = get_redis()
    sid = str(session_id)

    # The caller may not know the org/node (the reaper does, a crashed-socket
    # handler might not), so recover them from the hash.
    if org_slug is None or node_id is None:
        stored = await r.hmget(session_key(sid), "org", "node")
        org_slug = org_slug or stored[0]
        node_id = node_id or stored[1]

    if not org_slug:
        # No hash and no hint: already gone.
        log.debug("release session=%s: not found, nothing to do", sid)
        return False

    async with r.pipeline(transaction=True) as pipe:
        pipe.delete(session_key(sid))
        pipe.srem(org_sessions_key(org_slug), sid)
        if node_id:
            pipe.srem(node_sessions_key(node_id), sid)
        results = await pipe.execute()

    existed = bool(results[0])
    if not existed:
        log.debug("release session=%s: already released", sid)
        return False

    # Won the race for this session, so this call owns the decrement.
    # ponytail: floor-at-zero guard, because a count that drifts below zero
    # would hand an org unlimited capacity. Phase 2 folds this into admit.lua.
    async with r.pipeline(transaction=True) as pipe:
        pipe.decr(org_count_key(org_slug))
        pipe.decr(GLOBAL_COUNT_KEY)
        org_count, global_count = await pipe.execute()

    if org_count < 0:
        log.warning("org=%s count went negative (%s), clamping", org_slug, org_count)
        await r.set(org_count_key(org_slug), 0)
    if global_count < 0:
        log.warning("global count went negative (%s), clamping", global_count)
        await r.set(GLOBAL_COUNT_KEY, 0)

    log.info("released session=%s org=%s node=%s", sid, org_slug, node_id)
    return True


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
