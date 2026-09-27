"""Graceful drain. Requirement 5.

A node that is about to go away (deploy, scale-down) should not simply vanish
with its connections. Drain does two things in order: stop taking new sessions,
then close the live ones with a code that tells the client to reconnect -- the
load balancer sends the retry to a healthy node.

Sessions are NOT migrated. The connection genuinely breaks and the client
genuinely reconnects. Moving a live session would mean serialising its state and
handing it to another node, which is a substantially harder problem and out of
scope here. Saying so plainly beats implying seamlessness.

The live-consumer registry is a plain in-process set rather than a channel layer
group. Drain only ever closes connections on the node running it, so there is
nothing to broadcast: a channel layer would add serialisation and group
bookkeeping to reach objects already in this process's memory.
"""

import asyncio
import logging

from django.conf import settings

from . import registry

log = logging.getLogger(__name__)

# WebSocket close code meaning "this node is going away, reconnect and you will
# land somewhere else". Distinct from 4503 GLOBAL_FULL, which means the platform
# is out of capacity and retrying immediately will not help.
CLOSE_RECONNECT_ELSEWHERE = 4504

# Live consumers on this node. Consumers add themselves on connect and remove
# themselves on disconnect, so this is the set of sockets drain has to close.
_live_consumers = set()

_draining = False


def register_consumer(consumer):
    _live_consumers.add(consumer)


def unregister_consumer(consumer):
    _live_consumers.discard(consumer)


def live_count():
    return len(_live_consumers)


def is_draining():
    """Whether this process has begun draining.

    Checked locally as well as in Redis: the Redis flag is what makes the
    admission decision atomic across nodes, while this avoids a round-trip on a
    node that already knows it is going away.
    """
    return _draining


async def drain(batch_size=None, interval_ms=None, node_id=None):
    """Stop admitting, then close every live session. Returns how many were closed.

    Order matters. The drain flag goes into Redis first so admit.lua refuses new
    sessions for this node, and only then are existing sockets closed. The other
    order would let a connection arrive between the last close and the flag being
    set, leaving a session on a node that is shutting down.
    """
    global _draining

    node_id = node_id or settings.NODE_ID
    batch_size = settings.DRAIN_BATCH_SIZE if batch_size is None else batch_size
    interval_ms = settings.DRAIN_INTERVAL_MS if interval_ms is None else interval_ms

    _draining = True
    try:
        await registry.mark_draining(node_id)
    except Exception:
        # Redis is unreachable, so the flag cannot be set and other nodes cannot
        # see the drain. Carry on closing local sockets anyway: dropping them is
        # the point, and the local flag still stops this node admitting.
        log.exception("could not set drain flag node=%s; draining locally", node_id)

    total = len(_live_consumers)
    if not total:
        log.info("drain node=%s: no live sessions", node_id)
        return 0

    # 0 means all at once. Anything larger spreads the reconnects so a deploy does
    # not stampede the surviving nodes -- they are already carrying this node's
    # share of the load.
    effective_batch = batch_size if batch_size and batch_size > 0 else total

    log.info(
        "drain node=%s: closing %s sessions, batch=%s interval=%sms",
        node_id, total, effective_batch, interval_ms,
    )

    closed = 0
    while _live_consumers:
        # Snapshot: closing mutates the set as consumers unregister themselves.
        batch = list(_live_consumers)[:effective_batch]
        if not batch:
            break

        results = await asyncio.gather(
            *(_close_one(c) for c in batch), return_exceptions=True
        )
        closed += sum(1 for r in results if r is True)

        # Anything that failed to close must still leave the set, or this loops
        # forever on a socket that cannot be closed.
        for consumer in batch:
            unregister_consumer(consumer)

        if _live_consumers and interval_ms > 0:
            await asyncio.sleep(interval_ms / 1000)

    log.info("drain node=%s: closed %s of %s", node_id, closed, total)
    return closed


async def _close_one(consumer):
    """Close one connection, telling the client to reconnect elsewhere."""
    try:
        await consumer.close(code=CLOSE_RECONNECT_ELSEWHERE)
        return True
    except Exception:
        # Already gone, or the transport is broken. Either way the slot is freed
        # by the consumer's own disconnect, or by the reaper if this node dies.
        log.debug("drain: could not close a connection", exc_info=True)
        return False


def reset_for_tests():
    """Clear module state between tests."""
    global _draining
    _draining = False
    _live_consumers.clear()
