"""Background loops: prove this node is alive, and clean up after ones that are not.

Started from the ASGI lifespan (connmgr/asgi.py) so they share the process with
the consumers and stop with it. No Celery, no separate worker: these are two
timers against Redis, and a broker would be infrastructure to operate for no gain.

The reaper runs on every node rather than as a singleton service. That means no
special node to lose -- whoever is up does the work -- at the cost of needing a
lock so they do not all reap the same corpse at once.
"""

import asyncio
import logging

from django.conf import settings
from django.utils import timezone

from . import registry

log = logging.getLogger(__name__)


async def heartbeat_loop(stop_event):
    """Write this node's heartbeat until asked to stop.

    Writes once immediately: a node that took connections but had not yet
    heartbeated would look stale to the reaper, and its live sessions would be
    released underneath it.
    """
    node_id = settings.NODE_ID
    interval = settings.HEARTBEAT_SEC

    try:
        await registry.heartbeat(node_id)
        log.info("heartbeat started node=%s interval=%ss", node_id, interval)
    except Exception:
        log.exception("initial heartbeat failed node=%s", node_id)

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            break  # stop_event fired
        except TimeoutError:
            pass  # the normal path: interval elapsed

        try:
            await registry.heartbeat(node_id)
        except Exception:
            # Keep looping. A transient Redis failure means a missed beat, and
            # missing several means this node gets reaped -- which is the correct
            # outcome if it genuinely cannot reach Redis, since it cannot serve
            # sessions either.
            log.exception("heartbeat failed node=%s", node_id)

    log.info("heartbeat stopped node=%s", node_id)


async def reaper_loop(stop_event):
    """Find nodes that stopped heartbeating and release their sessions."""
    interval = settings.REAP_INTERVAL_SEC
    log.info(
        "reaper started interval=%ss node_timeout=%ss",
        interval, settings.NODE_TIMEOUT_SEC,
    )

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            break
        except TimeoutError:
            pass

        try:
            await reap_once()
        except Exception:
            log.exception("reaper pass failed")

    log.info("reaper stopped")


async def reap_once():
    """One reaper pass. Returns the number of sessions released.

    Split out from the loop so it can be called directly by tests and by the
    chaos scripts, with no waiting around for a timer.
    """
    stale = await registry.stale_nodes()
    if not stale:
        return 0

    # Only one node needs to do this. The reap script is idempotent so a lost
    # race is harmless, but duplicated work and duplicated alarming log lines are
    # worth avoiding.
    if not await registry.acquire_reap_lock():
        log.debug("reaper: another node holds the lock, skipping")
        return 0

    total = 0
    try:
        for node_id in stale:
            if node_id == settings.NODE_ID:
                # This node is alive -- it is running this code. A stale entry for
                # itself means its own heartbeat is failing, so reaping its live
                # sessions would be self-harm.
                log.warning("reaper: own heartbeat is stale, not reaping self")
                continue

            reaped, per_org = await registry.reap_node(node_id)
            total += reaped
            if reaped:
                await _mark_records_lost(node_id)
    finally:
        await registry.release_reap_lock()

    return total


async def _mark_records_lost(node_id):
    """Close the audit rows for sessions that died with their node.

    Best-effort and deliberately separate from the Redis reap: the slot is already
    freed, and a Postgres hiccup must not stop capacity being reclaimed. Worst
    case a row keeps ended_at NULL and looks live in history while the live count
    is correct.
    """
    from asgiref.sync import sync_to_async

    from .models import SessionRecord

    try:
        updated = await sync_to_async(
            SessionRecord.objects.filter(
                node_id=node_id, ended_at__isnull=True
            ).update
        )(ended_at=timezone.now(), end_reason=SessionRecord.EndReason.NODE_LOST)
        if updated:
            log.info("marked %s audit rows node_lost node=%s", updated, node_id)
    except Exception:
        log.exception("could not mark audit rows for node=%s", node_id)
