"""Node liveness and orphan reaping. Requirement 2.

Time is injected rather than slept through: a heartbeat is a timestamp in a sorted
set, so "this node went quiet two minutes ago" is a number, not a wait. Tests that
sleep for real are slow and flaky for no gain in confidence.

    pytest tests/test_reaper.py -v
"""

import asyncio
import uuid

import pytest

from connsessions import registry

pytestmark = pytest.mark.asyncio

ORG = "pytest-org"
NOW = 1_700_000_000_000  # fixed fake clock, ms


@pytest.fixture(autouse=True)
async def clean_keys():
    r = registry.get_redis()
    yield
    keys = []
    patterns = (
        "session:*", "org:pytest*", "node:pytest*",
        "global:count", "nodes:alive", "reaper:lock",
    )
    for pattern in patterns:
        keys.extend([k async for k in r.scan_iter(match=pattern)])
    if keys:
        await r.delete(*keys)


async def admit(org=ORG, node="pytest-node", limit=1000):
    sid = uuid.uuid4()
    await registry.admit_session(
        session_id=sid,
        org_slug=org,
        node_id=node,
        client_id="c",
        started_at="2026-01-01T00:00:00Z",
        max_sessions=limit,
        reserved_floor=0,
        global_max=100_000,
    )
    return sid


# --- heartbeats --------------------------------------------------------------


async def test_heartbeat_registers_the_node():
    await registry.heartbeat("pytest-n1", now_ms=NOW)
    assert "pytest-n1" in await registry.alive_nodes()


async def test_fresh_node_is_not_stale():
    await registry.heartbeat("pytest-n1", now_ms=NOW)

    # Checked one second later, with a fifteen second timeout.
    stale = await registry.stale_nodes(timeout_sec=15, now_ms=NOW + 1_000)
    assert stale == []


async def test_silent_node_becomes_stale_past_the_timeout():
    await registry.heartbeat("pytest-n1", now_ms=NOW)

    # Just inside the window: still alive.
    assert await registry.stale_nodes(timeout_sec=15, now_ms=NOW + 14_000) == []

    # Past it: stale.
    assert await registry.stale_nodes(timeout_sec=15, now_ms=NOW + 16_000) == [
        "pytest-n1"
    ]


async def test_heartbeat_refreshes_and_clears_staleness():
    await registry.heartbeat("pytest-n1", now_ms=NOW)
    assert await registry.stale_nodes(timeout_sec=10, now_ms=NOW + 20_000)

    # The node comes back and beats again.
    await registry.heartbeat("pytest-n1", now_ms=NOW + 20_000)
    assert await registry.stale_nodes(timeout_sec=10, now_ms=NOW + 25_000) == []


async def test_only_the_silent_node_is_reported_stale():
    await registry.heartbeat("pytest-dead", now_ms=NOW)
    await registry.heartbeat("pytest-live", now_ms=NOW + 30_000)

    stale = await registry.stale_nodes(timeout_sec=15, now_ms=NOW + 31_000)
    assert stale == ["pytest-dead"]


# --- reaping: the requirement ------------------------------------------------


async def test_reaping_frees_every_slot_the_node_held():
    """The whole point: a dead node's sessions stop counting against the org."""
    for _ in range(5):
        await admit(node="pytest-dead")
    assert await registry.org_session_count(ORG) == 5
    assert await registry.global_session_count() == 5

    reaped, per_org = await registry.reap_node("pytest-dead")

    assert reaped == 5
    assert per_org == {ORG: 5}
    assert await registry.org_session_count(ORG) == 0
    assert await registry.global_session_count() == 0
    assert await registry.org_session_ids(ORG) == set()


async def test_reaping_leaves_other_nodes_alone():
    """A node dying must not disturb sessions living elsewhere."""
    for _ in range(3):
        await admit(node="pytest-dead")
    survivors = [await admit(node="pytest-live") for _ in range(4)]

    reaped, _ = await registry.reap_node("pytest-dead")

    assert reaped == 3
    assert await registry.org_session_count(ORG) == 4
    for sid in survivors:
        assert await registry.get_session(sid) is not None


async def test_reaping_splits_counts_across_orgs():
    """One node holds sessions for several tenants; each gets its own back."""
    for _ in range(2):
        await admit(org="pytest-a", node="pytest-dead")
    for _ in range(3):
        await admit(org="pytest-b", node="pytest-dead")

    reaped, per_org = await registry.reap_node("pytest-dead")

    assert reaped == 5
    assert per_org == {"pytest-a": 2, "pytest-b": 3}
    assert await registry.org_session_count("pytest-a") == 0
    assert await registry.org_session_count("pytest-b") == 0


async def test_reaping_a_node_with_no_sessions_is_harmless():
    reaped, per_org = await registry.reap_node("pytest-empty")
    assert reaped == 0
    assert per_org == {}


async def test_reaping_twice_does_not_double_decrement():
    """Idempotent, same as release: a second pass must find nothing to do."""
    for _ in range(4):
        await admit(node="pytest-dead")
    others = [await admit(node="pytest-live") for _ in range(2)]

    first, _ = await registry.reap_node("pytest-dead")
    second, _ = await registry.reap_node("pytest-dead")

    assert (first, second) == (4, 0)
    assert await registry.org_session_count(ORG) == 2
    assert len(others) == 2


async def test_reaping_skips_sessions_already_released():
    """The overlap case, and it is the normal one rather than exotic.

    A client disconnects at the moment its node is declared dead. The consumer
    releases that session and the reaper must not count it a second time -- doing
    so would push the org's count below the truth and hand it free capacity.
    """
    sids = [await admit(node="pytest-dead") for _ in range(5)]
    assert await registry.org_session_count(ORG) == 5

    # Two clients got their disconnects in first.
    await registry.release_session(sids[0], ORG, "pytest-dead")
    await registry.release_session(sids[1], ORG, "pytest-dead")
    assert await registry.org_session_count(ORG) == 3

    reaped, _ = await registry.reap_node("pytest-dead")

    assert reaped == 3, "must only reap what was still live"
    assert await registry.org_session_count(ORG) == 0


async def test_concurrent_reap_and_release_keeps_the_count_honest():
    """Reap racing the consumers' own releases, on the same sessions."""
    sids = [await admit(node="pytest-dead") for _ in range(10)]

    results = await asyncio.gather(
        registry.reap_node("pytest-dead"),
        *(registry.release_session(s, ORG, "pytest-dead") for s in sids),
    )

    reaped = results[0][0]
    released = sum(1 for r in results[1:] if r is True)

    # Every session accounted for exactly once, whoever got there first.
    assert reaped + released == 10, f"reaped={reaped} released={released}"
    assert await registry.org_session_count(ORG) == 0
    assert await registry.global_session_count() == 0


async def test_reaping_ignores_stale_ids_left_in_the_node_set():
    """Why reap_node verifies each session hash instead of trusting the set.

    On the ordinary path release.lua removes the id from the node set, so the
    reaper never sees a released session and the check looks redundant. It is not:
    the reaper reads SMEMBERS once and then works through the list, so a release
    landing in between leaves it holding ids whose sessions are already gone and
    already accounted for.

    This reconstructs that window directly -- sessions deleted and counted down,
    ids still in the node set. Counting those again would drive the org's count
    below the truth, and a count that reads low is free capacity nobody paid for.
    """
    sids = [await admit(node="pytest-dead") for _ in range(10)]
    r = registry.get_redis()

    # Four sessions released, but their ids still in the node set -- exactly what
    # a reaper that snapshotted SMEMBERS a moment earlier still holds.
    for sid in sids[:4]:
        await r.delete(registry.session_key(sid))
        await r.srem(registry.org_sessions_key(ORG), str(sid))
        await r.decr(registry.org_count_key(ORG))
        await r.decr(registry.GLOBAL_COUNT_KEY)

    assert await registry.org_session_count(ORG) == 6
    assert len(await registry.node_session_ids("pytest-dead")) == 10

    reaped, per_org = await registry.reap_node("pytest-dead")

    assert reaped == 6, "must count only sessions that were still live"
    assert per_org == {ORG: 6}
    assert await registry.org_session_count(ORG) == 0
    assert await registry.global_session_count() == 0


async def test_reaping_removes_the_node_from_liveness():
    """Otherwise every subsequent pass rediscovers the same corpse."""
    await registry.heartbeat("pytest-dead", now_ms=NOW)
    await admit(node="pytest-dead")

    await registry.reap_node("pytest-dead")

    assert "pytest-dead" not in await registry.alive_nodes()
    assert await registry.stale_nodes(timeout_sec=1, now_ms=NOW + 99_000) == []


# --- the reaper lock ---------------------------------------------------------


async def test_reap_lock_admits_one_holder():
    assert await registry.acquire_reap_lock(ttl_sec=5) is True
    assert await registry.acquire_reap_lock(ttl_sec=5) is False

    await registry.release_reap_lock()
    assert await registry.acquire_reap_lock(ttl_sec=5) is True


async def test_concurrent_lock_attempts_yield_exactly_one_winner():
    """Every node runs a reaper, so they all try this at once."""
    results = await asyncio.gather(
        *(registry.acquire_reap_lock(ttl_sec=5) for _ in range(20))
    )
    assert sum(results) == 1, f"expected one winner, got {sum(results)}"


# --- the full pass -----------------------------------------------------------


async def test_reap_once_reaps_only_stale_nodes(settings):
    """End to end through reap_once: stale node cleaned, live node untouched."""
    from connsessions import tasks

    settings.NODE_TIMEOUT_SEC = 15
    settings.NODE_ID = "pytest-self"

    # A node that went quiet, and one that is beating normally.
    await registry.heartbeat("pytest-dead", now_ms=registry._now_ms() - 60_000)
    await registry.heartbeat("pytest-live", now_ms=registry._now_ms())

    for _ in range(3):
        await admit(node="pytest-dead")
    for _ in range(2):
        await admit(node="pytest-live")
    assert await registry.org_session_count(ORG) == 5

    reaped = await tasks.reap_once()

    assert reaped == 3
    assert await registry.org_session_count(ORG) == 2
    assert "pytest-live" in await registry.alive_nodes()
    assert "pytest-dead" not in await registry.alive_nodes()


async def test_reap_once_never_reaps_itself(settings):
    """A node whose own heartbeat is failing must not release its live sessions.

    It is running the code, so it is demonstrably alive; treating itself as dead
    would drop working connections.
    """
    from connsessions import tasks

    settings.NODE_TIMEOUT_SEC = 15
    settings.NODE_ID = "pytest-self"

    await registry.heartbeat("pytest-self", now_ms=registry._now_ms() - 60_000)
    for _ in range(3):
        await admit(node="pytest-self")

    reaped = await tasks.reap_once()

    assert reaped == 0
    assert await registry.org_session_count(ORG) == 3


async def test_reap_once_is_a_noop_when_nothing_is_stale(settings):
    from connsessions import tasks

    settings.NODE_TIMEOUT_SEC = 15
    await registry.heartbeat("pytest-live", now_ms=registry._now_ms())
    await admit(node="pytest-live")

    assert await tasks.reap_once() == 0
    assert await registry.org_session_count(ORG) == 1


async def test_reap_once_yields_to_the_lock_holder(settings):
    """A second node's pass stands down rather than duplicating the work."""
    from connsessions import tasks

    settings.NODE_TIMEOUT_SEC = 15
    settings.NODE_ID = "pytest-self"

    await registry.heartbeat("pytest-dead", now_ms=registry._now_ms() - 60_000)
    await admit(node="pytest-dead")

    # Another node is already reaping.
    r = registry.get_redis()
    await r.set(registry.REAP_LOCK_KEY, "pytest-other", ex=30)

    assert await tasks.reap_once() == 0
    assert await registry.org_session_count(ORG) == 1  # untouched

    await r.delete(registry.REAP_LOCK_KEY)
    assert await tasks.reap_once() == 1  # now it proceeds


async def test_forget_node_leaves_sessions_alone(settings):
    """Clean shutdown path: drop liveness records without reaping."""
    await registry.heartbeat("pytest-gone", now_ms=NOW)
    sid = await admit(node="pytest-gone")

    await registry.forget_node("pytest-gone")

    assert "pytest-gone" not in await registry.alive_nodes()
    assert await registry.get_session(sid) is not None
    assert await registry.org_session_count(ORG) == 1
