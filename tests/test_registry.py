"""Registry checks against a real Redis.

Real Redis, not a fake: the properties under test here (pipeline atomicity, that
exactly one concurrent DEL wins) are properties of Redis itself, and a mock would
only assert that the mock behaves how I imagined.

    pytest tests/test_registry.py
"""

import asyncio
import uuid

import pytest

from connsessions import registry

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
async def clean_keys():
    """Isolate each test, and leave nothing behind for the next one."""
    r = registry.get_redis()
    yield
    keys = []
    for pattern in ("session:*", "org:pytest*", "node:pytest*", "global:count"):
        keys.extend([k async for k in r.scan_iter(match=pattern)])
    if keys:
        await r.delete(*keys)


async def _register(org="pytest-org", node="pytest-node", sid=None):
    sid = sid or uuid.uuid4()
    await registry.register_session(sid, org, node, "client-1", "2026-01-01T00:00:00Z")
    return sid


async def test_register_makes_session_visible():
    sid = await _register()

    assert await registry.org_session_count("pytest-org") == 1
    assert await registry.global_session_count() == 1

    session = await registry.get_session(sid)
    assert session["org"] == "pytest-org"
    assert session["node"] == "pytest-node"

    # Both indexes must see it, or the reaper and the query API disagree.
    assert str(sid) in await registry.org_session_ids("pytest-org")
    assert str(sid) in await registry.node_session_ids("pytest-node")


async def test_release_frees_the_slot():
    sid = await _register()
    assert await registry.release_session(sid) is True

    assert await registry.org_session_count("pytest-org") == 0
    assert await registry.global_session_count() == 0
    assert await registry.get_session(sid) is None
    assert str(sid) not in await registry.org_session_ids("pytest-org")


async def test_double_release_does_not_double_decrement():
    """The one that matters: a corrupted count is a capacity bug.

    A session can plausibly be released twice -- client disconnects while the
    reaper is cleaning the same node. If both decrements landed, the count would
    drift below the truth and the org would get capacity it never paid for.
    """
    keep = await _register()
    victim = await _register()
    assert await registry.org_session_count("pytest-org") == 2

    assert await registry.release_session(victim) is True
    assert await registry.release_session(victim) is False  # second is a no-op

    # The surviving session must still be accounted for.
    assert await registry.org_session_count("pytest-org") == 1
    assert await registry.get_session(keep) is not None


async def test_concurrent_release_of_same_session_decrements_once():
    """Ten simultaneous releases, one slot freed.

    Exercises the DEL-as-gate: Redis runs each transaction without interleaving,
    so exactly one caller observes the hash and owns the decrement.
    """
    sid = await _register()
    for _ in range(4):
        await _register()
    assert await registry.org_session_count("pytest-org") == 5

    results = await asyncio.gather(
        *(registry.release_session(sid) for _ in range(10))
    )

    assert sum(results) == 1, f"expected exactly one winner, got {sum(results)}"
    assert await registry.org_session_count("pytest-org") == 4


async def test_release_unknown_session_is_a_noop():
    assert await registry.release_session(uuid.uuid4()) is False
    assert await registry.org_session_count("pytest-org") == 0


async def test_release_recovers_org_and_node_from_hash():
    """The reaper knows the node; a crashed-socket path may know neither."""
    sid = await _register()

    assert await registry.release_session(sid) is True  # no org/node passed

    assert await registry.org_session_count("pytest-org") == 0
    assert str(sid) not in await registry.node_session_ids("pytest-node")


async def test_counts_are_isolated_per_org():
    """Requirement 3 depends on this: one org's usage must not read as another's."""
    await _register(org="pytest-a")
    await _register(org="pytest-a")
    await _register(org="pytest-b")

    assert await registry.org_session_count("pytest-a") == 2
    assert await registry.org_session_count("pytest-b") == 1
    assert await registry.global_session_count() == 3
