"""Admission control checks. The race test is the point of this file.

Real Redis, not a fake: what is being tested is that Redis serialises a Lua
script, and a mock would only prove the mock behaves how I imagined.

    pytest tests/test_admission.py -v
"""

import asyncio
import uuid

import pytest

from connsessions import registry

pytestmark = pytest.mark.asyncio

ORG = "pytest-org"
NODE = "pytest-node"


@pytest.fixture(autouse=True)
async def clean_keys():
    r = registry.get_redis()
    yield
    keys = []
    for pattern in ("session:*", "org:pytest*", "node:pytest*", "global:count"):
        keys.extend([k async for k in r.scan_iter(match=pattern)])
    if keys:
        await r.delete(*keys)


async def admit(org=ORG, node=NODE, limit=10, floor=0, global_max=1000, sid=None):
    """Try to admit one session. Returns the session id, or raises Rejected."""
    sid = sid or uuid.uuid4()
    await registry.admit_session(
        session_id=sid,
        org_slug=org,
        node_id=node,
        client_id="c",
        started_at="2026-01-01T00:00:00Z",
        max_sessions=limit,
        reserved_floor=floor,
        global_max=global_max,
    )
    return sid


async def try_admit(**kwargs):
    """Admit, returning the reason string instead of raising. For gather()."""
    try:
        await admit(**kwargs)
        return "OK"
    except registry.Rejected as exc:
        return exc.reason


# --- the per-org ceiling -----------------------------------------------------


async def test_admits_up_to_the_limit_then_rejects():
    for _ in range(3):
        await admit(limit=3)

    assert await registry.org_session_count(ORG) == 3

    with pytest.raises(registry.Rejected) as caught:
        await admit(limit=3)
    assert caught.value.reason == "ORG_LIMIT"

    # A rejection must not consume a slot.
    assert await registry.org_session_count(ORG) == 3


async def test_releasing_frees_a_slot_for_the_next_connection():
    sids = [await admit(limit=2) for _ in range(2)]
    with pytest.raises(registry.Rejected):
        await admit(limit=2)

    await registry.release_session(sids[0], ORG, NODE)
    await admit(limit=2)  # must now fit

    assert await registry.org_session_count(ORG) == 2


async def test_zero_limit_admits_nothing():
    with pytest.raises(registry.Rejected) as caught:
        await admit(limit=0)
    assert caught.value.reason == "ORG_LIMIT"


# --- the race: requirement 3 -------------------------------------------------


async def test_concurrent_admissions_never_exceed_the_limit():
    """The test this phase exists for.

    Fifty connections race for ten slots. Without the Lua script they would
    interleave -- each reads the count, each sees room, each increments -- and the
    limit would be breached. Redis runs the script start to finish without another
    client's commands in between, so exactly ten win.
    """
    limit = 10
    attempts = 50

    results = await asyncio.gather(
        *(try_admit(limit=limit) for _ in range(attempts))
    )

    admitted = results.count("OK")
    assert admitted == limit, f"expected exactly {limit} admitted, got {admitted}"
    assert results.count("ORG_LIMIT") == attempts - limit

    # The count must agree with the decisions, and the set with the count.
    assert await registry.org_session_count(ORG) == limit
    assert len(await registry.org_session_ids(ORG)) == limit


async def test_concurrent_admissions_across_several_nodes():
    """Same race, but arrivals spread over nodes -- the real topology.

    A per-node limit check could not catch this: each node would be well under
    the cap on its own.
    """
    limit = 12
    nodes = ["pytest-n1", "pytest-n2", "pytest-n3", "pytest-n4"]

    results = await asyncio.gather(
        *(try_admit(limit=limit, node=nodes[i % len(nodes)]) for i in range(60))
    )

    assert results.count("OK") == limit
    assert await registry.org_session_count(ORG) == limit

    # Every admitted session must be findable on the node that took it, or the
    # reaper would miss it when that node dies.
    total_on_nodes = 0
    for node in nodes:
        total_on_nodes += len(await registry.node_session_ids(node))
    assert total_on_nodes == limit


async def test_concurrent_release_and_admit_keeps_the_count_honest():
    """Churn: releases and admissions interleaved, count must stay consistent."""
    limit = 20
    sids = [await admit(limit=limit) for _ in range(10)]

    results = await asyncio.gather(
        *([registry.release_session(s, ORG, NODE) for s in sids]
          + [try_admit(limit=limit) for _ in range(10)])
    )

    released = sum(1 for r in results[:10] if r is True)
    admitted = results[10:].count("OK")

    expected = 10 - released + admitted
    assert await registry.org_session_count(ORG) == expected
    assert len(await registry.org_session_ids(ORG)) == expected


# --- the global ceiling and fairness: requirement 4 --------------------------


async def test_global_ceiling_rejects_when_platform_is_full():
    results = await asyncio.gather(
        *(try_admit(limit=100, floor=0, global_max=5) for _ in range(20))
    )
    assert results.count("OK") == 5
    assert results.count("GLOBAL_FULL") == 15
    assert await registry.global_session_count() == 5


async def test_reserved_floor_survives_a_noisy_neighbour():
    """Requirement 4, the case that motivates the floor.

    A greedy org fills the whole platform. A quiet org with a reserved floor must
    still be able to connect, because that capacity is guaranteed rather than
    first-come. Without the floor it would be locked out despite being far below
    its own limit.
    """
    global_max = 10

    # Greedy org takes everything available.
    for _ in range(global_max):
        await admit(org="pytest-greedy", limit=100, floor=0, global_max=global_max)
    assert await registry.global_session_count() == global_max

    # Platform is full, and an org with no floor is refused.
    with pytest.raises(registry.Rejected) as caught:
        await admit(org="pytest-nofloor", limit=50, floor=0, global_max=global_max)
    assert caught.value.reason == "GLOBAL_FULL"

    # But the protected org reaches its floor anyway -- global is deliberately
    # overshot to honour the guarantee.
    for _ in range(3):
        await admit(org="pytest-protected", limit=50, floor=3, global_max=global_max)

    assert await registry.org_session_count("pytest-protected") == 3
    assert await registry.global_session_count() == global_max + 3


async def test_floor_is_a_floor_not_an_allowance():
    """Above its floor, a protected org competes like everyone else."""
    global_max = 5
    for _ in range(global_max):
        await admit(org="pytest-greedy", limit=100, global_max=global_max)

    # Up to the floor: guaranteed.
    for _ in range(2):
        await admit(org="pytest-protected", limit=50, floor=2, global_max=global_max)

    # Past it, with the platform still full: refused.
    with pytest.raises(registry.Rejected) as caught:
        await admit(org="pytest-protected", limit=50, floor=2, global_max=global_max)
    assert caught.value.reason == "GLOBAL_FULL"


async def test_org_limit_binds_even_below_the_floor():
    """A floor above the ceiling must not grant capacity past the ceiling.

    Organization.clean() rejects this configuration, but admission must not rely
    on validation having run -- a fixture or a direct SQL update could bypass it.
    """
    for _ in range(2):
        await admit(limit=2, floor=5, global_max=1000)

    with pytest.raises(registry.Rejected) as caught:
        await admit(limit=2, floor=5, global_max=1000)
    assert caught.value.reason == "ORG_LIMIT"


async def test_concurrent_floor_claims_do_not_overshoot():
    """The floor guarantee must itself be race-safe."""
    global_max = 4
    for _ in range(global_max):
        await admit(org="pytest-greedy", limit=100, global_max=global_max)

    results = await asyncio.gather(
        *(try_admit(org="pytest-protected", limit=50, floor=3, global_max=global_max)
          for _ in range(20))
    )

    # Exactly the floor, no more: the platform is full above it.
    assert results.count("OK") == 3
    assert await registry.org_session_count("pytest-protected") == 3


# --- guards ------------------------------------------------------------------


async def test_duplicate_session_id_is_refused():
    """A retry must not increment the counters twice for one connection."""
    sid = await admit(limit=10)

    with pytest.raises(registry.Rejected) as caught:
        await admit(limit=10, sid=sid)
    assert caught.value.reason == "DUPLICATE_SESSION"
    assert await registry.org_session_count(ORG) == 1


async def test_draining_node_takes_no_new_sessions():
    await registry.mark_draining(NODE)
    try:
        with pytest.raises(registry.Rejected) as caught:
            await admit(limit=10)
        assert caught.value.reason == "NODE_DRAINING"
    finally:
        await registry.clear_draining(NODE)

    # Cleared: admissions resume.
    await admit(limit=10)
    assert await registry.org_session_count(ORG) == 1


async def test_cached_config_round_trips():
    """The consumer reads limits from this cache on every connect.

    Regression guard: an earlier version wrote the cache from a signal but read
    limits straight off the Postgres row, so the cache was dead code and a stale
    or differing cached limit was silently ignored. Enforcement and the cached
    value have to be the same number.
    """
    assert await registry.get_org_config("pytest-uncached") is None

    await registry.cache_org_config("pytest-org", 5, 2)
    cfg = await registry.get_org_config("pytest-org")
    assert cfg == {"max_sessions": 5, "reserved_floor": 2}

    # And the cached numbers are the ones admission actually enforces.
    results = await asyncio.gather(
        *(try_admit(limit=cfg["max_sessions"], floor=cfg["reserved_floor"])
          for _ in range(15))
    )
    assert results.count("OK") == 5

    await registry.invalidate_org_config("pytest-org")
    assert await registry.get_org_config("pytest-org") is None


async def test_orgs_are_isolated():
    await asyncio.gather(
        *([try_admit(org="pytest-a", limit=3) for _ in range(10)]
          + [try_admit(org="pytest-b", limit=5) for _ in range(10)])
    )

    assert await registry.org_session_count("pytest-a") == 3
    assert await registry.org_session_count("pytest-b") == 5
    assert await registry.global_session_count() == 8
