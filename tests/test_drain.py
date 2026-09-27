"""Graceful drain. Requirement 5.

    pytest tests/test_drain.py -v
"""

import asyncio
import uuid

import pytest

from connsessions import drain, registry

pytestmark = pytest.mark.asyncio

NODE = "pytest-node"


@pytest.fixture(autouse=True)
async def clean():
    drain.reset_for_tests()
    r = registry.get_redis()
    yield
    drain.reset_for_tests()
    keys = []
    for pattern in ("session:*", "org:pytest*", "node:pytest*", "global:count"):
        keys.extend([k async for k in r.scan_iter(match=pattern)])
    if keys:
        await r.delete(*keys)


class FakeConsumer:
    """Stands in for a live connection.

    Only close() matters to drain, and the real consumer's close() goes through
    the ASGI send channel -- which would need a whole transport to exercise. What
    is under test here is drain's ordering and pacing, not Channels' socket
    handling.
    """

    def __init__(self, fail=False):
        self.closed_with = None
        self.fail = fail

    async def close(self, code=None):
        if self.fail:
            raise RuntimeError("socket already gone")
        self.closed_with = code
        drain.unregister_consumer(self)


def add_consumers(n, fail=False):
    consumers = [FakeConsumer(fail=fail) for _ in range(n)]
    for c in consumers:
        drain.register_consumer(c)
    return consumers


# --- the basics --------------------------------------------------------------


async def test_drain_closes_every_live_session():
    consumers = add_consumers(5)

    closed = await drain.drain(node_id=NODE)

    assert closed == 5
    assert drain.live_count() == 0
    for c in consumers:
        assert c.closed_with == drain.CLOSE_RECONNECT_ELSEWHERE


async def test_drain_uses_a_reconnect_code_not_a_generic_close():
    """The client has to be able to tell "move elsewhere" from "server broke"."""
    consumers = add_consumers(1)
    await drain.drain(node_id=NODE)
    assert consumers[0].closed_with == 4504


async def test_drain_with_nothing_live_is_a_noop():
    assert await drain.drain(node_id=NODE) == 0


async def test_drain_sets_the_flag_before_closing_anything():
    """Ordering is the whole correctness argument.

    If sockets were closed first, a connection arriving between the last close and
    the flag being set would land on a node that is shutting down.
    """
    observed = []

    class Watcher(FakeConsumer):
        async def close(self, code=None):
            observed.append(await registry.is_draining(NODE))
            await super().close(code)

    for _ in range(3):
        drain.register_consumer(Watcher())

    await drain.drain(node_id=NODE)

    assert observed == [True, True, True], "flag must be set before any close"


async def test_draining_node_refuses_new_sessions():
    """The flag has to actually stop admission, not just be set."""
    await drain.drain(node_id=NODE)

    with pytest.raises(registry.Rejected) as caught:
        await registry.admit_session(
            uuid.uuid4(), "pytest-org", NODE, "c", "t", 100, 0, 1000
        )
    assert caught.value.reason == "NODE_DRAINING"


async def test_drain_reports_itself_locally_without_a_redis_round_trip():
    assert drain.is_draining() is False
    await drain.drain(node_id=NODE)
    assert drain.is_draining() is True


# --- pacing ------------------------------------------------------------------


async def test_batch_size_zero_closes_everything_at_once():
    """The default. Simplest behaviour, and what the user asked for."""
    add_consumers(10)
    closed = await drain.drain(node_id=NODE, batch_size=0, interval_ms=0)
    assert closed == 10


async def test_batching_closes_in_waves():
    """Pacing exists so a deploy does not stampede the surviving nodes.

    They are already absorbing this node's share of the load, so handing them
    every reconnect in the same instant is the worst moment to do it.
    """
    add_consumers(10)

    waves = []
    original = drain._close_one

    async def counting_close(consumer):
        waves.append(drain.live_count())
        return await original(consumer)

    drain._close_one = counting_close
    try:
        closed = await drain.drain(node_id=NODE, batch_size=3, interval_ms=1)
    finally:
        drain._close_one = original

    assert closed == 10
    assert drain.live_count() == 0
    # Batches of three: the live count at the start of each close should step down
    # in threes rather than all being observed at 10.
    assert len(set(waves)) > 1, "expected several waves, not one"


async def test_pacing_interval_is_respected():
    add_consumers(6)

    loop = asyncio.get_running_loop()
    started = loop.time()
    await drain.drain(node_id=NODE, batch_size=2, interval_ms=50)
    elapsed = loop.time() - started

    # Three batches, two gaps between them: at least ~100ms.
    assert elapsed >= 0.09, f"expected pacing to take time, took {elapsed:.3f}s"


# --- failure handling --------------------------------------------------------


async def test_a_socket_that_will_not_close_does_not_hang_the_drain():
    """Otherwise a single broken transport blocks the deploy forever."""
    add_consumers(3, fail=True)

    closed = await asyncio.wait_for(drain.drain(node_id=NODE), timeout=5)

    assert closed == 0          # none closed cleanly
    assert drain.live_count() == 0   # but all removed, so the loop terminated


async def test_mixed_success_and_failure_still_drains_fully():
    good = add_consumers(4)
    add_consumers(2, fail=True)

    closed = await asyncio.wait_for(drain.drain(node_id=NODE), timeout=5)

    assert closed == 4
    assert drain.live_count() == 0
    for c in good:
        assert c.closed_with == drain.CLOSE_RECONNECT_ELSEWHERE


async def test_drain_proceeds_when_redis_is_unreachable(monkeypatch):
    """Local sockets still get closed: dropping them is the point.

    A node that cannot reach Redis cannot serve sessions either, so refusing to
    drain would leave connections hanging on a node that is going away regardless.
    """
    consumers = add_consumers(3)

    async def boom(node_id):
        raise ConnectionError("redis down")

    monkeypatch.setattr(registry, "mark_draining", boom)

    closed = await drain.drain(node_id=NODE)

    assert closed == 3
    for c in consumers:
        assert c.closed_with == drain.CLOSE_RECONNECT_ELSEWHERE


# --- interaction with the registry -------------------------------------------


async def test_clearing_the_flag_lets_the_node_admit_again():
    """A restarted node keeps its id under compose, so a stale flag would make it
    come back up refusing everything."""
    await drain.drain(node_id=NODE)
    await registry.clear_draining(NODE)

    await registry.admit_session(
        uuid.uuid4(), "pytest-org", NODE, "c", "t", 100, 0, 1000
    )
    assert await registry.org_session_count("pytest-org") == 1


async def test_draining_one_node_leaves_others_admitting():
    """A deploy rolls one node at a time; the rest must keep serving."""
    await drain.drain(node_id="pytest-dying")

    await registry.admit_session(
        uuid.uuid4(), "pytest-org", "pytest-healthy", "c", "t", 100, 0, 1000
    )
    assert await registry.org_session_count("pytest-org") == 1

    with pytest.raises(registry.Rejected):
        await registry.admit_session(
            uuid.uuid4(), "pytest-org", "pytest-dying", "c", "t", 100, 0, 1000
        )
