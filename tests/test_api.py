"""Query API. Requirement 6: any node answers, not just the one holding the
session.

The views are async (they share the app's Redis client, so bridging with
asyncio.run would fail with "Future attached to a different loop"), so these
drive them with Django's async test client.

    pytest tests/test_api.py -v
"""

import uuid

import pytest

from connsessions import registry
from connsessions.models import Organization

pytestmark = [pytest.mark.django_db(transaction=True), pytest.mark.asyncio]


async def admit(org="acme", node="pytest-n1", limit=100, floor=10):
    """Put a live session in Redis."""
    sid = uuid.uuid4()
    await registry.admit_session(
        session_id=sid,
        org_slug=org,
        node_id=node,
        client_id="c",
        started_at="2026-01-01T00:00:00Z",
        max_sessions=limit,
        reserved_floor=floor,
        global_max=10_000,
    )
    return sid


async def _wipe():
    r = registry.get_redis()
    keys = []
    patterns = (
        "session:*", "org:*", "node:*", "global:count",
        "nodes:alive", "reaper:lock",
    )
    for pattern in patterns:
        keys.extend([k async for k in r.scan_iter(match=pattern)])
    if keys:
        await r.delete(*keys)


@pytest.fixture(autouse=True)
async def clean_redis():
    await _wipe()
    yield
    await _wipe()


@pytest.fixture
async def orgs(db):
    return [
        await Organization.objects.acreate(
            slug="acme", name="Acme Corp", max_sessions=100, reserved_floor=10
        ),
        await Organization.objects.acreate(
            slug="globex", name="Globex", max_sessions=50, reserved_floor=5
        ),
    ]


# --- health ------------------------------------------------------------------


async def test_health_names_the_answering_node(async_client, settings):
    settings.NODE_ID = "pytest-n1"
    response = await async_client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["node_id"] == "pytest-n1"


async def test_draining_node_reports_unhealthy(async_client):
    """So the load balancer stops sending it new connections."""
    from connsessions import drain

    drain.reset_for_tests()
    await drain.drain(node_id="pytest-n1")
    try:
        response = await async_client.get("/health")
        assert response.status_code == 503
        assert response.json()["status"] == "draining"
    finally:
        drain.reset_for_tests()


# --- org sessions ------------------------------------------------------------


async def test_org_sessions_reports_usage_against_the_limit(async_client, orgs):
    for _ in range(3):
        await admit(org="acme")

    body = (await async_client.get("/api/orgs/acme/sessions")).json()

    assert body["org"] == "acme"
    assert body["sessions"] == 3
    assert body["limit"] == 100
    assert body["reserved_floor"] == 10
    assert body["available"] == 97


async def test_org_sessions_breaks_down_by_node(async_client, orgs):
    """The breakdown is what shows the balancer spreading an org across nodes."""
    for _ in range(2):
        await admit(org="acme", node="pytest-n1")
    for _ in range(3):
        await admit(org="acme", node="pytest-n2")

    body = (await async_client.get("/api/orgs/acme/sessions")).json()

    assert body["sessions"] == 5
    assert body["by_node"] == {"pytest-n1": 2, "pytest-n2": 3}


async def test_org_sessions_is_zero_not_missing_when_idle(async_client, orgs):
    body = (await async_client.get("/api/orgs/acme/sessions")).json()
    assert body["sessions"] == 0
    assert body["by_node"] == {}


async def test_unknown_org_is_404(async_client, orgs):
    assert (await async_client.get("/api/orgs/nosuchorg/sessions")).status_code == 404


async def test_one_org_usage_does_not_leak_into_another(async_client, orgs):
    for _ in range(4):
        await admit(org="acme")
    await admit(org="globex")

    assert (await async_client.get("/api/orgs/acme/sessions")).json()["sessions"] == 4
    assert (await async_client.get("/api/orgs/globex/sessions")).json()["sessions"] == 1


# --- session detail ----------------------------------------------------------


async def test_session_detail_says_which_node_holds_it(async_client, orgs):
    sid = await admit(org="acme", node="pytest-n2")

    body = (await async_client.get(f"/api/sessions/{sid}")).json()

    assert body["session_id"] == str(sid)
    assert body["org"] == "acme"
    assert body["node_id"] == "pytest-n2"
    assert body["started_at"] == "2026-01-01T00:00:00Z"


async def test_unknown_session_is_404(async_client, orgs):
    assert (await async_client.get(f"/api/sessions/{uuid.uuid4()}")).status_code == 404


async def test_released_session_stops_being_findable(async_client, orgs):
    sid = await admit(org="acme")
    assert (await async_client.get(f"/api/sessions/{sid}")).status_code == 200

    await registry.release_session(sid, "acme", "pytest-n1")

    assert (await async_client.get(f"/api/sessions/{sid}")).status_code == 404


# --- capacity ----------------------------------------------------------------


async def test_capacity_reports_the_global_picture(async_client, orgs, settings):
    settings.GLOBAL_MAX_SESSIONS = 1000
    for _ in range(3):
        await admit(org="acme")
    for _ in range(2):
        await admit(org="globex")

    body = (await async_client.get("/api/capacity")).json()

    assert body["global"]["sessions"] == 5
    assert body["global"]["limit"] == 1000
    assert body["global"]["available"] == 995


async def test_capacity_lists_every_org_busiest_first(async_client, orgs):
    await admit(org="globex")
    for _ in range(4):
        await admit(org="acme")

    rows = (await async_client.get("/api/capacity")).json()["orgs"]

    assert [r["org"] for r in rows] == ["acme", "globex"]
    assert rows[0]["sessions"] == 4
    assert rows[0]["utilization"] == 0.04


async def test_capacity_lists_live_nodes_and_their_load(async_client, orgs):
    await registry.heartbeat("pytest-n1")
    await registry.heartbeat("pytest-n2")
    for _ in range(3):
        await admit(org="acme", node="pytest-n1")
    await admit(org="acme", node="pytest-n2")

    nodes = (await async_client.get("/api/capacity")).json()["nodes"]

    assert {n["node_id"] for n in nodes} == {"pytest-n1", "pytest-n2"}
    by_id = {n["node_id"]: n for n in nodes}
    assert by_id["pytest-n1"]["sessions"] == 3
    assert by_id["pytest-n2"]["sessions"] == 1
    assert by_id["pytest-n1"]["draining"] is False


async def test_capacity_flags_a_draining_node(async_client, orgs):
    await registry.heartbeat("pytest-n1")
    await admit(org="acme", node="pytest-n1")
    await registry.mark_draining("pytest-n1")

    nodes = (await async_client.get("/api/capacity")).json()["nodes"]
    assert nodes[0]["draining"] is True


async def test_capacity_is_empty_but_valid_when_idle(async_client, orgs):
    body = (await async_client.get("/api/capacity")).json()
    assert body["global"]["sessions"] == 0
    assert body["nodes"] == []
    assert all(r["sessions"] == 0 for r in body["orgs"])


# --- the requirement itself --------------------------------------------------


async def test_any_node_gives_the_same_answer(async_client, orgs, settings):
    """Requirement 6 in one test.

    A session admitted on n1 is reported identically by a process calling itself
    n2, because the answer comes from Redis rather than from either node's own
    view of its connections.
    """
    sid = await admit(org="acme", node="pytest-n1")

    settings.NODE_ID = "pytest-n2"
    body = (await async_client.get(f"/api/sessions/{sid}")).json()

    assert body["node_id"] == "pytest-n1"   # where the session lives
    assert body["answered_by"] == "pytest-n2"  # who answered

    counts = (await async_client.get("/api/orgs/acme/sessions")).json()
    assert counts["sessions"] == 1
    assert counts["answered_by"] == "pytest-n2"


async def test_reaped_sessions_leave_the_api_immediately(async_client, orgs):
    """A dead node's sessions must stop showing as live."""
    for _ in range(3):
        await admit(org="acme", node="pytest-dead")
    assert (await async_client.get("/api/orgs/acme/sessions")).json()["sessions"] == 3

    await registry.reap_node("pytest-dead")

    body = (await async_client.get("/api/orgs/acme/sessions")).json()
    assert body["sessions"] == 0
    assert body["by_node"] == {}
