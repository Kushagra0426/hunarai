"""Read API. Requirement 6: any node answers these, not just the one holding
the session.

That works because the answers come from Redis rather than from any node's local
view. Ask node 3 how many sessions Acme has and it gives the same number node 1
would, including sessions on nodes that have since died.

Plain Django views, no DRF: three read-only JSON endpoints do not justify a
serialisation framework, a router and a viewset hierarchy.
"""

from django.conf import settings
from django.http import Http404, JsonResponse
from django.shortcuts import render

from . import drain, registry
from .models import Organization

# Async views, not sync ones. The registry is async and its client is shared with
# the consumers in this process, so a sync view would have to bridge with
# asyncio.run() -- which creates a second loop inside the running server and
# fails with "Future attached to a different loop" the moment it touches that
# shared client. Async views run on the server's own loop and just await.


async def dashboard(request):
    """Live cluster view. Static page; everything on it comes from /api/capacity.

    The load buttons open real WebSocket connections from the browser rather than
    asking the server to simulate them -- simulated load would prove nothing about
    the connection layer, which is the only thing worth showing here.
    """
    return render(request, "connsessions/dashboard.html")


async def health(request):
    """Liveness probe. Used by compose healthchecks and the load balancer.

    Reports which node answered, which is how the chaos scripts confirm the LB is
    actually spreading connections across nodes. A draining node reports itself
    unhealthy so the balancer stops sending it new connections.
    """
    draining = drain.is_draining()
    return JsonResponse(
        {
            "status": "draining" if draining else "ok",
            "node_id": settings.NODE_ID,
            "live_sessions": drain.live_count(),
        },
        status=503 if draining else 200,
    )


async def org_sessions(request, org_slug):
    """Live session count for one org, with a per-node breakdown.

    The breakdown is the interesting part: it is what shows the load balancer
    spreading an org across nodes, and what shows a node's sessions vanishing
    when it dies.
    """
    org = await Organization.objects.filter(slug=org_slug).afirst()
    if org is None:
        raise Http404(f"unknown organization: {org_slug}")

    count, by_node = await _org_detail(org_slug)

    return JsonResponse(
        {
            "org": org_slug,
            "name": org.name,
            "active": org.is_active,
            "sessions": count,
            "limit": org.max_sessions,
            "reserved_floor": org.reserved_floor,
            "available": max(org.max_sessions - count, 0),
            "by_node": by_node,
            "answered_by": settings.NODE_ID,
        }
    )


async def session_detail(request, session_id):
    """Which node is session Y on, and since when."""
    session = await registry.get_session(session_id)
    if session is None:
        raise Http404(f"unknown or ended session: {session_id}")

    return JsonResponse(
        {
            "session_id": session.get("session_id", str(session_id)),
            "org": session.get("org"),
            "node_id": session.get("node"),
            "client_id": session.get("client") or None,
            "started_at": session.get("started_at"),
            "answered_by": settings.NODE_ID,
        }
    )


async def capacity(request):
    """Platform-wide view: global usage, every org, every live node."""
    orgs = [
        org
        async for org in Organization.objects.filter(is_active=True).values(
            "slug", "name", "max_sessions", "reserved_floor"
        )
    ]
    global_count, per_org, nodes = await _capacity(orgs)

    rows = []
    for org in orgs:
        used = per_org.get(org["slug"], 0)
        rows.append(
            {
                "org": org["slug"],
                "name": org["name"],
                "sessions": used,
                "limit": org["max_sessions"],
                "reserved_floor": org["reserved_floor"],
                "utilization": round(used / org["max_sessions"], 3)
                if org["max_sessions"]
                else None,
            }
        )
    rows.sort(key=lambda r: -r["sessions"])

    return JsonResponse(
        {
            "global": {
                "sessions": global_count,
                "limit": settings.GLOBAL_MAX_SESSIONS,
                "available": max(settings.GLOBAL_MAX_SESSIONS - global_count, 0),
            },
            "orgs": rows,
            "nodes": nodes,
            "answered_by": settings.NODE_ID,
        }
    )


# --- redis reads -------------------------------------------------------------


async def _org_detail(org_slug):
    count = await registry.org_session_count(org_slug)
    session_ids = await registry.org_session_ids(org_slug)

    # Which node each session lives on. Pipelined so a thousand sessions is one
    # round-trip rather than a thousand.
    by_node = {}
    if session_ids:
        r = registry.get_redis()
        ids = list(session_ids)
        async with r.pipeline(transaction=False) as pipe:
            for sid in ids:
                pipe.hget(registry.session_key(sid), "node")
            nodes = await pipe.execute()
        for node in nodes:
            if node:
                by_node[node] = by_node.get(node, 0) + 1

    return count, by_node


async def _capacity(orgs):
    global_count = await registry.global_session_count()

    r = registry.get_redis()
    slugs = [o["slug"] for o in orgs]
    per_org = {}
    if slugs:
        async with r.pipeline(transaction=False) as pipe:
            for slug in slugs:
                pipe.get(registry.org_count_key(slug))
            values = await pipe.execute()
        per_org = {slug: int(v or 0) for slug, v in zip(slugs, values)}

    # Live nodes, with how many sessions each is carrying.
    node_ids = await registry.alive_nodes()
    nodes = []
    if node_ids:
        async with r.pipeline(transaction=False) as pipe:
            for node_id in node_ids:
                pipe.scard(registry.node_sessions_key(node_id))
                pipe.exists(registry.node_draining_key(node_id))
            values = await pipe.execute()
        for i, node_id in enumerate(node_ids):
            nodes.append(
                {
                    "node_id": node_id,
                    "sessions": values[i * 2],
                    "draining": bool(values[i * 2 + 1]),
                }
            )
        nodes.sort(key=lambda n: -n["sessions"])

    return global_count, per_org, nodes
