"""WebSocket consumer: the session lifecycle.

Channels calls connect() when a client arrives and disconnect() when it leaves,
including on abnormal closes -- a killed client, a dropped network. That makes
disconnect() the right place to release the slot, and it is why release must be
tolerant of being called for a session that was never fully registered.

Limits are enforced by a single Lua script (registry.admit_session), not here.
That matters: the check and the increment have to be one indivisible step, or two
connections arriving simultaneously on different nodes would both read "499 of
500" and both be admitted.
"""

import logging
import uuid

from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncJsonWebsocketConsumer
from django.conf import settings
from django.utils import timezone

from . import registry
from .models import Organization, SessionRecord

log = logging.getLogger(__name__)

# Application close codes. 4000-4999 is the range reserved for application use,
# so a client can tell "you are not allowed" from a transport failure and decide
# whether retrying is pointless or worth it.
CLOSE_UNKNOWN_ORG = 4404
CLOSE_ORG_INACTIVE = 4403
CLOSE_INTERNAL = 4500

# Rejection reasons from admit.lua, mapped to codes a client can act on. The
# distinction is deliberate: ORG_LIMIT means "you are at your own ceiling, buy
# more" and retrying elsewhere is pointless, while GLOBAL_FULL and NODE_DRAINING
# are transient and a retry is the right response.
CLOSE_ORG_LIMIT = 4429
CLOSE_GLOBAL_FULL = 4503
CLOSE_NODE_DRAINING = 4504
CLOSE_DUPLICATE = 4409

REJECT_CODES = {
    "ORG_LIMIT": CLOSE_ORG_LIMIT,
    "GLOBAL_FULL": CLOSE_GLOBAL_FULL,
    "NODE_DRAINING": CLOSE_NODE_DRAINING,
    "DUPLICATE_SESSION": CLOSE_DUPLICATE,
}


class SessionConsumer(AsyncJsonWebsocketConsumer):
    """One instance per connection.

    Instance attributes hold this session's identity so disconnect() can clean
    up without re-reading the URL route.
    """

    async def connect(self):
        self.org_slug = self.scope["url_route"]["kwargs"]["org_slug"]
        self.session_id = uuid.uuid4()
        self.node_id = settings.NODE_ID
        self.registered = False

        # Opaque to this layer: the connection manager does not care who the end
        # user is, only which tenant to bill the slot to.
        params = self.scope.get("query_string", b"").decode()
        self.client_id = _query_value(params, "client_id") or ""

        org = await self._get_org(self.org_slug)
        if org is None:
            # Accept before close so the client receives the code rather than a
            # bare handshake failure, which is indistinguishable from the server
            # being down.
            await self.accept()
            await self.close(code=CLOSE_UNKNOWN_ORG)
            log.info("rejected session org=%s: unknown org", self.org_slug)
            return

        if not org.is_active:
            await self.accept()
            await self.close(code=CLOSE_ORG_INACTIVE)
            log.info("rejected session org=%s: org inactive", self.org_slug)
            return

        self.org_id = org.id
        started_at = timezone.now()

        # Limits come from the Redis cache, not the row just loaded. The cache is
        # the fast path (admission reads it on every connect) and the signal in
        # models.py writes through on save, so it is as current as the row. On a
        # cold miss fall back to the row and populate the cache.
        limits = await registry.get_org_config(self.org_slug)
        if limits is None:
            limits = {
                "max_sessions": org.max_sessions,
                "reserved_floor": org.reserved_floor,
            }
            await registry.cache_org_config(
                self.org_slug, org.max_sessions, org.reserved_floor
            )

        try:
            org_count, global_count = await registry.admit_session(
                session_id=self.session_id,
                org_slug=self.org_slug,
                node_id=self.node_id,
                client_id=self.client_id,
                started_at=started_at.isoformat(),
                max_sessions=limits["max_sessions"],
                reserved_floor=limits["reserved_floor"],
            )
            self.registered = True
        except registry.Rejected as exc:
            # Over a limit. Record the refusal so capacity pressure is visible
            # after the fact, then close with a code the client can act on.
            await self._write_rejection(started_at, org.id)
            await self.accept()
            await self.close(code=REJECT_CODES.get(exc.reason, CLOSE_INTERNAL))
            return

        try:
            await self._write_record(started_at)
        except Exception:
            # Registered in Redis but the audit write failed, or Redis itself is
            # down. Either way do not hold a slot we cannot account for.
            log.exception("session=%s setup failed", self.session_id)
            if self.registered:
                await registry.release_session(
                    self.session_id, self.org_slug, self.node_id
                )
                self.registered = False
            await self.accept()
            await self.close(code=CLOSE_INTERNAL)
            return

        await self.accept()
        await self.send_json(
            {
                "type": "session.established",
                "session_id": str(self.session_id),
                "org": self.org_slug,
                "node_id": self.node_id,
                "org_sessions": org_count,
                "org_limit": limits["max_sessions"],
            }
        )

    async def disconnect(self, code):
        """Release the slot. Called for clean and abnormal closes alike."""
        if not getattr(self, "registered", False):
            return

        try:
            await registry.release_session(
                self.session_id, self.org_slug, self.node_id
            )
            await self._close_record(SessionRecord.EndReason.CLIENT)
        except Exception:
            # Never raise out of disconnect: the socket is already gone, and an
            # exception here buys nothing but a noisy traceback. The phase 3
            # reaper is the backstop -- if this node dies before releasing, the
            # heartbeat timeout catches the orphan.
            log.exception("session=%s cleanup failed", self.session_id)
        finally:
            self.registered = False

    async def receive_json(self, content, **kwargs):
        """Minimal protocol: echo and ping.

        The point of this phase is the lifecycle, not messaging. A ping lets the
        chaos scripts hold a connection open and confirm it is genuinely alive
        rather than merely unclosed.
        """
        if content.get("type") == "ping":
            await self.send_json({"type": "pong", "session_id": str(self.session_id)})
        else:
            await self.send_json({"type": "echo", "data": content})

    # --- db access -----------------------------------------------------------
    # Wrapped: the ORM is sync, the consumer is async.

    @database_sync_to_async
    def _get_org(self, slug):
        return Organization.objects.filter(slug=slug).first()

    @database_sync_to_async
    def _write_record(self, started_at):
        SessionRecord.objects.create(
            session_id=self.session_id,
            organization_id=self.org_id,
            node_id=self.node_id,
            client_id=self.client_id,
            started_at=started_at,
        )

    @database_sync_to_async
    def _write_rejection(self, started_at, org_id):
        """Log the refusal. Born already ended -- it never held a slot."""
        SessionRecord.objects.create(
            session_id=self.session_id,
            organization_id=org_id,
            node_id=self.node_id,
            client_id=self.client_id,
            started_at=started_at,
            ended_at=started_at,
            end_reason=SessionRecord.EndReason.REJECTED,
        )

    @database_sync_to_async
    def _close_record(self, reason):
        # Guard on ended_at so a later reap cannot overwrite the real reason a
        # session ended.
        SessionRecord.objects.filter(
            session_id=self.session_id, ended_at__isnull=True
        ).update(ended_at=timezone.now(), end_reason=reason)


def _query_value(query_string, key):
    """Single value from a raw query string, or None."""
    from urllib.parse import parse_qs

    values = parse_qs(query_string).get(key)
    return values[-1] if values else None
