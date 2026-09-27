"""ASGI entrypoint.

Routes HTTP to Django and /ws/ to the Channels consumer stack, and owns the
lifetime of the two background loops: this node's heartbeat, and the reaper that
cleans up after nodes which stopped sending one.
"""

import asyncio
import logging
import os

from channels.routing import ProtocolTypeRouter, URLRouter
from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "connmgr.settings")

# Must run before importing anything that touches models or settings-dependent
# module state.
django_asgi_app = get_asgi_application()

from connsessions import registry, tasks  # noqa: E402
from connsessions.routing import websocket_urlpatterns  # noqa: E402

log = logging.getLogger(__name__)

inner_application = ProtocolTypeRouter(
    {
        "http": django_asgi_app,
        "websocket": URLRouter(websocket_urlpatterns),
    }
)


class Lifespan:
    """Starts and stops the background loops around the wrapped app.

    Channels does not handle the ASGI lifespan protocol itself, so this wraps it.
    Servers that do not send lifespan events (some test harnesses) simply never
    reach this code, and the loops can be driven directly instead -- which is what
    the tests do.
    """

    def __init__(self, app):
        self.app = app
        self.stop_event = None
        self.background = []

    async def __call__(self, scope, receive, send):
        if scope["type"] != "lifespan":
            return await self.app(scope, receive, send)

        while True:
            message = await receive()

            if message["type"] == "lifespan.startup":
                try:
                    await self.startup()
                except Exception as exc:
                    log.exception("startup failed")
                    await send({"type": "lifespan.startup.failed", "message": str(exc)})
                    return
                await send({"type": "lifespan.startup.complete"})

            elif message["type"] == "lifespan.shutdown":
                try:
                    await self.shutdown()
                except Exception:
                    log.exception("shutdown failed")
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def startup(self):
        from django.conf import settings

        self.stop_event = asyncio.Event()

        # A node that restarted keeps its id under compose, so clear any drain
        # flag left behind by the previous process -- otherwise it would come back
        # up refusing every connection.
        await registry.clear_draining(settings.NODE_ID)

        self.background = [
            asyncio.create_task(
                tasks.heartbeat_loop(self.stop_event), name="heartbeat"
            ),
            asyncio.create_task(tasks.reaper_loop(self.stop_event), name="reaper"),
        ]
        log.info("node=%s startup complete", settings.NODE_ID)

    async def shutdown(self):
        if self.stop_event is not None:
            self.stop_event.set()

        if self.background:
            # Bounded: a loop stuck on a hung Redis call must not stop the process
            # from exiting.
            done, pending = await asyncio.wait(self.background, timeout=5)
            for task in pending:
                task.cancel()
            self.background = []

        await registry.close_redis()
        log.info("shutdown complete")


application = Lifespan(inner_application)
