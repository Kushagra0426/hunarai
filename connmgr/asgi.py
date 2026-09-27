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

from connsessions import drain, registry, tasks  # noqa: E402
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
        self._install_sigterm_handler()
        log.info("node=%s startup complete", settings.NODE_ID)

    def _install_sigterm_handler(self):
        """Drain on SIGTERM, before the server closes anything itself.

        Lifespan shutdown is too late: uvicorn handles SIGTERM by closing live
        websockets with 1012 (Service Restart) and only then runs the shutdown
        event, so by the time drain() was reached there was nothing left to close
        and clients could not tell a drain from any other restart.

        Chaining to uvicorn's own handler rather than replacing it keeps the
        normal shutdown sequence intact -- this only interposes the drain.
        """
        import signal

        loop = asyncio.get_running_loop()
        previous = signal.getsignal(signal.SIGTERM)

        def on_sigterm(signum, frame):
            # Signal context: schedule the drain rather than running it here.
            loop.create_task(self._drain_then(previous, signum, frame))

        try:
            signal.signal(signal.SIGTERM, on_sigterm)
        except ValueError:
            # Not the main thread (some test runners). The lifespan shutdown
            # drain still covers the ordinary path.
            log.debug("could not install SIGTERM handler off the main thread")

    async def _drain_then(self, previous_handler, signum, frame):
        """Close sessions cleanly, then let the server shut down as usual."""
        from django.conf import settings

        try:
            closed = await drain.drain(node_id=settings.NODE_ID)
            log.info("sigterm drain closed %s sessions", closed)
        except Exception:
            log.exception("sigterm drain failed")

        if callable(previous_handler):
            previous_handler(signum, frame)

    async def shutdown(self):
        from django.conf import settings

        node_id = settings.NODE_ID

        # Drain before stopping the heartbeat. The reaper on another node must
        # keep seeing this one as alive while it winds down, or it would start
        # reaping sessions that are being closed cleanly right here and the audit
        # trail would call them node_lost.
        try:
            closed = await drain.drain(node_id=node_id)
            if closed:
                log.info("drained %s sessions node=%s", closed, node_id)
        except Exception:
            log.exception("drain failed node=%s", node_id)

        if self.stop_event is not None:
            self.stop_event.set()

        if self.background:
            # Bounded: a loop stuck on a hung Redis call must not stop the process
            # from exiting.
            done, pending = await asyncio.wait(self.background, timeout=5)
            for task in pending:
                task.cancel()
            self.background = []

        # Sessions are released and the node is going away, so drop its liveness
        # records. Without this the reaper keeps finding a node that left cleanly
        # and logs it as a corpse.
        try:
            await registry.forget_node(node_id)
        except Exception:
            log.exception("could not clear liveness records node=%s", node_id)

        await registry.close_redis()
        log.info("shutdown complete node=%s", node_id)


application = Lifespan(inner_application)
