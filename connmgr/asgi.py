"""ASGI entrypoint.

Routes HTTP to Django and /ws/ to the Channels consumer stack. The heartbeat and
reaper background tasks attach here in a later phase.
"""

import os

from channels.routing import ProtocolTypeRouter, URLRouter
from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "connmgr.settings")

# Must run before importing anything that touches models or settings-dependent
# module state.
django_asgi_app = get_asgi_application()

from connsessions.routing import websocket_urlpatterns  # noqa: E402

application = ProtocolTypeRouter(
    {
        "http": django_asgi_app,
        "websocket": URLRouter(websocket_urlpatterns),
    }
)
