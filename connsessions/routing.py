from django.urls import path

from .consumers import SessionConsumer

# The org is in the path, not a header: it is the tenant the slot is billed to,
# and having it in the URL keeps the load balancer able to see it too.
websocket_urlpatterns = [
    path("ws/<slug:org_slug>/", SessionConsumer.as_asgi()),
]
