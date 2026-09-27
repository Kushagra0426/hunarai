from django.conf import settings
from django.http import JsonResponse


def health(request):
    """Liveness probe. Used by compose healthchecks and the load balancer.

    Reports which node answered, which is how the chaos scripts confirm the LB
    is actually spreading connections across nodes.
    """
    return JsonResponse({"status": "ok", "node_id": settings.NODE_ID})
