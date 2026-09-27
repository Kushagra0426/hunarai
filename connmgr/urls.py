from django.contrib import admin
from django.urls import path

from connsessions import views

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("admin/", admin.site.urls),
    path("health", views.health, name="health"),
    # Requirement 6: any node answers these, because the answers come from Redis
    # rather than from the node's own view.
    path("api/capacity", views.capacity, name="capacity"),
    path("api/orgs/<slug:org_slug>/sessions", views.org_sessions, name="org-sessions"),
    path("api/sessions/<uuid:session_id>", views.session_detail, name="session-detail"),
]
