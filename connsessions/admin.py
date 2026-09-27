"""Admin is the free read UI for requirement 6.

Note the deliberate asymmetry: Organization is editable because limits are
configuration, while SessionRecord is read-only because it is an audit trail and
live state belongs to Redis anyway. Editing a row here would change nothing
about what is actually connected -- it would only make the history lie.
"""

from django.contrib import admin

from .models import Organization, SessionRecord


@admin.register(Organization)
class OrganizationAdmin(admin.ModelAdmin):
    list_display = ("slug", "name", "max_sessions", "reserved_floor", "is_active")
    list_filter = ("is_active",)
    search_fields = ("slug", "name")
    readonly_fields = ("created_at", "updated_at")


@admin.register(SessionRecord)
class SessionRecordAdmin(admin.ModelAdmin):
    list_display = (
        "session_id",
        "organization",
        "node_id",
        "client_id",
        "started_at",
        "ended_at",
        "end_reason",
    )
    list_filter = ("end_reason", "node_id", "organization")
    search_fields = ("session_id", "client_id", "node_id")
    date_hierarchy = "started_at"
    list_select_related = ("organization",)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False
