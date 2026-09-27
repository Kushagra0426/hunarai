from django.apps import AppConfig


class ConnSessionsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    # Named connsessions rather than sessions: django.contrib.sessions already
    # claims that label, and this app is the connection registry, not cookie
    # sessions.
    name = "connsessions"
    verbose_name = "Connection sessions"
