"""Test settings. Never touches a real database or the default Redis db.

A separate settings module rather than environment fiddling in conftest, because
conftest runs after pytest-django has already read DJANGO_SETTINGS_MODULE and
built the connection. Doing it here means there is no ordering to get wrong: a
developer with DATABASE_URL pointing at production Postgres cannot have a test
run create test_<their-db> on it, which is exactly what happened once.
"""

from .settings import *  # noqa: F401,F403
from .settings import BASE_DIR

# sqlite in a file, not :memory: -- the API tests call asyncio.run(), and an
# in-memory database is per-connection, so a view opening its own connection
# would see an empty schema.
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "test-db.sqlite3",
        "TEST": {"NAME": BASE_DIR / "test-db.sqlite3"},
    }
}

# db 15, never the default 0 a developer is running the app against.
REDIS_URL = "redis://127.0.0.1:6379/15"

# Predictable regardless of the machine's hostname.
NODE_ID = "pytest-node"

GLOBAL_MAX_SESSIONS = 10_000
HEARTBEAT_SEC = 5
NODE_TIMEOUT_SEC = 15
REAP_INTERVAL_SEC = 5
DRAIN_BATCH_SIZE = 0
DRAIN_INTERVAL_MS = 0

# Quiet: the registry logs every admission, which buries real failures.
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "root": {"handlers": ["console"], "level": "WARNING"},
}

PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]
