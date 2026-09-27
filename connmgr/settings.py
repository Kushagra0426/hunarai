"""Django settings for connmgr.

Everything environment-specific is read from env vars so the same image runs as
any node in the cluster. See .env.example for the full list.
"""

import os
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent

load_dotenv(BASE_DIR / ".env")


def _env(key, default=None):
    value = os.environ.get(key, default)
    if value is None:
        raise RuntimeError(f"missing required environment variable: {key}")
    return value


def _env_int(key, default):
    return int(os.environ.get(key, default))


# --- node identity -----------------------------------------------------------
# Every process needs a stable id: it is the key under which this node's
# heartbeat and session set live in Redis, and what the reaper cleans up when
# the node stops heartbeating. Defaults to the hostname, which is the container
# id under compose.

NODE_ID = os.environ.get("NODE_ID") or os.uname().nodename

# --- core --------------------------------------------------------------------

SECRET_KEY = os.environ.get(
    "SECRET_KEY", "django-insecure-dev-only-+@aooz2i*6tl9_$s85hle@)!@=ryh#kd"
)

DEBUG = os.environ.get("DEBUG", "1") == "1"

ALLOWED_HOSTS = os.environ.get("ALLOWED_HOSTS", "*").split(",")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "channels",
    "connsessions.apps.ConnSessionsConfig",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "connmgr.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "connmgr.wsgi.application"
ASGI_APPLICATION = "connmgr.asgi.application"

# --- storage -----------------------------------------------------------------
# Postgres holds org config and the session audit trail. It is deliberately NOT
# the concurrency enforcer -- that lives in Redis, see sessions/registry.py.
# Falls back to sqlite so the project runs with no services for a smoke test.

DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    _db = urlparse(DATABASE_URL)
    # Query params carry through to libpq as connection options -- managed
    # Postgres (Neon, RDS) needs sslmode=require, and dropping it fails the
    # handshake rather than silently downgrading.
    _opts = {k: v[-1] for k, v in parse_qs(_db.query).items()}
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": _db.path.lstrip("/"),
            "USER": unquote(_db.username or ""),
            "PASSWORD": unquote(_db.password or ""),
            "HOST": _db.hostname or "",
            "PORT": _db.port or "",
            "OPTIONS": _opts,
            # Reuse connections instead of reconnecting per request: a managed
            # Postgres sits across the network, so the handshake is not free.
            "CONN_MAX_AGE": _env_int("DB_CONN_MAX_AGE", 60),
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "db.sqlite3",
        }
    }

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")

# In-memory, not Redis-backed. Nothing broadcasts across nodes: sessions are
# tracked in Redis directly (see connsessions/registry.py) and drain only ever
# closes connections on the node running it, so a cross-node layer would be a
# Redis pub/sub subscription per process for no subscriber.
CHANNEL_LAYERS = {
    "default": {"BACKEND": "channels.layers.InMemoryChannelLayer"},
}

# --- capacity and liveness ---------------------------------------------------
# GLOBAL_MAX_SESSIONS is the platform-wide ceiling shared by all orgs. Per-org
# limits and reserved floors are per-Organization rows, not settings.

GLOBAL_MAX_SESSIONS = _env_int("GLOBAL_MAX_SESSIONS", 10_000)

# How often a node proves it is alive, and how long silence must last before the
# reaper releases its sessions. A crashed node and a briefly unreachable one look
# identical from outside, so NODE_TIMEOUT_SEC is a tuning knob, not a correct
# answer: too low and a network blip kills a healthy node's sessions, too high
# and orphaned sessions hold quota. Must be a comfortable multiple of the
# heartbeat interval.
HEARTBEAT_SEC = _env_int("HEARTBEAT_SEC", 5)
NODE_TIMEOUT_SEC = _env_int("NODE_TIMEOUT_SEC", 15)
REAP_INTERVAL_SEC = _env_int("REAP_INTERVAL_SEC", 5)

# --- drain -------------------------------------------------------------------
# On SIGTERM the node stops admitting and closes live sessions. Defaults to
# closing everything at once; raising DRAIN_INTERVAL_MS spreads reconnects out
# to avoid a thundering herd against the surviving nodes.
DRAIN_BATCH_SIZE = _env_int("DRAIN_BATCH_SIZE", 0)  # 0 == all at once
DRAIN_INTERVAL_MS = _env_int("DRAIN_INTERVAL_MS", 0)

# --- i18n / static -----------------------------------------------------------

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "node": {"format": f"%(asctime)s [{NODE_ID}] %(levelname)s %(name)s: %(message)s"},
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "node"},
    },
    "root": {"handlers": ["console"], "level": os.environ.get("LOG_LEVEL", "INFO")},
}
