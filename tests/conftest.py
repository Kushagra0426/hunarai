import os

import pytest

# Tests talk to a real local Redis and must never touch a shared database.
# Point at db 15 and drop DATABASE_URL before Django loads, so a configured
# Neon/production URL cannot be reached from a test run.
os.environ["REDIS_URL"] = os.environ.get("TEST_REDIS_URL", "redis://127.0.0.1:6379/15")
os.environ.pop("DATABASE_URL", None)


@pytest.fixture(autouse=True)
async def reset_redis_client():
    """Close the cached client between tests.

    registry.get_redis() memoises a client bound to the running event loop, and
    pytest-asyncio gives each test a new loop. Reusing the old client across
    loops raises "attached to a different loop", so tear it down each time.
    """
    from connsessions import registry

    await registry.close_redis()
    yield
    await registry.close_redis()
