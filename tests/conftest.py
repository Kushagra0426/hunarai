"""Shared test fixtures.

Settings come from connmgr.settings_test via pytest.ini -- sqlite and Redis db 15,
pinned there rather than patched here so no environment variable can redirect a
test run at real infrastructure.
"""

import pytest


@pytest.fixture(autouse=True)
async def reset_redis_client():
    """Close the cached Redis client between tests.

    The registry memoises a client bound to whichever event loop created it, and
    pytest-asyncio gives each test a fresh loop. Reusing it across loops raises
    "attached to a different loop", so tear it down each time.
    """
    from connsessions import registry

    await registry.close_redis()
    yield
    await registry.close_redis()
