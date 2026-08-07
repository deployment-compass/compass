from __future__ import annotations

import redis.asyncio as redis

from compass.config import settings

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(settings.redis_url, decode_responses=True)
    return _client


async def claim_once(dedupe_key: str, ttl_seconds: int = 3600) -> bool:
    """
    Atomically claims a dedupe key. Returns True the first time it's seen
    (caller should proceed), False on any repeat within the TTL (caller
    should drop the event). This is the idempotency guard mentioned in the
    design notes — applied at the normalizer, before anything is published.
    """
    client = get_redis()
    was_set = await client.set(f"dedupe:{dedupe_key}", "1", nx=True, ex=ttl_seconds)
    return bool(was_set)