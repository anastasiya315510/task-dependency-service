import json
import logging

import redis.asyncio as aioredis
from pydantic import ValidationError
from redis.exceptions import RedisError

from app.log_call import log_call
from app.models import Task

logger = logging.getLogger(__name__)

_CACHE_NS = "tds:chain"
_CHAIN_CACHE_TTL = 3600  # 1 hour
# TTL rationale: balances freshness vs. Redis memory usage. The Kafka consumer
# invalidates the key immediately on any task update, so stale reads only occur
# during the brief window before the consumer processes the event.


class TaskChainCacheService:
    def __init__(self, redis_client: aioredis.Redis):
        self._redis = redis_client

    def _key(self, task_id: str) -> str:
        return f"{_CACHE_NS}:{task_id}"

    @log_call
    async def get(self, task_id: str) -> list[Task] | None:
        try:
            cached = await self._redis.get(self._key(task_id))
            if cached:
                result = [Task(**t) for t in json.loads(cached)]
                logger.debug("Cache hit for task %r — %d item(s)", task_id, len(result))
                return result
            logger.debug("Cache miss for task %r", task_id)
        except RedisError:
            logger.warning("Redis unavailable, cache miss for %s", task_id)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.error("Corrupt cache entry for %s: %s — self-healing", task_id, exc)
            await self.invalidate(task_id)
        return None

    @log_call
    async def set(self, task_id: str, chain: list[Task]) -> None:
        try:
            await self._redis.set(
                self._key(task_id),
                json.dumps([t.model_dump() for t in chain]),
                ex=_CHAIN_CACHE_TTL,
            )
            logger.debug(
                "Cache written for task %r — %d item(s), TTL %ds",
                task_id,
                len(chain),
                _CHAIN_CACHE_TTL,
            )
        except RedisError:
            logger.warning("Redis unavailable, skipping cache write for %s", task_id)

    @log_call
    async def invalidate(self, task_id: str) -> None:
        try:
            await self._redis.delete(self._key(task_id))
            logger.debug("Cache invalidated for task %r", task_id)
        except RedisError:
            logger.warning("Redis unavailable, skipping cache invalidation for %s", task_id)