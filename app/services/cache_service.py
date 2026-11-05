import json
import logging
from typing import Awaitable, cast

import redis.asyncio as aioredis
from pydantic import ValidationError
from redis.exceptions import RedisError

from app.config import settings
from app.log_call import log_call
from app.models import ChainResult, Task

logger = logging.getLogger(__name__)

_DEFAULT_CACHE_NS = "tds:chain"
_DEFAULT_MEMBERS_NS = "tds:members"


class TaskChainCacheService:
    def __init__(
        self,
        redis_client: aioredis.Redis,
        cache_ns: str = _DEFAULT_CACHE_NS,
        members_ns: str = _DEFAULT_MEMBERS_NS,
    ):
        self._redis = redis_client
        self._cache_ns = cache_ns
        self._members_ns = members_ns

    def _key(self, task_id: str) -> str:
        """Redis key for the cached chain: ``{cache_ns}:{task_id}``."""
        return f"{self._cache_ns}:{task_id}"

    def _descendant_chains_key(self, task_id: str) -> str:
        """Key for the set of descendant-task IDs whose cached chains include task_id as an ancestor."""
        return f"{self._members_ns}:{task_id}"

    @log_call
    async def get(self, task_id: str) -> ChainResult | None:
        try:
            cached = await self._redis.get(self._key(task_id))
            if cached:
                data = json.loads(cached)
                chain = [Task(**t) for t in data["chain"]]
                result = ChainResult(
                    tasks=chain,
                    complete=data["complete"],
                    cycle=data["cycle"],
                    # depth_limited was added later; default to False for old entries
                    depth_limited=data.get("depth_limited", False),
                )
                logger.debug("Cache hit for task %r — %d item(s)", task_id, len(chain))
                return result
            logger.debug("Cache miss for task %r", task_id)
        except RedisError:
            logger.warning("Redis unavailable, cache miss for %s", task_id)
        except (json.JSONDecodeError, KeyError, ValidationError) as exc:
            logger.error("Corrupt cache entry for %s: %s — self-healing", task_id, exc)
            try:
                await self.invalidate(task_id)
            except RedisError:
                pass  # already logged by invalidate()
        return None

    @log_call
    async def set(self, task_id: str, result: ChainResult) -> None:
        try:
            ttl = (
                settings.cache_chain_ttl_seconds
                if result.complete
                else settings.cache_incomplete_chain_ttl_seconds
            )
            payload = json.dumps(
                {
                    "chain": [t.model_dump(mode="json") for t in result.tasks],
                    "complete": result.complete,
                    "cycle": result.cycle,
                    "depth_limited": result.depth_limited,
                }
            )
            # Use a pipeline so all writes go in one round-trip and
            # sadd/expire are always paired atomically.
            pipe = self._redis.pipeline(transaction=True)
            pipe.set(self._key(task_id), payload, ex=ttl)
            # Build the reverse membership index only for complete chains.
            # Incomplete chains use a short TTL as the backstop; building a partial
            # index could cause spurious invalidations for the missing ancestors.
            if result.complete:
                for ancestor in result.tasks:
                    mkey = self._descendant_chains_key(ancestor.id)
                    pipe.sadd(mkey, task_id)
                    pipe.expire(mkey, settings.cache_chain_ttl_seconds)
            await pipe.execute()
            logger.debug(
                "Cache written for task %r — %d item(s), complete=%s, cycle=%s, TTL %ds",
                task_id,
                len(result.tasks),
                result.complete,
                result.cycle,
                ttl,
            )
        except RedisError:
            logger.warning("Redis unavailable, skipping cache write for %s", task_id)

    @log_call
    async def invalidate(self, task_id: str) -> None:
        """Delete task_id's chain and the cached chains of every task that is a descendant of task_id."""
        try:
            member_key = self._descendant_chains_key(task_id)
            # cast: redis-py stubs type smembers as Awaitable[set[Any]] | set[Any].
            # decode_responses=True (see db.py) guarantees the members are str, not bytes.
            chain_ids: set[str] = await cast(Awaitable[set[str]], self._redis.smembers(member_key))
            keys = [self._key(task_id), member_key]
            for cid in chain_ids:
                keys.append(self._key(cid))
                keys.append(self._descendant_chains_key(cid))
                # NOTE: we do NOT remove cid from other ancestors' _members sets here.
                # Those stale entries are harmless (DEL on a missing key is a no-op) and
                # will expire naturally via TTL. Cleaning them up would require an O(N)
                # scan of all ancestor keys, which costs more than the occasional no-op DEL.
            # Pipeline all deletes in one network round-trip.
            # transaction=False = fire-and-forget pipeline (no MULTI/EXEC), which is
            # correct here since each DEL is independent.
            pipe = self._redis.pipeline(transaction=False)
            for key in keys:
                pipe.delete(key)
            await pipe.execute()
            logger.debug(
                "Cache invalidated for task %r and %d descendant chain(s)",
                task_id,
                len(chain_ids),
            )
        except RedisError:
            logger.warning("Redis unavailable, skipping cache invalidation for %s", task_id)