import logging

from app.config import settings
from app.log_call import log_call
from app.models import ChainResult
from app.repositories.protocols import TaskChainCache, TaskRepository

logger = logging.getLogger(__name__)


class TaskChainService:
    """Resolves task ancestor chains with read-through caching.

    Complete chains (resolved to root) are cached with a long TTL.
    Incomplete chains (missing parents or depth-limited) use a short TTL to
    reduce MongoDB pressure during Kafka ingestion lag.
    Cycle results are NOT cached — they indicate corrupted data that should be fixed.
    """

    def __init__(self, repo: TaskRepository, cache: TaskChainCache):
        self._repo = repo
        self._cache = cache

    @log_call(level=logging.INFO)
    async def get_chain(self, task_id: str) -> ChainResult | None:
        """Returns the dependency chain, or None if the task does not exist.

        complete=True means the full chain was resolved.
        complete=False means a parent was referenced but not yet ingested,
        or the depth limit was hit — the caller should not treat [] as "root task"
        in those cases.
        """
        cached = await self._cache.get(task_id)
        if cached is not None:
            return cached

        result = await self._repo.find_ancestor_chain(task_id, settings.max_chain_depth)
        if result is None:
            logger.debug("get_chain %r — task not found", task_id)
            return None

        logger.info(
            "get_chain %r — computed %d item(s), complete=%s, cycle=%s",
            task_id,
            len(result.tasks),
            result.complete,
            result.cycle,
        )
        # Cache complete chains (long TTL) and incomplete chains (short TTL) to
        # reduce MongoDB pressure during Kafka ingestion lag. Cycle results are
        # not cached — they indicate corrupted data that should be fixed.
        if not result.cycle:
            await self._cache.set(task_id, result)
        return result