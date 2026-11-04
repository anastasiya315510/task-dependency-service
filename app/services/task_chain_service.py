import logging
from typing import NamedTuple

from app.log_call import log_call
from app.models import Task
from app.repositories.protocols import TaskChainCache, TaskRepository

logger = logging.getLogger(__name__)

MAX_CHAIN_DEPTH = 100


class ChainResult(NamedTuple):
    tasks: list[Task]
    complete: bool


class TaskChainService:
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
            logger.info("get_chain %r — cache hit, %d item(s)", task_id, len(cached))
            return ChainResult(tasks=cached, complete=True)

        root = await self._repo.find_by_id(task_id)
        if root is None:
            logger.info("get_chain %r — task not found", task_id)
            return None

        ancestor_chain, complete = await self._walk_ancestor_chain(task_id, root)
        logger.info(
            "get_chain %r — computed %d item(s), complete=%s",
            task_id,
            len(ancestor_chain),
            complete,
        )
        if complete:
            await self._cache.set(task_id, ancestor_chain)
        return ChainResult(tasks=ancestor_chain, complete=complete)

    @log_call
    async def _walk_ancestor_chain(
        self, start_id: str, root: Task
    ) -> tuple[list[Task], bool]:
        """Walk the ancestor chain. Returns (ancestor_chain, is_complete).

        is_complete is False when a parent is referenced but not yet ingested,
        or when MAX_CHAIN_DEPTH is reached — in either case the result must not
        be cached so the next request re-resolves against MongoDB.
        """
        ancestor_chain: list[Task] = []
        visited_ids: set[str] = {start_id}
        current_id = root.parent_task_id
        depth = 0

        while current_id and current_id not in visited_ids:
            if depth >= MAX_CHAIN_DEPTH:
                logger.warning(
                    "_walk_ancestor_chain — depth limit %d reached for %r, stopping",
                    MAX_CHAIN_DEPTH,
                    start_id,
                )
                return ancestor_chain, False

            visited_ids.add(current_id)
            parent = await self._repo.find_by_id(current_id)
            if parent is None:
                logger.debug(
                    "_walk_ancestor_chain — parent %r not yet ingested, stopping",
                    current_id,
                )
                return ancestor_chain, False

            logger.debug("_walk_ancestor_chain — visited %r", current_id)
            ancestor_chain.append(parent)
            current_id = parent.parent_task_id
            depth += 1

        return ancestor_chain, True
