from typing import Protocol

from app.models import ChainResult, Task


class TaskRepository(Protocol):
    """Storage interface for Task documents."""

    async def find_by_id(self, task_id: str) -> Task | None:
        """Return the task with the given ID, or None if it does not exist."""
        ...

    async def upsert(self, task: Task) -> None:
        """Insert or update the task. Raises on infrastructure failure."""
        ...

    async def find_ancestor_chain(self, task_id: str, max_depth: int) -> ChainResult | None:
        """Return the ordered ancestor chain for task_id up to max_depth hops.

        Returns None if task_id does not exist.
        ChainResult.complete is False when a parent is missing or the depth
        limit is reached; ChainResult.cycle is True when a back-edge is found.
        """
        ...


class TaskChainCache(Protocol):
    """Read-through cache for resolved ancestor chains."""

    async def get(self, task_id: str) -> ChainResult | None:
        """Return the cached ChainResult, or None on miss or error."""
        ...

    async def set(self, task_id: str, result: ChainResult) -> None:
        """Store result under task_id. Silently skips on Redis error."""
        ...

    async def invalidate(self, task_id: str) -> None:
        """Delete the cached chain for task_id and all known descendants."""
        ...


class TaskChainServiceProtocol(Protocol):
    """Business logic for resolving a task's dependency chain."""

    async def get_chain(self, task_id: str) -> ChainResult | None:
        """Resolve and return the ancestor chain, or None if task_id does not exist."""
        ...
