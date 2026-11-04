import logging
from typing import Any

from motor.motor_asyncio import AsyncIOMotorCollection
from pydantic import ValidationError

from app.log_call import log_call
from app.models import ChainResult, Task

logger = logging.getLogger(__name__)

_TASK_FIELDS = set(Task.model_fields)


class MongoTaskRepository:
    def __init__(self, collection: AsyncIOMotorCollection):
        self._collection = collection

    @log_call
    async def find_by_id(self, task_id: str) -> Task | None:
        doc = await self._collection.find_one({"id": task_id})
        if doc is None:
            logger.debug("find_by_id %r — not found", task_id)
            return None
        # Filter to known fields: handles documents written before schema changes
        # and prevents hard failures on schema evolution or corrupted documents.
        known = {k: v for k, v in doc.items() if k in _TASK_FIELDS}
        try:
            task = Task(**known)
        except ValidationError as exc:
            logger.error(
                "find_by_id %r — document fails validation, treating as missing: %s",
                task_id,
                exc,
            )
            return None
        logger.debug("find_by_id %r — found", task_id)
        return task

    @log_call
    async def upsert(self, task: Task) -> None:
        doc = task.model_dump(mode="json")
        await self._collection.update_one(
            {"id": task.id},
            {"$set": doc},
            upsert=True,
        )
        logger.debug("upsert — task %r written to MongoDB", task.id)

    @log_call
    async def find_ancestor_chain(
        self, task_id: str, max_depth: int
    ) -> ChainResult | None:
        """Return (ancestors_ordered, is_complete, is_cycle) in a single $graphLookup query.

        Replaces the previous per-ancestor find_by_id loop (N+1 queries) with one
        aggregation pipeline that fetches the entire chain in one round-trip.
        Returns None if the task itself does not exist.
        """
        if max_depth <= 0:
            raise ValueError(f"max_depth must be >= 1, got {max_depth}")
        pipeline: list[dict[str, Any]] = [
            {"$match": {"id": task_id}},
            {
                "$graphLookup": {
                    "from": self._collection.name,
                    "startWith": "$parent_task_id",
                    "connectFromField": "parent_task_id",
                    "connectToField": "id",
                    "as": "_ancestors",
                    # maxDepth 0 = direct parent only; N-1 = up to N ancestors.
                    "maxDepth": max_depth - 1,
                    "depthField": "_depth",
                }
            },
            # id projected explicitly for clarity; only parent_task_id is used from root_doc.
            {"$project": {"id": 1, "parent_task_id": 1, "_ancestors": 1, "_id": 0}},
        ]
        cursor = self._collection.aggregate(pipeline)  # type: ignore[arg-type]
        docs = await cursor.to_list(length=1)
        if not docs:
            logger.debug("find_ancestor_chain %r — task not found", task_id)
            return None

        root_doc = docs[0]
        # Exclude the queried task itself before building seen_ids: $graphLookup
        # starts from parent_task_id so task_id normally won't appear, but a cycle
        # back to the root would include it, causing the cycle-closing node to be
        # silently dropped from the returned chain.
        ancestor_docs: list[dict] = [
            d for d in root_doc.get("_ancestors", []) if d.get("id") != task_id
        ]
        ancestor_docs.sort(key=lambda d: d.get("_depth", 0))

        ancestors: list[Task] = []
        # Seed seen_ids with task_id so a back-edge to the queried task (e.g.
        # t3→t2→t3) is detected even though task_id is filtered from ancestor_docs.
        seen_ids: set[str] = {task_id}
        cycle_at: str | None = None
        for doc in ancestor_docs:
            known = {k: v for k, v in doc.items() if k in _TASK_FIELDS}
            try:
                task = Task(**known)
            except ValidationError as exc:
                logger.error(
                    "find_ancestor_chain %r — ancestor %r fails validation, treating as incomplete: %s",
                    task_id,
                    doc.get("id"),
                    exc,
                )
                return ChainResult(ancestors, False, False)
            # Check for a back-edge BEFORE adding the current node: if this
            # ancestor's parent is already in seen_ids, it points to a shallower
            # node (or the queried task itself), which is a cycle.  Checking
            # after building the full seen_ids would fire for every valid
            # parent→child edge in a linear chain.
            if task.parent_task_id is not None and task.parent_task_id in seen_ids:
                cycle_at = task.parent_task_id
            ancestors.append(task)
            seen_ids.add(task.id)

        if not ancestors:
            # No ancestors fetched — either a genuine root or the parent is missing.
            is_root = root_doc.get("parent_task_id") is None
            logger.debug(
                "find_ancestor_chain %r — %s",
                task_id,
                "root task" if is_root else "parent not yet ingested",
            )
            return ChainResult(ancestors, is_root, False)

        cycle = cycle_at is not None
        last = ancestors[-1]
        if cycle:
            complete = False
            logger.warning("find_ancestor_chain %r — cycle detected at %r", task_id, cycle_at)
        elif last.parent_task_id is None:
            complete = True
            logger.debug("find_ancestor_chain %r — complete, %d ancestor(s)", task_id, len(ancestors))
        elif len(ancestors) >= max_depth:
            # Could also be a missing parent at exactly max_depth — the two cases
            # are indistinguishable without an extra query. Treat conservatively
            # as incomplete; do NOT log at warning level to avoid false alarms
            # during normal out-of-order ingestion.
            complete = False
            logger.debug(
                "find_ancestor_chain %r — depth limit %d reached (or missing parent at that depth)",
                task_id,
                max_depth,
            )
        else:
            complete = False
            logger.debug(
                "find_ancestor_chain %r — parent %r not yet ingested",
                task_id,
                last.parent_task_id,
            )
        return ChainResult(ancestors, complete, cycle)
