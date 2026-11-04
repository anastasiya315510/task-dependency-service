import logging

from motor.motor_asyncio import AsyncIOMotorCollection
from pydantic import ValidationError

from app.log_call import log_call
from app.models import Task

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
    async def upsert(self, doc: dict) -> None:
        task_id = doc.get("id")
        if not task_id or not isinstance(task_id, str):
            raise ValueError(f"Task document missing or invalid 'id': {doc!r}")
        await self._collection.update_one(
            {"id": task_id},
            {"$set": doc},
            upsert=True,
        )
        logger.debug("upsert — task %r written to MongoDB", task_id)
