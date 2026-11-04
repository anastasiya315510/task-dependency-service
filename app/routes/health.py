import logging

from fastapi import APIRouter, HTTPException
from pymongo.errors import PyMongoError
from redis.exceptions import RedisError

from app.db import get_redis, get_tasks_collection
from app.services.kafka_consumer import is_healthy as consumer_is_healthy

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict:
    """Liveness + dependency probe. Returns 503 if any backing service is unreachable."""
    errors: list[str] = []

    try:
        await get_tasks_collection().database.command("ping")
    except PyMongoError as exc:
        logger.error("health — MongoDB ping failed: %s", exc)
        errors.append("mongodb")

    try:
        await get_redis().ping()
    except RedisError as exc:
        logger.error("health — Redis ping failed: %s", exc)
        errors.append("redis")

    if not consumer_is_healthy():
        logger.error("health — Kafka consumer is not running")
        errors.append("kafka_consumer")

    if errors:
        raise HTTPException(
            status_code=503,
            detail=f"Unavailable: {', '.join(errors)}",
        )

    return {"status": "ok"}
