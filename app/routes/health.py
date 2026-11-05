import asyncio
import logging

from fastapi import APIRouter, HTTPException, Request

from app.db import get_redis, get_tasks_collection
from app.dependencies import limiter

logger = logging.getLogger(__name__)

# /health is intentionally unauthenticated — required for load-balancer health probes.
# Do NOT add sensitive runtime data to this response.
router = APIRouter(tags=["health"])


@router.get("/health/live")
async def health_live() -> dict:
    """Liveness probe — returns 200 as long as the process is responding.

    Use this for container/orchestrator liveness checks so a temporary
    MongoDB or Redis outage does not trigger a pod restart. Wire the
    readiness probe to GET /health instead.
    """
    return {"status": "ok"}


@router.get("/health")
@limiter.limit("30/minute")
async def health(request: Request) -> dict:
    """Readiness probe — returns 503 if any backing service is unreachable."""
    errors: list[str] = []

    async def _ping_mongo() -> None:
        await get_tasks_collection().database.command("ping")

    async def _ping_redis() -> None:
        await get_redis().ping()

    try:
        mongo_result, redis_result = await asyncio.wait_for(
            asyncio.gather(_ping_mongo(), _ping_redis(), return_exceptions=True),
            timeout=5.0,
        )
    except asyncio.TimeoutError as exc:
        logger.error("health — dependency ping timed out")
        raise HTTPException(status_code=503, detail="Unavailable: ping timeout") from exc

    if isinstance(mongo_result, Exception):
        logger.error("health — MongoDB ping failed: %s", mongo_result)
        errors.append("mongodb")

    if isinstance(redis_result, Exception):
        logger.error("health — Redis ping failed: %s", redis_result)
        errors.append("redis")

    runner = getattr(request.app.state, "kafka_runner", None)
    if runner is not None and not runner.is_healthy():
        logger.error("health — Kafka consumer is not running")
        errors.append("kafka_consumer")

    if errors:
        raise HTTPException(
            status_code=503,
            detail=f"Unavailable: {', '.join(errors)}",
        )

    return {"status": "ok"}
