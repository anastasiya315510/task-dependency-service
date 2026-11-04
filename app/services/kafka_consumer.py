import asyncio
import json
import logging
import time

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from app.config import settings
from app.repositories.protocols import TaskChainCache, TaskRepository

logger = logging.getLogger(__name__)

_running: bool = False
_started_at: float | None = None  # set on first consume_forever call, not at import
_STARTUP_GRACE_SECONDS: float = 60.0


def is_healthy() -> bool:
    """Returns False only if the consumer stopped after the startup grace period."""
    if _running:
        return True
    if _started_at is None:
        return True  # consume_forever not yet called — still starting up
    return (time.monotonic() - _started_at) < _STARTUP_GRACE_SECONDS


async def _process_message(raw: bytes, repo: TaskRepository, cache: TaskChainCache) -> None:
    try:
        doc = json.loads(raw)
    except Exception:
        logger.error("_process_message — invalid JSON: %r", raw)
        return

    task_id = doc.get("id")
    if not task_id:
        logger.error("_process_message — message missing 'id' field: %s", doc)
        return

    logger.info("_process_message — processing task %r", task_id)
    # Invalidate before upsert: a concurrent read between the two operations
    # gets a cache miss and hits MongoDB (old-but-consistent), not stale cache.
    await cache.invalidate(task_id)
    try:
        await repo.upsert(doc)
    except Exception as exc:
        logger.error("_process_message — failed to upsert task %r: %s", task_id, exc)
        return
    logger.info("_process_message — task %r upserted and cache invalidated", task_id)


async def consume_forever(repo: TaskRepository, cache: TaskChainCache) -> None:
    global _running, _started_at

    # Record startup time once — drives the health-check grace period.
    if _started_at is None:
        _started_at = time.monotonic()

    while True:
        # Fresh consumer each attempt: aiokafka does not guarantee that
        # stop()/start() on the same instance is safe after a KafkaError.
        consumer = AIOKafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
        )
        try:
            await consumer.start()
            _running = True
            logger.info(
                "consume_forever — Kafka consumer started on topic %r",
                settings.kafka_topic,
            )
            async for message in consumer:
                logger.debug(
                    "consume_forever — received message partition=%s offset=%s",
                    message.partition,
                    message.offset,
                )
                # Create the inner task explicitly so we can await it after a
                # cancellation — asyncio.shield alone leaves it orphaned.
                inner = asyncio.ensure_future(
                    _process_message(message.value, repo, cache)
                )
                try:
                    await asyncio.shield(inner)
                except asyncio.CancelledError:
                    logger.info(
                        "consume_forever — shutdown during message processing, "
                        "waiting for current message to finish"
                    )
                    await inner  # _process_message never raises; let it finish
                    raise
        except KafkaError as exc:
            logger.error("consume_forever — Kafka error: %s — retrying in 5s", exc)
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.info("consume_forever — consumer cancelled, shutting down")
            break
        finally:
            _running = False
            await consumer.stop()
            logger.info("consume_forever — Kafka consumer stopped")