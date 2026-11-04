import asyncio
import enum
import json
import logging
import time

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from pydantic import ValidationError

from app.config import settings
from app.models import Task
from app.repositories.protocols import TaskChainCache, TaskRepository

logger = logging.getLogger(__name__)

_STARTUP_GRACE_SECONDS: float = 60.0
_RETRY_BASE_SECONDS: float = 5.0
_RETRY_MAX_SECONDS: float = 60.0
_MAX_MESSAGE_BYTES: int = 1 * 1024 * 1024  # 1 MiB; tune via broker max.message.bytes


class RunnerState(enum.Enum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    BACKOFF = "backoff"   # sleeping between reconnect attempts; not an error condition
    STOPPED = "stopped"


async def _process_message(raw: bytes, repo: TaskRepository, cache: TaskChainCache) -> None:
    if len(raw) > _MAX_MESSAGE_BYTES:
        logger.error("_process_message — message too large (%d bytes), skipping", len(raw))
        return
    try:
        doc = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        logger.error("_process_message — invalid JSON: %r", raw)
        return

    # Task.id has a default_factory, so without this guard a message missing "id"
    # would silently create a phantom task with a random UUID instead of being rejected.
    # isinstance check catches non-string IDs (e.g. 0, false) that would pass a falsy test
    # but fail Pydantic validation, swallowing a producer bug with no actionable error.
    if not isinstance(doc.get("id"), str) or not doc["id"]:
        logger.error("_process_message — missing or non-string 'id': %s", doc)
        return

    try:
        task = Task(**{k: v for k, v in doc.items() if k in Task.model_fields})
    except (ValidationError, TypeError) as exc:
        logger.error("_process_message — invalid task document, skipping: %s — %s", doc, exc)
        return

    logger.info("_process_message — processing task %r", task.id)
    # First invalidation: concurrent reads that arrive now will fall through to
    # MongoDB rather than seeing stale cached data.
    await cache.invalidate(task.id)
    # Upsert raises on infrastructure failure — the caller must NOT commit the
    # Kafka offset so the message is redelivered and retried.
    await repo.upsert(task)
    # Second invalidation: evicts any stale cache entry that a concurrent chain
    # request may have written between the two invalidations, closing the race
    # window from "up to TTL (1 h)" down to "microseconds".
    # NOTE: a narrow residual window remains — a reader that started its
    # $graphLookup *before* the first invalidation but writes to cache *after*
    # the second invalidation will cache stale data until the next task update
    # or the TTL expires. Eliminating this fully requires distributed atomics;
    # the 1-hour TTL is the backstop for this service.
    await cache.invalidate(task.id)
    logger.info(
        "_process_message — task %r upserted, cache invalidated (known descendants cleared)",
        task.id,
    )


class KafkaConsumerRunner:
    """Encapsulates Kafka consumer lifecycle and health state."""

    def __init__(self) -> None:
        self._state: RunnerState = RunnerState.NOT_STARTED
        self._started_at: float | None = None

    @classmethod
    def _create_for_testing(
        cls,
        state: RunnerState,
        started_at: float | None = None,
    ) -> "KafkaConsumerRunner":
        """Construct a runner in a specific state for unit tests.

        Using this factory keeps tests decoupled from the names of the
        individual state fields — only the public enum values are part of
        the test contract.
        """
        runner = cls()
        runner._state = state
        runner._started_at = started_at
        return runner

    def is_healthy(self) -> bool:
        """Returns False only if the consumer stopped after the startup grace period."""
        if self._state in (RunnerState.RUNNING, RunnerState.BACKOFF):
            return True
        if self._state is RunnerState.NOT_STARTED:
            return True  # run() not yet called — still starting up
        # STOPPED
        if self._started_at is None:
            return True
        return (time.monotonic() - self._started_at) < _STARTUP_GRACE_SECONDS

    async def run(self, repo: TaskRepository, cache: TaskChainCache) -> None:
        # Record startup time once — drives the health-check grace period.
        if self._started_at is None:
            self._started_at = time.monotonic()

        retry_delay = _RETRY_BASE_SECONDS
        while True:
            # Fresh consumer each attempt: aiokafka does not guarantee that
            # stop()/start() on the same instance is safe after a KafkaError.
            consumer = AIOKafkaConsumer(
                settings.kafka_topic,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=settings.kafka_consumer_group,
                auto_offset_reset="earliest",
                # Manual commit: offset is committed only after a successful upsert.
                # If _process_message raises (e.g. MongoDB down), the offset is not
                # committed and the message is redelivered after reconnect (at-least-once).
                enable_auto_commit=False,
            )
            try:
                await consumer.start()
                self._state = RunnerState.RUNNING
                retry_delay = _RETRY_BASE_SECONDS
                logger.info(
                    "KafkaConsumerRunner.run — Kafka consumer started on topic %r",
                    settings.kafka_topic,
                )
                async for message in consumer:
                    logger.debug(
                        "KafkaConsumerRunner.run — received message partition=%s offset=%s",
                        message.partition,
                        message.offset,
                    )
                    # Create the inner task explicitly so we can await it after a
                    # cancellation — asyncio.shield alone leaves it orphaned.
                    inner = asyncio.create_task(
                        _process_message(message.value, repo, cache)
                    )
                    try:
                        await asyncio.shield(inner)
                    except asyncio.CancelledError:
                        logger.info(
                            "KafkaConsumerRunner.run — shutdown during message processing, "
                            "waiting for current message to finish"
                        )
                        try:
                            await inner
                        except Exception as _exc:
                            logger.warning(
                                "KafkaConsumerRunner.run — in-flight message failed during shutdown: %s"
                                " — message will be redelivered on restart",
                                _exc,
                            )
                        # At-least-once trade-off: even if inner succeeded here, we
                        # re-raise CancelledError without committing the offset so the
                        # message is redelivered on restart rather than risking a
                        # partial shutdown state.
                        raise
                    # Commit only after _process_message returned without raising.
                    await consumer.commit()
            except asyncio.CancelledError:
                logger.info("KafkaConsumerRunner.run — consumer cancelled, shutting down")
                break
            except Exception as exc:
                # Covers KafkaError and upsert failures — both warrant a reconnect
                # so the unprocessed message is redelivered from the last committed offset.
                logger.error(
                    "KafkaConsumerRunner.run — error: %s — retrying in %.0fs",
                    exc,
                    retry_delay,
                )
                self._state = RunnerState.BACKOFF
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, _RETRY_MAX_SECONDS)
            finally:
                await consumer.stop()
                self._state = RunnerState.STOPPED  # set after stop() so health checks stay green during graceful drain
                logger.info("KafkaConsumerRunner.run — Kafka consumer stopped")

