import json
import time

import pytest

from app.models import ChainResult, Task
from app.services.kafka_consumer import KafkaConsumerRunner, RunnerState, _MAX_BACKOFF_SECONDS, _STARTUP_GRACE_SECONDS, _process_message
from tests.conftest import FakeTaskRepository

pytestmark = pytest.mark.unit


async def test_process_message_upserts_task(fake_repo, fake_cache):
    doc = {"id": "t1", "title": "Root", "parent_task_id": None}
    raw = json.dumps(doc).encode()

    await _process_message(raw, fake_repo, fake_cache)

    assert "t1" in fake_repo._tasks
    assert fake_repo._tasks["t1"].title == "Root"


async def test_process_message_invalidates_cache_after_upsert(fake_repo, fake_cache):
    doc = {"id": "t1", "title": "Root", "parent_task_id": None}
    raw = json.dumps(doc).encode()
    await fake_cache.set("t1", ChainResult(tasks=[Task(id="t1", title="Root", parent_task_id=None)], complete=True, cycle=False))

    await _process_message(raw, fake_repo, fake_cache)

    assert await fake_cache.get("t1") is None


async def test_process_message_idempotent_on_replay(fake_repo, fake_cache):
    """Replaying the same event twice must not duplicate the task."""
    doc = {"id": "t1", "title": "Root", "parent_task_id": None}
    raw = json.dumps(doc).encode()

    await _process_message(raw, fake_repo, fake_cache)
    await _process_message(raw, fake_repo, fake_cache)

    assert len([t for t in fake_repo._tasks if t == "t1"]) == 1


async def test_process_message_ignores_invalid_json(fake_repo, fake_cache):
    await _process_message(b"not-valid-json{{{", fake_repo, fake_cache)

    assert fake_repo._tasks == {}


async def test_process_message_ignores_message_missing_id(fake_repo, fake_cache):
    raw = json.dumps({"title": "Task without id"}).encode()

    await _process_message(raw, fake_repo, fake_cache)

    assert fake_repo._tasks == {}


async def test_process_message_invalidates_cache_around_upsert(fake_repo, fake_cache):
    """Cache is targeted-invalidated both before and after the upsert.
    Before: concurrent reads fall through to MongoDB instead of serving stale data.
    After: evicts any stale cache entry written between the two invalidations,
    closing the race window from 'up to TTL' down to microseconds."""
    call_order: list[str] = []

    original_invalidate = fake_cache.invalidate
    original_upsert = fake_repo.upsert

    async def tracked_invalidate(task_id: str) -> None:
        call_order.append(f"invalidate:{task_id}")
        await original_invalidate(task_id)

    async def tracked_upsert(task) -> None:
        call_order.append("upsert")
        await original_upsert(task)

    fake_cache.invalidate = tracked_invalidate
    fake_repo.upsert = tracked_upsert

    raw = json.dumps({"id": "t1", "title": "Root", "parent_task_id": None}).encode()
    await _process_message(raw, fake_repo, fake_cache)

    assert call_order == ["invalidate:t1", "upsert", "invalidate:t1"]


async def test_process_message_raises_when_upsert_fails(fake_repo, fake_cache):
    """Infrastructure failures must propagate so the caller skips the Kafka commit
    and the message is redelivered (at-least-once delivery)."""
    async def broken_upsert(task) -> None:
        raise ValueError("DB write failed")

    fake_repo.upsert = broken_upsert
    raw = json.dumps({"id": "t1", "title": "Root", "parent_task_id": None}).encode()

    with pytest.raises(ValueError, match="DB write failed"):
        await _process_message(raw, fake_repo, fake_cache)


async def test_process_message_ignores_message_missing_title(fake_repo, fake_cache):
    """A message that fails Task validation (missing required 'title') must be skipped."""
    raw = json.dumps({"id": "t1"}).encode()

    await _process_message(raw, fake_repo, fake_cache)

    assert fake_repo._tasks == {}


async def test_process_message_ignores_message_with_empty_string_parent(fake_repo, fake_cache):
    """parent_task_id='' is rejected by Task validation (min_length=1)."""
    raw = json.dumps({"id": "t1", "title": "Root", "parent_task_id": ""}).encode()

    await _process_message(raw, fake_repo, fake_cache)

    assert fake_repo._tasks == {}


def test_is_healthy_before_run_called():
    runner = KafkaConsumerRunner._create_for_testing(RunnerState.NOT_STARTED)
    assert runner.is_healthy() is True


def test_is_healthy_while_running():
    runner = KafkaConsumerRunner._create_for_testing(RunnerState.RUNNING)
    assert runner.is_healthy() is True


def test_is_healthy_stopped_within_grace_period():
    runner = KafkaConsumerRunner._create_for_testing(
        RunnerState.STOPPED,
        started_at=time.monotonic() - (_STARTUP_GRACE_SECONDS / 2),
    )
    assert runner.is_healthy() is True


def test_is_healthy_stopped_past_grace_period():
    runner = KafkaConsumerRunner._create_for_testing(
        RunnerState.STOPPED,
        started_at=time.monotonic() - (_STARTUP_GRACE_SECONDS + 1),
    )
    assert runner.is_healthy() is False


def test_is_healthy_while_in_backoff():
    """Consumer is considered healthy during a recent reconnect back-off."""
    runner = KafkaConsumerRunner._create_for_testing(
        RunnerState.BACKOFF,
        backoff_started_at=time.monotonic() - (_MAX_BACKOFF_SECONDS / 2),
    )
    assert runner.is_healthy() is True


def test_is_healthy_backoff_past_max_threshold():
    """Consumer is unhealthy when it has been in BACKOFF longer than the allowed maximum."""
    runner = KafkaConsumerRunner._create_for_testing(
        RunnerState.BACKOFF,
        backoff_started_at=time.monotonic() - (_MAX_BACKOFF_SECONDS + 1),
    )
    assert runner.is_healthy() is False