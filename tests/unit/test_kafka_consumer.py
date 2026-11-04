import json

import pytest

from app.models import Task
from app.services.kafka_consumer import _process_message
from tests.conftest import FakeTaskChainCacheService, FakeTaskRepository


@pytest.fixture
def fake_repo() -> FakeTaskRepository:
    return FakeTaskRepository()


@pytest.fixture
def fake_cache() -> FakeTaskChainCacheService:
    return FakeTaskChainCacheService()


async def test_process_message_upserts_task(fake_repo, fake_cache):
    doc = {"id": "t1", "title": "Root", "parent_task_id": None}
    raw = json.dumps(doc).encode()

    await _process_message(raw, fake_repo, fake_cache)

    assert "t1" in fake_repo._tasks
    assert fake_repo._tasks["t1"].title == "Root"


async def test_process_message_invalidates_cache_after_upsert(fake_repo, fake_cache):
    doc = {"id": "t1", "title": "Root", "parent_task_id": None}
    raw = json.dumps(doc).encode()
    await fake_cache.set("t1", [Task(id="t1", title="Root", parent_task_id=None)])

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


async def test_process_message_invalidates_cache_before_upsert(fake_repo, fake_cache):
    """Cache must be cleared before the upsert so concurrent reads see old data
    (cache miss -> MongoDB) rather than stale cached data."""
    call_order: list[str] = []

    original_invalidate = fake_cache.invalidate
    original_upsert = fake_repo.upsert

    async def tracked_invalidate(task_id: str) -> None:
        call_order.append("invalidate")
        await original_invalidate(task_id)

    async def tracked_upsert(doc: dict) -> None:
        call_order.append("upsert")
        await original_upsert(doc)

    fake_cache.invalidate = tracked_invalidate
    fake_repo.upsert = tracked_upsert

    raw = json.dumps({"id": "t1", "title": "Root", "parent_task_id": None}).encode()
    await _process_message(raw, fake_repo, fake_cache)

    assert call_order == ["invalidate", "upsert"]


async def test_process_message_does_not_raise_when_upsert_fails(fake_repo, fake_cache):
    async def broken_upsert(doc: dict) -> None:
        raise ValueError("DB write failed")

    fake_repo.upsert = broken_upsert
    raw = json.dumps({"id": "t1", "title": "Root", "parent_task_id": None}).encode()

    await _process_message(raw, fake_repo, fake_cache)  # must not raise