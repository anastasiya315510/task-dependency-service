import json
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import RedisError

from app.models import Task
from app.services.cache_service import TaskChainCacheService


@pytest.fixture
def mock_redis():
    return AsyncMock()


@pytest.fixture
def cache(mock_redis) -> TaskChainCacheService:
    return TaskChainCacheService(mock_redis)


async def test_get_returns_none_when_key_missing(cache, mock_redis):
    mock_redis.get.return_value = None

    assert await cache.get("t1") is None


async def test_get_returns_deserialized_tasks(cache, mock_redis):
    tasks = [Task(id="t1", title="Root", parent_task_id=None)]
    mock_redis.get.return_value = json.dumps([t.model_dump() for t in tasks]).encode()

    result = await cache.get("t1")

    assert result is not None
    assert len(result) == 1
    assert result[0].id == "t1"


async def test_get_returns_none_on_redis_error(cache, mock_redis):
    mock_redis.get.side_effect = RedisError("connection refused")

    assert await cache.get("t1") is None  # graceful degradation


async def test_get_returns_none_on_corrupt_cache_entry(cache, mock_redis):
    mock_redis.get.return_value = b"not-json{{{"

    assert await cache.get("t1") is None


async def test_get_self_heals_corrupt_cache_entry(cache, mock_redis):
    """A corrupt entry must be deleted so it doesn't silently block future writes."""
    mock_redis.get.return_value = b"not-json{{{"

    await cache.get("t1")

    mock_redis.delete.assert_called_once_with("tds:chain:t1")


async def test_set_stores_serialized_tasks(cache, mock_redis):
    tasks = [Task(id="t1", title="Root", parent_task_id=None)]

    await cache.set("t2", tasks)

    mock_redis.set.assert_called_once()
    key, value = mock_redis.set.call_args.args[:2]
    assert key == "tds:chain:t2"
    assert json.loads(value)[0]["id"] == "t1"


async def test_set_does_not_raise_on_redis_error(cache, mock_redis):
    mock_redis.set.side_effect = RedisError("connection refused")
    tasks = [Task(id="t1", title="Root", parent_task_id=None)]

    await cache.set("t2", tasks)  # must not raise


async def test_invalidate_deletes_key(cache, mock_redis):
    await cache.invalidate("t1")

    mock_redis.delete.assert_called_once_with("tds:chain:t1")


async def test_invalidate_does_not_raise_on_redis_error(cache, mock_redis):
    mock_redis.delete.side_effect = RedisError("connection refused")

    await cache.invalidate("t1")  # must not raise


async def test_set_passes_ttl_to_redis(cache, mock_redis):
    from app.services.cache_service import _CHAIN_CACHE_TTL

    tasks = [Task(id="t1", title="Root", parent_task_id=None)]
    await cache.set("t2", tasks)

    kwargs = mock_redis.set.call_args.kwargs
    assert kwargs["ex"] == _CHAIN_CACHE_TTL


async def test_get_self_heals_on_valid_json_invalid_task_schema(cache, mock_redis):
    """Valid JSON that doesn't match Task schema (missing required 'title') triggers self-heal."""
    mock_redis.get.return_value = json.dumps([{"id": "t1"}]).encode()  # missing title

    result = await cache.get("t1")

    assert result is None
    mock_redis.delete.assert_called_once_with("tds:chain:t1")