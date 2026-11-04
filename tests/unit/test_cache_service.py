import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.exceptions import RedisError

from app.models import ChainResult, Task
from app.services.cache_service import TaskChainCacheService

pytestmark = pytest.mark.unit


def _make_pipe():
    pipe = MagicMock()
    pipe.set = MagicMock()
    pipe.sadd = MagicMock()
    pipe.expire = MagicMock()
    pipe.execute = AsyncMock(return_value=[])
    return pipe


@pytest.fixture
def mock_redis():
    m = AsyncMock()
    m.smembers.return_value = set()
    m.pipeline = MagicMock(return_value=_make_pipe())
    return m


@pytest.fixture
def cache(mock_redis) -> TaskChainCacheService:
    return TaskChainCacheService(mock_redis)


async def test_get_returns_none_when_key_missing(cache, mock_redis):
    mock_redis.get.return_value = None

    assert await cache.get("t1") is None


async def test_get_returns_deserialized_tasks(cache, mock_redis):
    tasks = [Task(id="t1", title="Root", parent_task_id=None)]
    payload = json.dumps({"chain": [t.model_dump(mode="json") for t in tasks], "complete": True, "cycle": False})
    mock_redis.get.return_value = payload

    result = await cache.get("t1")

    assert result is not None
    chain, complete, cycle = result
    assert len(chain) == 1
    assert chain[0].id == "t1"
    assert complete is True
    assert cycle is False


async def test_get_returns_none_on_redis_error(cache, mock_redis):
    mock_redis.get.side_effect = RedisError("connection refused")

    assert await cache.get("t1") is None  # graceful degradation


async def test_get_returns_none_on_corrupt_cache_entry(cache, mock_redis):
    mock_redis.get.return_value = "not-json{{{"

    assert await cache.get("t1") is None


async def test_get_self_heals_corrupt_cache_entry(cache, mock_redis):
    """A corrupt entry must be deleted so it doesn't silently block future writes."""
    mock_redis.get.return_value = "not-json{{{"

    await cache.get("t1")

    deleted = set(mock_redis.delete.call_args.args)
    assert "tds:chain:t1" in deleted
    assert "tds:members:t1" in deleted


async def test_get_self_heals_on_missing_key_in_payload(cache, mock_redis):
    """Valid JSON missing the 'chain' key triggers self-heal."""
    mock_redis.get.return_value = json.dumps({"complete": True})

    result = await cache.get("t1")

    assert result is None
    mock_redis.delete.assert_called()


async def test_set_stores_serialized_tasks(cache, mock_redis):
    tasks = [Task(id="t1", title="Root", parent_task_id=None)]

    await cache.set("t2", ChainResult(tasks=tasks, complete=True, cycle=False))

    pipe = mock_redis.pipeline.return_value
    pipe.set.assert_called_once()
    key, value = pipe.set.call_args.args[:2]
    assert key == "tds:chain:t2"
    data = json.loads(value)
    assert data["chain"][0]["id"] == "t1"
    assert data["complete"] is True
    assert data["cycle"] is False


async def test_set_does_not_raise_on_redis_error(cache, mock_redis):
    mock_redis.pipeline.return_value.execute.side_effect = RedisError("connection refused")
    tasks = [Task(id="t1", title="Root", parent_task_id=None)]

    await cache.set("t2", ChainResult(tasks=tasks, complete=True, cycle=False))  # must not raise


async def test_invalidate_deletes_own_chain_and_members_key(cache, mock_redis):
    mock_redis.smembers.return_value = set()

    await cache.invalidate("t1")

    mock_redis.delete.assert_called_once_with("tds:chain:t1", "tds:members:t1")


async def test_invalidate_also_deletes_descendant_chains(cache, mock_redis):
    """Chains that list t1 as an ancestor must also be cleared, along with their members keys."""
    mock_redis.smembers.return_value = {"t3", "t4"}

    await cache.invalidate("t1")

    deleted = set(mock_redis.delete.call_args.args)
    assert "tds:chain:t1" in deleted
    assert "tds:members:t1" in deleted
    assert "tds:chain:t3" in deleted
    assert "tds:members:t3" in deleted
    assert "tds:chain:t4" in deleted
    assert "tds:members:t4" in deleted


async def test_invalidate_does_not_raise_on_redis_error(cache, mock_redis):
    mock_redis.smembers.side_effect = RedisError("connection refused")

    await cache.invalidate("t1")  # must not raise


async def test_set_passes_ttl_to_redis(cache, mock_redis):
    from app.services.cache_service import _CHAIN_CACHE_TTL

    tasks = [Task(id="t1", title="Root", parent_task_id=None)]
    await cache.set("t2", ChainResult(tasks=tasks, complete=True, cycle=False))

    pipe = mock_redis.pipeline.return_value
    kwargs = pipe.set.call_args.kwargs
    assert kwargs["ex"] == _CHAIN_CACHE_TTL


async def test_set_registers_ancestor_in_members_index(cache, mock_redis):
    """set() must record that t2's chain includes t1 so invalidate(t1) can clear t2's chain."""
    from app.services.cache_service import _CHAIN_CACHE_TTL

    chain = [Task(id="t1", title="Root", parent_task_id=None)]

    await cache.set("t2", ChainResult(tasks=chain, complete=True, cycle=False))

    pipe = mock_redis.pipeline.return_value
    pipe.sadd.assert_called_with("tds:members:t1", "t2")
    pipe.expire.assert_called_with("tds:members:t1", _CHAIN_CACHE_TTL)


async def test_set_uses_pipeline_for_all_writes(cache, mock_redis):
    """All write commands must be batched into one pipeline execute() call."""
    chain = [
        Task(id="t1", title="Root", parent_task_id=None),
        Task(id="t2", title="Middle", parent_task_id="t1"),
    ]

    await cache.set("t3", ChainResult(tasks=chain, complete=True, cycle=False))

    pipe = mock_redis.pipeline.return_value
    # 1 set + 2×(sadd + expire) = 5 queued commands, all flushed in one execute
    pipe.execute.assert_awaited_once()
    assert pipe.set.call_count == 1
    assert pipe.sadd.call_count == 2
    assert pipe.expire.call_count == 2


async def test_set_uses_short_ttl_for_incomplete_chain(cache, mock_redis):
    """Incomplete chains must use _INCOMPLETE_CHAIN_CACHE_TTL, not _CHAIN_CACHE_TTL."""
    from app.services.cache_service import _INCOMPLETE_CHAIN_CACHE_TTL

    await cache.set("t1", ChainResult(tasks=[], complete=False, cycle=False))

    pipe = mock_redis.pipeline.return_value
    kwargs = pipe.set.call_args.kwargs
    assert kwargs["ex"] == _INCOMPLETE_CHAIN_CACHE_TTL


async def test_set_skips_members_index_for_incomplete_chain(cache, mock_redis):
    """Incomplete chains must not register ancestors in the members index."""
    tasks = [Task(id="t0", title="Root", parent_task_id=None)]
    await cache.set("t1", ChainResult(tasks=tasks, complete=False, cycle=False))

    pipe = mock_redis.pipeline.return_value
    pipe.sadd.assert_not_called()
    pipe.expire.assert_not_called()


async def test_get_self_heals_on_valid_json_invalid_task_schema(cache, mock_redis):
    """Valid JSON that doesn't match Task schema (missing required 'title') triggers self-heal."""
    mock_redis.get.return_value = json.dumps({"chain": [{"id": "t1"}], "complete": True, "cycle": False})

    result = await cache.get("t1")

    assert result is None
    deleted = set(mock_redis.delete.call_args.args)
    assert "tds:chain:t1" in deleted
    assert "tds:members:t1" in deleted