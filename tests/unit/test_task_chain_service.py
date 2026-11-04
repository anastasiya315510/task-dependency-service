import fakeredis.aioredis
import pytest

from app.models import ChainResult, Task
from app.services.cache_service import TaskChainCacheService
from app.services.task_chain_service import MAX_CHAIN_DEPTH, TaskChainService
from tests.conftest import FakeTaskRepository

pytestmark = pytest.mark.unit


def _make_cache() -> TaskChainCacheService:
    return TaskChainCacheService(fakeredis.aioredis.FakeRedis(decode_responses=True))


def _service(tasks: dict[str, Task], cache: TaskChainCacheService | None = None):
    repo = FakeTaskRepository(tasks)
    if cache is None:
        cache = _make_cache()
    return TaskChainService(repo, cache), repo, cache


async def test_get_chain_returns_none_when_task_not_found():
    service, _, _ = _service({})
    assert await service.get_chain("missing") is None


async def test_get_chain_returns_empty_list_for_root_task():
    service, _, _ = _service({"t1": Task(id="t1", title="Root", parent_task_id=None)})
    result = await service.get_chain("t1")
    assert result is not None
    assert result.tasks == []
    assert result.complete is True


async def test_get_chain_returns_ordered_parents():
    tasks = {
        "t1": Task(id="t1", title="Root", parent_task_id=None),
        "t2": Task(id="t2", title="Middle", parent_task_id="t1"),
        "t3": Task(id="t3", title="Leaf", parent_task_id="t2"),
    }
    service, _, _ = _service(tasks)

    result = await service.get_chain("t3")

    assert result is not None
    assert [t.id for t in result.tasks] == ["t2", "t1"]
    assert result.complete is True


async def test_get_chain_stops_safely_on_circular_dependency():
    tasks = {
        "t1": Task(id="t1", title="A", parent_task_id="t2"),
        "t2": Task(id="t2", title="B", parent_task_id="t1"),  # circular
    }
    service, _, _ = _service(tasks)

    result = await service.get_chain("t1")

    assert result is not None
    assert len(result.tasks) == 1
    assert result.tasks[0].id == "t2"
    # A cycle is not a complete chain — it never reached a true root.
    assert result.complete is False
    assert result.cycle is True


async def test_get_chain_cycle_is_not_cached():
    tasks = {
        "t1": Task(id="t1", title="A", parent_task_id="t2"),
        "t2": Task(id="t2", title="B", parent_task_id="t1"),
    }
    service, _, cache = _service(tasks)

    await service.get_chain("t1")

    assert await cache.get("t1") is None


async def test_get_chain_returns_partial_chain_when_parent_not_ingested():
    tasks = {"t2": Task(id="t2", title="Middle", parent_task_id="t1-not-here")}
    service, _, _ = _service(tasks)

    result = await service.get_chain("t2")

    assert result is not None
    assert result.tasks == []
    assert result.complete is False  # distinguishable from a true root task


async def test_get_chain_returns_cached_result_without_hitting_repo():
    cached_chain = [Task(id="t1", title="Root", parent_task_id=None)]
    cache = _make_cache()
    # Pre-populate cache — t2 is NOT in repo, proves cache is used, not repo
    await cache.set("t2", ChainResult(tasks=cached_chain, complete=True, cycle=False))
    service, _, _ = _service({}, cache=cache)

    result = await service.get_chain("t2")

    assert result is not None
    assert result.tasks == cached_chain
    assert result.complete is True  # cached results are always complete


async def test_get_chain_writes_result_to_cache():
    tasks = {
        "t1": Task(id="t1", title="Root", parent_task_id=None),
        "t2": Task(id="t2", title="Child", parent_task_id="t1"),
    }
    service, _, cache = _service(tasks)

    await service.get_chain("t2")

    cached = await cache.get("t2")
    assert cached is not None
    assert cached.tasks[0].id == "t1"
    assert cached.complete is True
    assert cached.cycle is False


async def test_get_chain_caches_partial_chain_as_incomplete():
    """Partial chains are cached (marked incomplete) to reduce MongoDB pressure
    during Kafka ingestion lag. TTL is set on the Redis key but fakeredis does
    not auto-advance time, so expiry is not asserted here."""
    tasks = {"t2": Task(id="t2", title="Child", parent_task_id="t1-missing")}
    service, _, cache = _service(tasks)

    result = await service.get_chain("t2")

    assert result is not None
    assert result.tasks == []
    assert result.complete is False
    cached = await cache.get("t2")
    assert cached is not None
    assert cached.complete is False


async def test_get_chain_stops_at_max_depth():
    """A chain longer than MAX_CHAIN_DEPTH must not hang; result is cached as incomplete."""
    tasks: dict[str, Task] = {}
    prev_id: str | None = None
    for i in range(MAX_CHAIN_DEPTH + 5):
        tid = f"deep-{i}"
        tasks[tid] = Task(id=tid, title=f"Task {i}", parent_task_id=prev_id)
        prev_id = tid

    last_id = f"deep-{MAX_CHAIN_DEPTH + 4}"
    service, _, cache = _service(tasks)

    result = await service.get_chain(last_id)

    assert result is not None
    # Both FakeTaskRepository and MongoTaskRepository's $graphLookup (maxDepth=max_depth-1)
    # return exactly max_depth ancestors when the depth limit is hit.
    assert len(result.tasks) == MAX_CHAIN_DEPTH
    assert result.complete is False
    cached = await cache.get(last_id)
    assert cached is not None
    assert cached.complete is False


async def test_get_chain_cascade_invalidation():
    """Updating an ancestor must clear cached chains of all its descendants."""
    tasks = {
        "t1": Task(id="t1", title="Root", parent_task_id=None),
        "t2": Task(id="t2", title="Child", parent_task_id="t1"),
        "t3": Task(id="t3", title="Grandchild", parent_task_id="t2"),
    }
    service, _, cache = _service(tasks)

    # Cache chains for t2 and t3
    await service.get_chain("t2")
    await service.get_chain("t3")
    assert await cache.get("t2") is not None
    assert await cache.get("t3") is not None

    # Invalidating t1 (an ancestor) must clear t2's and t3's chains
    await cache.invalidate("t1")

    assert await cache.get("t2") is None
    assert await cache.get("t3") is None


# --- Model validation tests ---

def test_task_model_rejects_self_referential_parent():
    with pytest.raises(ValueError, match="cannot be its own parent"):
        Task(id="t1", title="Self-loop", parent_task_id="t1")


def test_task_model_rejects_extra_fields():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Task(id="t1", title="Extra", parent_task_id=None, unknown_field="bad")  # type: ignore[call-arg]