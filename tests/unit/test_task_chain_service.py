import pytest

from app.models import Task
from app.services.task_chain_service import MAX_CHAIN_DEPTH, TaskChainService
from tests.conftest import FakeTaskChainCacheService, FakeTaskRepository


def _service(tasks: dict[str, Task], cached: dict[str, list[Task]] | None = None):
    repo = FakeTaskRepository(tasks)
    cache = FakeTaskChainCacheService()
    if cached:
        cache._store.update(cached)
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
    assert result.complete is True  # loop terminated cleanly, not due to missing parent


async def test_get_chain_returns_partial_chain_when_parent_not_ingested():
    tasks = {"t2": Task(id="t2", title="Middle", parent_task_id="t1-not-here")}
    service, _, _ = _service(tasks)

    result = await service.get_chain("t2")

    assert result is not None
    assert result.tasks == []
    assert result.complete is False  # distinguishable from a true root task


async def test_get_chain_returns_cached_result_without_hitting_repo():
    cached_chain = [Task(id="t1", title="Root", parent_task_id=None)]
    # t2 is NOT in repo — proves cache is used, not repo
    service, _, _ = _service({}, cached={"t2": cached_chain})

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

    assert "t2" in cache._store
    assert cache._store["t2"][0].id == "t1"


async def test_get_chain_does_not_cache_partial_chain():
    # t2 references t1 but t1 is not ingested yet — chain is partial
    tasks = {"t2": Task(id="t2", title="Child", parent_task_id="t1-missing")}
    service, _, cache = _service(tasks)

    result = await service.get_chain("t2")

    assert result is not None
    assert result.tasks == []
    assert result.complete is False
    assert "t2" not in cache._store  # must NOT be cached


async def test_get_chain_stops_at_max_depth():
    """A chain longer than MAX_CHAIN_DEPTH must not hang and must not be cached."""
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
    assert len(result.tasks) == MAX_CHAIN_DEPTH
    assert result.complete is False
    assert last_id not in cache._store


# --- Model validation tests ---

def test_task_model_rejects_self_referential_parent():
    with pytest.raises(ValueError, match="cannot be its own parent"):
        Task(id="t1", title="Self-loop", parent_task_id="t1")


def test_task_model_rejects_extra_fields():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Task(id="t1", title="Extra", parent_task_id=None, unknown_field="bad")  # type: ignore[call-arg]