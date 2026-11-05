"""Unit tests for MongoTaskRepository using a fake Motor collection."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models import Task
from app.repositories.mongo_task_repository import MongoTaskRepository

pytestmark = pytest.mark.unit


def _make_repo(find_one_return=None, update_one_side_effect=None):
    collection = MagicMock()
    collection.find_one = AsyncMock(return_value=find_one_return)
    collection.update_one = AsyncMock(side_effect=update_one_side_effect)
    return MongoTaskRepository(collection)


async def test_find_by_id_returns_task_when_found():
    doc = {"id": "t1", "title": "Root", "parent_task_id": None}
    repo = _make_repo(find_one_return=doc)

    result = await repo.find_by_id("t1")

    assert result is not None
    assert result.id == "t1"
    assert result.title == "Root"


async def test_find_by_id_returns_none_when_not_found():
    repo = _make_repo(find_one_return=None)

    result = await repo.find_by_id("missing")

    assert result is None


async def test_find_by_id_returns_none_for_corrupt_document():
    # title is required — omitting it produces a ValidationError
    doc = {"id": "t1", "parent_task_id": None}
    repo = _make_repo(find_one_return=doc)

    result = await repo.find_by_id("t1")

    assert result is None


async def test_find_by_id_ignores_unknown_fields():
    # Extra field that doesn't exist on Task (simulates schema evolution)
    doc = {"id": "t1", "title": "Root", "parent_task_id": None, "legacy_field": "old"}
    repo = _make_repo(find_one_return=doc)

    result = await repo.find_by_id("t1")

    assert result is not None
    assert result.id == "t1"


async def test_upsert_calls_update_one_with_upsert_true():
    repo = _make_repo()
    task = Task(id="t1", title="Root", parent_task_id=None)

    await repo.upsert(task)

    repo._collection.update_one.assert_called_once_with(
        {"id": "t1"},
        {"$set": task.model_dump(mode="json")},
        upsert=True,
    )


def _make_repo_with_aggregate(aggregate_docs):
    collection = MagicMock()
    collection.find_one = AsyncMock()
    collection.update_one = AsyncMock()
    cursor = AsyncMock()
    cursor.to_list = AsyncMock(return_value=aggregate_docs)
    collection.aggregate = MagicMock(return_value=cursor)
    return MongoTaskRepository(collection)


async def test_find_ancestor_chain_returns_none_when_task_not_found():
    repo = _make_repo_with_aggregate([])

    result = await repo.find_ancestor_chain("missing", 100)

    assert result is None


async def test_find_ancestor_chain_returns_empty_complete_for_root_task():
    # $graphLookup returns no ancestors for a root task (parent_task_id is None)
    repo = _make_repo_with_aggregate([{"parent_task_id": None, "_ancestors": []}])

    ancestors, complete, cycle, *_ = await repo.find_ancestor_chain("t1", 100)

    assert ancestors == []
    assert complete is True
    assert cycle is False


async def test_find_ancestor_chain_returns_empty_incomplete_when_parent_missing():
    # parent_task_id is set but $graphLookup found nothing (parent not ingested yet)
    repo = _make_repo_with_aggregate([{"parent_task_id": "t0-missing", "_ancestors": []}])

    ancestors, complete, cycle, *_ = await repo.find_ancestor_chain("t1", 100)

    assert ancestors == []
    assert complete is False
    assert cycle is False


async def test_find_ancestor_chain_returns_ordered_ancestors():
    repo = _make_repo_with_aggregate([{
        "parent_task_id": "t2",
        "_ancestors": [
            {"id": "t1", "title": "Root", "parent_task_id": None, "_depth": 1},
            {"id": "t2", "title": "Middle", "parent_task_id": "t1", "_depth": 0},
        ],
    }])

    ancestors, complete, cycle, *_ = await repo.find_ancestor_chain("t3", 100)

    assert [a.id for a in ancestors] == ["t2", "t1"]
    assert complete is True
    assert cycle is False


async def test_find_ancestor_chain_detects_cycle():
    # t2's parent is t1, t1's parent is t2 — cycle. $graphLookup deduplicates,
    # so last ancestor's parent_task_id points back into the seen set.
    repo = _make_repo_with_aggregate([{
        "parent_task_id": "t2",
        "_ancestors": [
            {"id": "t2", "title": "B", "parent_task_id": "t1", "_depth": 0},
            {"id": "t1", "title": "A", "parent_task_id": "t2", "_depth": 1},
        ],
    }])

    ancestors, complete, cycle, *_ = await repo.find_ancestor_chain("t3", 100)

    assert cycle is True
    assert complete is False


async def test_find_ancestor_chain_returns_partial_result_for_corrupt_ancestor():
    """A corrupt ancestor document (fails Task validation) returns the partial chain seen so far."""
    repo = _make_repo_with_aggregate([{
        "parent_task_id": "t1",
        "_ancestors": [
            # t2 is valid — depth 0, closest ancestor
            {"id": "t2", "title": "Middle", "parent_task_id": "t1", "_depth": 0},
            # t1 is corrupt: missing required 'title'
            {"id": "t1", "parent_task_id": None, "_depth": 1},
        ],
    }])

    result = await repo.find_ancestor_chain("t3", 100)

    assert result is not None
    ancestors, complete, cycle, *_ = result
    # Only t2 (before the corrupt t1) should be returned
    assert len(ancestors) == 1
    assert ancestors[0].id == "t2"
    assert complete is False
    assert cycle is False


async def test_find_ancestor_chain_reports_depth_limit():
    # Exactly max_depth ancestors returned, last one still has a parent.
    max_depth = 3
    ancestor_docs = [
        {"id": f"t{i}", "title": f"Task {i}", "parent_task_id": f"t{i+1}", "_depth": i - 1}
        for i in range(1, max_depth + 1)
    ]
    # Last ancestor's parent_task_id is not in seen set → depth limit
    repo = _make_repo_with_aggregate([{
        "parent_task_id": "t1",
        "_ancestors": ancestor_docs,
    }])

    result = await repo.find_ancestor_chain("t0", max_depth)

    assert len(result.tasks) == max_depth
    assert result.complete is False
    assert result.cycle is False
    assert result.depth_limited is True