"""Unit tests for MongoTaskRepository using a fake Motor collection."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models import Task
from app.repositories.mongo_task_repository import MongoTaskRepository


def _make_repo(find_one_return=None, update_one_side_effect=None):
    collection = MagicMock()
    collection.find_one = AsyncMock(return_value=find_one_return)
    collection.update_one = AsyncMock(side_effect=update_one_side_effect)
    return MongoTaskRepository(collection)


@pytest.mark.unit
async def test_find_by_id_returns_task_when_found():
    doc = {"id": "t1", "title": "Root", "parent_task_id": None}
    repo = _make_repo(find_one_return=doc)

    result = await repo.find_by_id("t1")

    assert result is not None
    assert result.id == "t1"
    assert result.title == "Root"


@pytest.mark.unit
async def test_find_by_id_returns_none_when_not_found():
    repo = _make_repo(find_one_return=None)

    result = await repo.find_by_id("missing")

    assert result is None


@pytest.mark.unit
async def test_find_by_id_returns_none_for_corrupt_document():
    # title is required — omitting it produces a ValidationError
    doc = {"id": "t1", "parent_task_id": None}
    repo = _make_repo(find_one_return=doc)

    result = await repo.find_by_id("t1")

    assert result is None


@pytest.mark.unit
async def test_find_by_id_ignores_unknown_fields():
    # Extra field that doesn't exist on Task (simulates schema evolution)
    doc = {"id": "t1", "title": "Root", "parent_task_id": None, "legacy_field": "old"}
    repo = _make_repo(find_one_return=doc)

    result = await repo.find_by_id("t1")

    assert result is not None
    assert result.id == "t1"


@pytest.mark.unit
async def test_upsert_calls_update_one_with_upsert_true():
    repo = _make_repo()
    doc = {"id": "t1", "title": "Root", "parent_task_id": None}

    await repo.upsert(doc)

    repo._collection.update_one.assert_called_once_with(
        {"id": "t1"},
        {"$set": doc},
        upsert=True,
    )


@pytest.mark.unit
async def test_upsert_raises_on_missing_id():
    repo = _make_repo()

    with pytest.raises(ValueError, match="missing or invalid"):
        await repo.upsert({"title": "No ID"})