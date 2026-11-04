import asyncio
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.dependencies import get_task_chain_service, get_task_repository
from app.main import app
from app.models import Task
from app.services.task_chain_service import TaskChainService


class FakeTaskRepository:
    def __init__(self, tasks: dict[str, Task] | None = None):
        self._tasks: dict[str, Task] = tasks or {}

    async def find_by_id(self, task_id: str) -> Task | None:
        return self._tasks.get(task_id)

    async def upsert(self, doc: dict) -> None:
        # Mimic MongoTaskRepository: accepts any dict, ignores unknown fields,
        # so tests are not broken by extra fields that MongoDB would store fine.
        known = {k: v for k, v in doc.items() if k in Task.model_fields}
        self._tasks[doc["id"]] = Task(**known)


class FakeTaskChainCacheService:
    def __init__(self):
        self._store: dict[str, list[Task]] = {}

    async def get(self, task_id: str) -> list[Task] | None:
        return self._store.get(task_id)

    async def set(self, task_id: str, chain: list[Task]) -> None:
        self._store[task_id] = chain

    async def invalidate(self, task_id: str) -> None:
        self._store.pop(task_id, None)


async def _kafka_stub() -> None:
    """Stand-in for the Kafka consumer so tests don't need a broker."""
    await asyncio.sleep(float("inf"))


@pytest.fixture
def fake_repo() -> FakeTaskRepository:
    return FakeTaskRepository()


@pytest.fixture
def fake_cache() -> FakeTaskChainCacheService:
    return FakeTaskChainCacheService()


@pytest.fixture
async def api_client(fake_repo: FakeTaskRepository, fake_cache: FakeTaskChainCacheService):
    service = TaskChainService(fake_repo, fake_cache)
    app.dependency_overrides[get_task_repository] = lambda: fake_repo
    app.dependency_overrides[get_task_chain_service] = lambda: service
    with patch("app.main.consume_forever", _kafka_stub):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client
    app.dependency_overrides.clear()