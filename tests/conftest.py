import fakeredis.aioredis
import pytest
from httpx import ASGITransport, AsyncClient

from app.dependencies import get_cache_service, get_task_chain_service, get_task_repository
from app.main import app
from app.models import ChainResult, Task
from app.services.cache_service import TaskChainCacheService
from app.services.kafka_consumer import KafkaConsumerRunner
from app.services.task_chain_service import TaskChainService


class FakeTaskRepository:
    def __init__(self, tasks: dict[str, Task] | None = None):
        self._tasks: dict[str, Task] = tasks or {}

    async def find_by_id(self, task_id: str) -> Task | None:
        return self._tasks.get(task_id)

    async def upsert(self, task: Task) -> None:
        self._tasks[task.id] = task

    async def find_ancestor_chain(
        self, task_id: str, max_depth: int
    ) -> ChainResult | None:
        root = self._tasks.get(task_id)
        if root is None:
            return None
        ancestors: list[Task] = []
        visited: set[str] = {task_id}
        current_id = root.parent_task_id
        depth = 0
        while current_id:
            if current_id in visited:
                return ChainResult(ancestors, False, True)
            if depth >= max_depth:
                return ChainResult(ancestors, False, False, True)
            visited.add(current_id)
            parent = self._tasks.get(current_id)
            if parent is None:
                return ChainResult(ancestors, False, False)
            ancestors.append(parent)
            current_id = parent.parent_task_id
            depth += 1
        return ChainResult(ancestors, True, False)


@pytest.fixture
def fake_repo() -> FakeTaskRepository:
    return FakeTaskRepository()


@pytest.fixture
def fake_cache() -> TaskChainCacheService:
    """Real TaskChainCacheService backed by an in-process fakeredis instance.

    Using the real implementation means invalidation logic is exercised in
    unit tests, not duplicated in a fake that could drift from production.
    decode_responses=True mirrors the real client in db.py.
    """
    return TaskChainCacheService(fakeredis.aioredis.FakeRedis(decode_responses=True))


@pytest.fixture
async def api_client(fake_repo: FakeTaskRepository, fake_cache: TaskChainCacheService):
    service = TaskChainService(fake_repo, fake_cache)
    app.dependency_overrides[get_task_repository] = lambda: fake_repo
    app.dependency_overrides[get_cache_service] = lambda: fake_cache
    app.dependency_overrides[get_task_chain_service] = lambda: service
    original_runner = getattr(app.state, "kafka_runner", None)
    app.state.kafka_runner = KafkaConsumerRunner()
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client
    finally:
        app.dependency_overrides.clear()
        if original_runner is not None:
            app.state.kafka_runner = original_runner
        elif hasattr(app.state, "kafka_runner"):
            del app.state.kafka_runner