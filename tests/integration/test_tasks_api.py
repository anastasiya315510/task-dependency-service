from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from pydantic import SecretStr
from pymongo.errors import PyMongoError

from app.config import settings
from app.models import Task
from app.services.kafka_consumer import KafkaConsumerRunner
from tests.conftest import FakeTaskRepository

pytestmark = pytest.mark.integration


async def test_get_task_returns_task(api_client: AsyncClient, fake_repo: FakeTaskRepository):
    fake_repo._tasks["t1"] = Task(id="t1", title="Root", parent_task_id=None)

    response = await api_client.get("/tasks/t1")

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "t1"
    assert data["title"] == "Root"
    assert data["parent_task_id"] is None


async def test_get_task_returns_404_when_not_found(api_client: AsyncClient):
    response = await api_client.get("/tasks/missing")

    assert response.status_code == 404
    assert response.json()["detail"] == "Task not found"


async def test_get_chain_returns_empty_for_root_task(api_client: AsyncClient, fake_repo: FakeTaskRepository):
    fake_repo._tasks["t1"] = Task(id="t1", title="Root", parent_task_id=None)

    response = await api_client.get("/tasks/t1/chain")

    assert response.status_code == 200
    assert response.json() == []


async def test_get_chain_returns_ordered_dependency_chain(api_client: AsyncClient, fake_repo: FakeTaskRepository):
    fake_repo._tasks["t1"] = Task(id="t1", title="Root", parent_task_id=None)
    fake_repo._tasks["t2"] = Task(id="t2", title="Middle", parent_task_id="t1")
    fake_repo._tasks["t3"] = Task(id="t3", title="Leaf", parent_task_id="t2")

    response = await api_client.get("/tasks/t3/chain")

    assert response.status_code == 200
    chain = response.json()
    assert len(chain) == 2
    assert chain[0]["id"] == "t2"
    assert chain[1]["id"] == "t1"


async def test_get_chain_returns_404_when_task_not_found(api_client: AsyncClient):
    response = await api_client.get("/tasks/missing/chain")

    assert response.status_code == 404
    assert response.json()["detail"] == "Task not found"


async def test_get_chain_handles_out_of_order_arrival(api_client: AsyncClient, fake_repo: FakeTaskRepository):
    # Child arrives before parent — parent not yet ingested
    fake_repo._tasks["t2"] = Task(id="t2", title="Child", parent_task_id="t1-not-yet")

    response = await api_client.get("/tasks/t2/chain")

    assert response.status_code == 200
    assert response.json() == []  # partial chain, no error
    assert response.headers["X-Chain-Complete"] == "false"
    assert response.headers["X-Chain-Cycle"] == "false"


async def test_get_task_returns_503_when_mongo_unavailable(
    api_client: AsyncClient, fake_repo: FakeTaskRepository
):
    async def broken_find(task_id: str):
        raise PyMongoError("connection refused")

    fake_repo.find_by_id = broken_find

    response = await api_client.get("/tasks/t1")

    assert response.status_code == 503


async def test_get_task_rejects_invalid_task_id(api_client: AsyncClient):
    # @ is outside the allowed pattern ^[a-zA-Z0-9_-]+$
    response = await api_client.get("/tasks/task@invalid")

    assert response.status_code == 422


async def test_request_id_echoed_in_response(api_client: AsyncClient, fake_repo: FakeTaskRepository):
    fake_repo._tasks["t1"] = Task(id="t1", title="Root", parent_task_id=None)

    response = await api_client.get("/tasks/t1", headers={"X-Request-ID": "trace-abc123"})

    assert response.headers["X-Request-ID"] == "trace-abc123"


async def test_request_id_generated_when_absent(api_client: AsyncClient, fake_repo: FakeTaskRepository):
    fake_repo._tasks["t1"] = Task(id="t1", title="Root", parent_task_id=None)

    response = await api_client.get("/tasks/t1")

    assert "X-Request-ID" in response.headers
    assert len(response.headers["X-Request-ID"]) == 12  # uuid4().hex[:12]


async def test_get_chain_complete_header_true_for_root(
    api_client: AsyncClient, fake_repo: FakeTaskRepository
):
    fake_repo._tasks["t1"] = Task(id="t1", title="Root", parent_task_id=None)

    response = await api_client.get("/tasks/t1/chain")

    assert response.headers["X-Chain-Complete"] == "true"


async def test_get_chain_complete_header_false_when_parent_missing(
    api_client: AsyncClient, fake_repo: FakeTaskRepository
):
    fake_repo._tasks["t2"] = Task(id="t2", title="Child", parent_task_id="t1-missing")

    response = await api_client.get("/tasks/t2/chain")

    assert response.headers["X-Chain-Complete"] == "false"


async def test_get_chain_cycle_headers(
    api_client: AsyncClient, fake_repo: FakeTaskRepository
):
    fake_repo._tasks["t1"] = Task(id="t1", title="A", parent_task_id="t2")
    fake_repo._tasks["t2"] = Task(id="t2", title="B", parent_task_id="t1")

    response = await api_client.get("/tasks/t1/chain")

    assert response.status_code == 200
    assert response.headers["X-Chain-Cycle"] == "true"
    assert response.headers["X-Chain-Complete"] == "false"


async def test_get_chain_returns_503_when_mongo_raises(
    api_client: AsyncClient, fake_repo: FakeTaskRepository
):
    fake_repo._tasks["t1"] = Task(id="t1", title="Root", parent_task_id=None)

    async def broken_find_ancestor_chain(task_id: str, max_depth: int):
        raise PyMongoError("connection refused")

    fake_repo.find_ancestor_chain = broken_find_ancestor_chain

    response = await api_client.get("/tasks/t1/chain")

    assert response.status_code == 503


async def test_health_returns_200_when_all_deps_healthy(api_client: AsyncClient):
    with (
        patch("app.routes.health.get_tasks_collection") as mock_col,
        patch("app.routes.health.get_redis") as mock_redis,
        patch.object(KafkaConsumerRunner, "is_healthy", return_value=True),
    ):
        mock_col.return_value.database.command = AsyncMock(return_value={"ok": 1})
        mock_redis.return_value.ping = AsyncMock(return_value=True)

        response = await api_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_health_returns_503_when_mongo_unavailable(api_client: AsyncClient):
    with (
        patch("app.routes.health.get_tasks_collection") as mock_col,
        patch("app.routes.health.get_redis") as mock_redis,
        patch.object(KafkaConsumerRunner, "is_healthy", return_value=True),
    ):
        mock_col.return_value.database.command = AsyncMock(
            side_effect=PyMongoError("unreachable")
        )
        mock_redis.return_value.ping = AsyncMock(return_value=True)

        response = await api_client.get("/health")

    assert response.status_code == 503
    assert "mongodb" in response.json()["detail"]


async def test_health_returns_503_when_kafka_consumer_unhealthy(api_client: AsyncClient):
    with (
        patch("app.routes.health.get_tasks_collection") as mock_col,
        patch("app.routes.health.get_redis") as mock_redis,
        patch.object(KafkaConsumerRunner, "is_healthy", return_value=False),
    ):
        mock_col.return_value.database.command = AsyncMock(return_value={"ok": 1})
        mock_redis.return_value.ping = AsyncMock(return_value=True)

        response = await api_client.get("/health")

    assert response.status_code == 503
    assert "kafka_consumer" in response.json()["detail"]


# --- API key authentication ---

async def test_api_key_absent_returns_401(api_client: AsyncClient):
    with patch.object(settings, "api_key", SecretStr("test-secret")):
        response = await api_client.get("/tasks/t1")
    assert response.status_code == 401


async def test_api_key_wrong_returns_401(api_client: AsyncClient):
    with patch.object(settings, "api_key", SecretStr("test-secret")):
        response = await api_client.get("/tasks/t1", headers={"X-API-Key": "wrong"})
    assert response.status_code == 401


async def test_api_key_correct_returns_200(api_client: AsyncClient, fake_repo: FakeTaskRepository):
    fake_repo._tasks["t1"] = Task(id="t1", title="Root", parent_task_id=None)
    with patch.object(settings, "api_key", SecretStr("test-secret")):
        response = await api_client.get("/tasks/t1", headers={"X-API-Key": "test-secret"})
    assert response.status_code == 200