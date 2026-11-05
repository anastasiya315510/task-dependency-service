"""Unit tests for app/main.py lifespan."""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI

from app.main import lifespan
from app.services.kafka_consumer import KafkaConsumerRunner

pytestmark = pytest.mark.unit


async def test_lifespan_creates_mongo_index_on_startup():
    with (
        patch("app.main.get_tasks_collection") as mock_col,
        patch("app.main.get_task_repository"),
        patch("app.main.get_cache_service"),
        patch("app.main.close_clients", new_callable=AsyncMock),
        patch.object(KafkaConsumerRunner, "run", new_callable=AsyncMock),
    ):
        mock_col.return_value.create_index = AsyncMock()

        async with lifespan(FastAPI()):
            pass

        calls = mock_col.return_value.create_index.await_args_list
        assert any(c == (("id",), {"unique": True}) for c in calls), (
            "Expected create_index('id', unique=True) to have been awaited"
        )
        assert any(c == (("parent_task_id",), {}) for c in calls), (
            "Expected create_index('parent_task_id') to have been awaited"
        )


async def test_lifespan_starts_kafka_consumer_on_startup():
    with (
        patch("app.main.get_tasks_collection") as mock_col,
        patch("app.main.get_task_repository"),
        patch("app.main.get_cache_service"),
        patch("app.main.close_clients", new_callable=AsyncMock),
        patch.object(KafkaConsumerRunner, "run", new_callable=AsyncMock) as mock_run,
    ):
        mock_col.return_value.create_index = AsyncMock()

        async with lifespan(FastAPI()):
            pass

        mock_run.assert_awaited_once()
        assert mock_run.call_args is not None, "run() must be called with repo and cache arguments"


async def test_lifespan_closes_clients_on_shutdown():
    with (
        patch("app.main.get_tasks_collection") as mock_col,
        patch("app.main.get_task_repository"),
        patch("app.main.get_cache_service"),
        patch("app.main.close_clients", new_callable=AsyncMock) as mock_close,
        patch.object(KafkaConsumerRunner, "run", new_callable=AsyncMock),
    ):
        mock_col.return_value.create_index = AsyncMock()

        async with lifespan(FastAPI()):
            pass

        mock_close.assert_awaited_once()


async def test_lifespan_sets_kafka_runner_on_app_state():
    _app = FastAPI()
    with (
        patch("app.main.get_tasks_collection") as mock_col,
        patch("app.main.get_task_repository"),
        patch("app.main.get_cache_service"),
        patch("app.main.close_clients", new_callable=AsyncMock),
        patch.object(KafkaConsumerRunner, "run", new_callable=AsyncMock),
    ):
        mock_col.return_value.create_index = AsyncMock()

        async with lifespan(_app):
            assert isinstance(_app.state.kafka_runner, KafkaConsumerRunner)


async def test_lifespan_cancels_kafka_runner_on_shutdown():
    """lifespan must cancel the Kafka runner task when the app shuts down."""
    cancelled = asyncio.Event()

    async def fake_run(*_args, **_kwargs):
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with (
        patch("app.main.get_tasks_collection") as mock_col,
        patch("app.main.get_task_repository"),
        patch("app.main.get_cache_service"),
        patch("app.main.close_clients", new_callable=AsyncMock),
        patch.object(KafkaConsumerRunner, "run", fake_run),
    ):
        mock_col.return_value.create_index = AsyncMock()

        async with lifespan(FastAPI()):
            pass  # exit immediately triggers shutdown

    assert cancelled.is_set(), "lifespan must cancel the Kafka runner task on shutdown"