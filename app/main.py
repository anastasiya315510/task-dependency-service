import asyncio
import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError

from app.config import settings
from app.db import close_clients, get_tasks_collection
from app.dependencies import get_cache_service, get_task_repository
from app.logging_config import configure_logging, request_id_var
from app.routes.health import router as health_router
from app.routes.tasks import router as tasks_router
from app.services.kafka_consumer import consume_forever

configure_logging(settings.log_level, settings.log_format)
logger = logging.getLogger(__name__)

_SHUTDOWN_TIMEOUT = 10.0  # seconds to wait for in-flight Kafka message to finish


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await get_tasks_collection().create_index("id", unique=True, background=True)
    logger.info("MongoDB index on 'id' ensured")
    task = asyncio.create_task(
        consume_forever(get_task_repository(), get_cache_service())
    )
    yield
    task.cancel()
    try:
        await asyncio.wait_for(
            asyncio.gather(task, return_exceptions=True),
            timeout=_SHUTDOWN_TIMEOUT,
        )
    except asyncio.TimeoutError:
        logger.error(
            "Kafka consumer did not stop within %ss — forcing shutdown",
            _SHUTDOWN_TIMEOUT,
        )
    await close_clients()
    logger.info("Shutdown complete")


app = FastAPI(title="Task Dependency Service", lifespan=lifespan)
app.include_router(health_router)
app.include_router(tasks_router)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
    request_id_var.set(rid)
    response = await call_next(request)
    response.headers["X-Request-ID"] = rid
    return response


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_request, exc: PyMongoError):
    logger.error("MongoDB unavailable: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "Service temporarily unavailable"})
