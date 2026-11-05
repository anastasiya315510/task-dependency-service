import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from typing import Any, cast

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError
from slowapi.errors import RateLimitExceeded
from slowapi import _rate_limit_exceeded_handler

from app.config import settings
from app.db import close_clients, get_tasks_collection
from app.dependencies import get_cache_service, get_task_repository, limiter
from app.logging_config import configure_logging, request_id_var
from app.routes.health import router as health_router
from app.routes.tasks import router as tasks_router
from app.services.kafka_consumer import KafkaConsumerRunner

# Configure logging at import time so that all log statements — including those
# emitted during module loading and FastAPI startup — use the correct format and
# level. Deferring this to the lifespan would leave early log lines unformatted.
configure_logging(settings.log_level, settings.log_format)
logger = logging.getLogger(__name__)

if settings.api_key is None:
    logger.warning("API_KEY is unset — all endpoints are publicly accessible")

if settings.rate_limit_storage_uri == "memory://":
    logger.warning(
        "RATE_LIMIT_STORAGE_URI=memory:// — rate-limit counters are per-process. "
        "In a multi-replica deployment each replica enforces limits independently, "
        "allowing up to N× the documented rate. "
        "Set RATE_LIMIT_STORAGE_URI=redis://<host>:6379 for cluster-wide enforcement."
    )

_SHUTDOWN_TIMEOUT = float(settings.shutdown_timeout_seconds)
_REQUEST_ID_MAX_LEN = 64


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Set state before any fallible I/O so health checks always find the runner.
    runner = KafkaConsumerRunner()
    _app.state.kafka_runner = runner
    try:
        col = get_tasks_collection()
        await asyncio.wait_for(
            asyncio.gather(
                col.create_index("id", unique=True),
                col.create_index("parent_task_id"),
            ),
            timeout=10.0,
        )
    except (asyncio.TimeoutError, PyMongoError) as exc:
        logger.error("MongoDB index creation failed — continuing startup: %s", exc)
    else:
        logger.info("MongoDB indexes on 'id' and 'parent_task_id' ensured")
    task = asyncio.create_task(
        runner.run(get_task_repository, get_cache_service)
    )
    await asyncio.sleep(0)  # yield to event loop so the consumer task starts
    yield
    # Signal graceful shutdown before cancelling so is_healthy() returns True
    # during the drain window — prevents false-positive 503s on load balancers.
    runner.shutdown()
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
app.state.limiter = limiter
# cast: slowapi stubs _rate_limit_exceeded_handler with a non-Optional Request,
# but FastAPI's add_exception_handler signature is broader — the cast is safe.
app.add_exception_handler(RateLimitExceeded, cast(Any, _rate_limit_exceeded_handler))
app.include_router(health_router)
app.include_router(tasks_router)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    raw_rid = request.headers.get("X-Request-ID", "")
    # Strip non-printable and non-ASCII bytes first (prevents header injection via CRLF),
    # then truncate so a long ID with many non-printable chars isn't silently dropped.
    cleaned = "".join(c for c in raw_rid if 0x20 <= ord(c) < 0x7F).strip()
    if len(cleaned) > _REQUEST_ID_MAX_LEN:
        logger.warning(
            "X-Request-ID truncated from %d to %d chars — trace correlation may break",
            len(cleaned),
            _REQUEST_ID_MAX_LEN,
        )
    rid = cleaned[:_REQUEST_ID_MAX_LEN] or uuid.uuid4().hex[:12]
    request_id_var.set(rid)
    response = await call_next(request)
    response.headers["X-Request-ID"] = rid
    return response


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_request: Request, exc: PyMongoError):
    logger.error("MongoDB unavailable on %s %s: %s", _request.method, _request.url.path, exc)
    return JSONResponse(status_code=503, content={"detail": "Service temporarily unavailable"})
