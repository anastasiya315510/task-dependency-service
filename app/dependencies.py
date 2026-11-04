import hmac
from functools import lru_cache

from fastapi import HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.config import settings
from app.db import get_redis, get_tasks_collection
from app.repositories.mongo_task_repository import MongoTaskRepository
from app.repositories.protocols import TaskChainCache, TaskChainServiceProtocol, TaskRepository
from app.services.cache_service import TaskChainCacheService
from app.services.task_chain_service import TaskChainService


def _get_client_ip(request: Request) -> str:
    """Return the real client IP.

    X-Forwarded-For is only honoured when the socket peer is in the
    configured trusted_proxy_ips set, preventing IP spoofing on direct
    deployments.
    """
    socket_ip = get_remote_address(request)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for and socket_ip in settings.trusted_proxy_ips:
        # Use the rightmost entry: the proxy appends its own record so the
        # rightmost value is the last hop we can verify.  The leftmost entry
        # is client-controlled and must not be trusted.
        return forwarded_for.split(",")[-1].strip()
    return socket_ip


_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(key: str | None = Security(_api_key_header)) -> None:
    """Validate X-API-Key when API_KEY is configured.

    When settings.api_key is unset the check is skipped entirely, so
    deployments that rely on network-level controls (VPC, mTLS) don't need
    to set the env var.
    """
    configured = settings.api_key
    if configured is None:
        return
    if key is None or not hmac.compare_digest(key, configured.get_secret_value()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )


limiter = Limiter(
    key_func=_get_client_ip,
    storage_uri=settings.redis_url.get_secret_value(),
    # Fall back to in-memory when Redis is unreachable (dev, CI, Redis restart).
    # In production all replicas share the Redis counter, enforcing the limit
    # cluster-wide. Without this, each process tracks its own counter and the
    # effective limit scales with replica count.
    in_memory_fallback_enabled=True,
)


@lru_cache(maxsize=1)
def get_task_repository() -> TaskRepository:
    return MongoTaskRepository(get_tasks_collection())


@lru_cache(maxsize=1)
def get_cache_service() -> TaskChainCache:
    return TaskChainCacheService(get_redis())


@lru_cache(maxsize=1)
def get_task_chain_service() -> TaskChainServiceProtocol:
    return TaskChainService(get_task_repository(), get_cache_service())
