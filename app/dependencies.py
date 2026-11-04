import hmac
import ipaddress
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
        candidate = forwarded_for.split(",")[-1].strip()
        try:
            ipaddress.ip_address(candidate)
            return candidate
        except ValueError:
            # Malformed or empty entry — fall through to socket IP.
            pass
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
    if not hmac.compare_digest(key or "", configured.get_secret_value()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )


limiter = Limiter(
    key_func=_get_client_ip,
    storage_uri=settings.rate_limit_storage_uri,
    # Enable in-memory fallback when using Redis storage so a Redis restart
    # or connection blip doesn't block requests entirely.
    # With the default memory:// URI (dev/CI), this flag has no effect.
    in_memory_fallback_enabled=settings.rate_limit_storage_uri.startswith("redis"),
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
