import redis.asyncio as redis
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from app.config import settings

# Clients are created lazily on first access so that importing this module
# during tests does not immediately open connections or fail on missing env vars.
# Using a dict avoids module-level reassignment (global statements).
_clients: dict = {}


def _get_mongo_client() -> AsyncIOMotorClient:
    if "mongo" not in _clients:
        _clients["mongo"] = AsyncIOMotorClient(
            settings.mongodb_url.get_secret_value(),
            serverSelectionTimeoutMS=5_000,
            connectTimeoutMS=10_000,
            retryWrites=True,
        )
    return _clients["mongo"]


def _get_redis_client() -> redis.Redis:
    if "redis" not in _clients:
        _clients["redis"] = redis.from_url(
            settings.redis_url.get_secret_value(),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_keepalive=True,
            health_check_interval=30,
        )
    return _clients["redis"]


def get_tasks_collection() -> AsyncIOMotorCollection:
    return _get_mongo_client()[settings.mongodb_db_name]["tasks"]


def get_redis() -> redis.Redis:
    return _get_redis_client()


async def close_clients() -> None:
    mongo = _clients.get("mongo")
    if mongo is not None:
        mongo.close()
    redis_client = _clients.get("redis")
    if redis_client is not None:
        await redis_client.aclose()
