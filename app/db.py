import redis.asyncio as redis
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from app.config import settings

_mongo_client: AsyncIOMotorClient = AsyncIOMotorClient(
    settings.mongodb_url,
    serverSelectionTimeoutMS=5_000,
    connectTimeoutMS=10_000,
    retryWrites=True,
)
_redis_client: redis.Redis = redis.from_url(
    settings.redis_url,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_keepalive=True,
    health_check_interval=30,
)


def get_tasks_collection() -> AsyncIOMotorCollection:
    return _mongo_client[settings.mongodb_db_name]["tasks"]


def get_redis() -> redis.Redis:
    return _redis_client


async def close_clients() -> None:
    _mongo_client.close()
    await _redis_client.aclose()
