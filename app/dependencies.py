from app.db import get_redis, get_tasks_collection
from app.repositories.mongo_task_repository import MongoTaskRepository
from app.repositories.protocols import TaskChainCache, TaskRepository
from app.services.cache_service import TaskChainCacheService
from app.services.task_chain_service import TaskChainService


def get_task_repository() -> TaskRepository:
    return MongoTaskRepository(get_tasks_collection())


def get_cache_service() -> TaskChainCache:
    return TaskChainCacheService(get_redis())


def get_task_chain_service() -> TaskChainService:
    return TaskChainService(get_task_repository(), get_cache_service())
