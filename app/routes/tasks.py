import logging

from fastapi import APIRouter, Depends, HTTPException, Path, Response

from app.dependencies import get_task_chain_service, get_task_repository
from app.models import Task
from app.repositories.protocols import TaskRepository
from app.services.task_chain_service import TaskChainService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tasks", tags=["tasks"])

_TASK_ID = Path(..., min_length=1, max_length=255, pattern=r"^[a-zA-Z0-9_\-]+$")


@router.get("/{task_id}", response_model=Task)
async def get_task(
    task_id: str = _TASK_ID,
    repo: TaskRepository = Depends(get_task_repository),
):
    logger.info("GET /tasks/%s", task_id)
    task = await repo.find_by_id(task_id)
    if not task:
        logger.warning("GET /tasks/%s — not found", task_id)
        raise HTTPException(status_code=404, detail="Task not found")
    logger.info("GET /tasks/%s — returning task %r", task_id, task.title)
    return task


@router.get("/{task_id}/chain", response_model=list[Task])
async def get_dependency_chain(
    response: Response,
    task_id: str = _TASK_ID,
    service: TaskChainService = Depends(get_task_chain_service),
):
    logger.info("GET /tasks/%s/chain", task_id)
    result = await service.get_chain(task_id)
    if result is None:
        logger.warning("GET /tasks/%s/chain — task not found", task_id)
        raise HTTPException(status_code=404, detail="Task not found")
    # Signals whether the full ancestor chain was resolved. False means a parent
    # task has not yet been ingested — the chain may grow once it arrives.
    response.headers["X-Chain-Complete"] = str(result.complete).lower()
    logger.info(
        "GET /tasks/%s/chain — returning %d item(s), complete=%s",
        task_id,
        len(result.tasks),
        result.complete,
    )
    return result.tasks
