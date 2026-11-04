import logging

from fastapi import APIRouter, Depends, HTTPException, Path, Request, Response

from app.dependencies import get_task_chain_service, get_task_repository, limiter, require_api_key
from app.models import Task
from app.repositories.protocols import TaskChainServiceProtocol, TaskRepository

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tasks", tags=["tasks"], dependencies=[Depends(require_api_key)])

_TASK_ID = Path(..., min_length=1, max_length=255, pattern=r"^[a-zA-Z0-9_\-]+$")


@router.get("/{task_id}", response_model=Task)
@limiter.limit("120/minute")
async def get_task(
    request: Request,
    task_id: str = _TASK_ID,
    repo: TaskRepository = Depends(get_task_repository),
):
    logger.info("GET /tasks/%s", task_id)
    task = await repo.find_by_id(task_id)
    if not task:
        logger.debug("GET /tasks/%s — not found", task_id)
        raise HTTPException(status_code=404, detail="Task not found")
    logger.info("GET /tasks/%s — found", task_id)
    return task


@router.get("/{task_id}/chain", response_model=list[Task])
@limiter.limit("60/minute")
async def get_dependency_chain(
    request: Request,
    response: Response,
    task_id: str = _TASK_ID,
    service: TaskChainServiceProtocol = Depends(get_task_chain_service),
):
    logger.info("GET /tasks/%s/chain", task_id)
    result = await service.get_chain(task_id)
    if result is None:
        logger.debug("GET /tasks/%s/chain — task not found", task_id)
        raise HTTPException(status_code=404, detail="Task not found")
    # X-Chain-Complete: false when a parent is missing, depth limit hit, or cycle detected.
    response.headers["X-Chain-Complete"] = str(result.complete).lower()
    # X-Chain-Cycle: true signals a circular dependency was found in the data.
    response.headers["X-Chain-Cycle"] = str(result.cycle).lower()
    logger.info(
        "GET /tasks/%s/chain — returning %d item(s), complete=%s, cycle=%s",
        task_id,
        len(result.tasks),
        result.complete,
        result.cycle,
    )
    return result.tasks