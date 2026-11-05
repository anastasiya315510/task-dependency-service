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
    # pylint: disable=unused-argument  # request is required by slowapi
    logger.info("GET /tasks/%s", task_id)
    task = await repo.find_by_id(task_id)
    if not task:
        logger.debug("GET /tasks/%s — not found", task_id)
        raise HTTPException(status_code=404, detail="Task not found")
    logger.info("GET /tasks/%s — found", task_id)
    return task


@router.get(
    "/{task_id}/chain",
    response_model=list[Task],
    description=(
        "Return the ordered ancestor chain for the given task. "
        "The task itself is **not** included — only its ancestors, nearest first. "
        "Response headers signal chain state:\n"
        "- `X-Chain-Complete: true` — full chain resolved to a root task.\n"
        "- `X-Chain-Complete: false` — chain is incomplete (parent not yet ingested, "
        "depth limit reached, or cycle detected).\n"
        "- `X-Chain-Depth-Limited: true` — chain was cut at the configured depth cap; "
        "retrying will not yield more ancestors.\n"
        "- `X-Chain-Cycle: true` — a circular dependency was detected in the data.\n\n"
        "Note: `X-Chain-Cycle: false` only guarantees no cycle *within the traversed depth*."
    ),
)
@limiter.limit("60/minute")
async def get_dependency_chain(
    request: Request,
    response: Response,
    task_id: str = _TASK_ID,
    service: TaskChainServiceProtocol = Depends(get_task_chain_service),
):
    # pylint: disable=unused-argument  # request is required by slowapi
    logger.info("GET /tasks/%s/chain", task_id)
    result = await service.get_chain(task_id)
    if result is None:
        logger.debug("GET /tasks/%s/chain — task not found", task_id)
        raise HTTPException(status_code=404, detail="Task not found")
    response.headers["X-Chain-Complete"] = "true" if result.complete else "false"
    response.headers["X-Chain-Cycle"] = "true" if result.cycle else "false"
    response.headers["X-Chain-Depth-Limited"] = "true" if result.depth_limited else "false"
    logger.info(
        "GET /tasks/%s/chain — returning %d item(s), complete=%s, cycle=%s, depth_limited=%s",
        task_id,
        len(result.tasks),
        result.complete,
        result.cycle,
        result.depth_limited,
    )
    return result.tasks
