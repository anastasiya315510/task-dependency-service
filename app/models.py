import uuid
from typing import ClassVar, NamedTuple

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _new_task_id() -> str:
    return str(uuid.uuid4())


class Task(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Pattern matches the HTTP path constraint so Kafka-ingested tasks are
    # always reachable via the API — rejects IDs at ingestion time rather
    # than writing unreadable documents.
    _ID_PATTERN: ClassVar[str] = r"^[a-zA-Z0-9_\-]+$"
    id: str = Field(default_factory=_new_task_id, min_length=1, max_length=255, pattern=_ID_PATTERN)
    title: str = Field(..., min_length=1, max_length=500)
    # min_length=1 prevents empty-string parent_task_id which would silently
    # never match any task and produce an incomplete chain without warning.
    parent_task_id: str | None = Field(None, min_length=1, max_length=255, pattern=_ID_PATTERN)

    @model_validator(mode="after")
    def parent_not_self(self) -> "Task":
        if self.parent_task_id is not None and self.parent_task_id == self.id:
            raise ValueError("A task cannot be its own parent")
        return self


class ChainResult(NamedTuple):
    tasks: list[Task]
    complete: bool
    cycle: bool = False
    # True when the chain was cut short by the configured depth limit rather than
    # by a genuinely missing parent.  Callers should not retry when this is True —
    # the chain is structurally longer than the service is configured to traverse.
    depth_limited: bool = False
