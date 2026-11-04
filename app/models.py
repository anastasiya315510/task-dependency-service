import uuid
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _new_task_id() -> str:
    return str(uuid.uuid4())


class Task(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=_new_task_id, min_length=1, max_length=255)
    title: str = Field(..., min_length=1, max_length=500)
    # min_length=1 prevents empty-string parent_task_id which would silently
    # never match any task and produce an incomplete chain without warning.
    parent_task_id: Optional[str] = Field(None, min_length=1, max_length=255)

    @model_validator(mode="after")
    def parent_not_self(self) -> "Task":
        if self.parent_task_id is not None and self.parent_task_id == self.id:
            raise ValueError("A task cannot be its own parent")
        return self
