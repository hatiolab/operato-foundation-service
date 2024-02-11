from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum


"""
Schedule Interface Definitions

"""


class Locking(BaseModel):
    name: str = Field(default="", title="Locking event name")
    wait_until: Optional[str] = Field(default="10", title="wait until")
    status: Optional[str] = Field(
        default="READY", title="status [READY, LOCKED, RELEASED, FAILED]"
    )
    id: Optional[str] = Field(default="", title="Locking id")


class LockingRequestResult(BaseModel):
    name: str = Field(default="", title="Locking event name")
    id: str = Field(default="", title="Locking id")


class LockingResultCount(BaseModel):
    count: int = Field(default=0, title="Locking count")
