from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum


"""
Schedule Interface Definitions

"""


class LockingStatus(str, Enum):
    READY = "READY"
    LOCKED = "LOCKED"
    RELEASED = "RELEASED"
    FAILED = "FAILED"


class Locking(BaseModel):
    id: Optional[str] = Field(default="", title="Locking id")
    status: Optional[LockingStatus] = Field(
        default="READY", title="status [READY, LOCKED, RELEASED, FAILED]"
    )
    wait_until: Optional[str] = Field(default="10", title="wait until")


class LockingRegisterInput(BaseModel):
    wait_until: Optional[str] = Field(default="10", title="wait until")


class LockingId(BaseModel):
    id: str = Field(default="", title="Locking id")


class LockingRequestResult(BaseModel):
    id: str = Field(default="", title="Locking id")
    status: Optional[LockingStatus] = Field(
        default="READY", title="status [READY, LOCKED, RELEASED, FAILED]"
    )


class LockingResultCount(BaseModel):
    count: int = Field(default=0, title="Locking count")
