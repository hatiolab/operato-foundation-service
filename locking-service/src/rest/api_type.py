from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
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
    payload: Optional[Dict[str, Any]] = Field(default=None, title="payload")


class LockingInput(BaseModel):
    id: str = Field(default="", title="Locking id")
    payload: Optional[Dict[str, Any]] = Field(default=None, title="payload")


class LockingPayload(BaseModel):
    payload: Optional[Dict[str, Any]] = Field(default=None, title="payload")


class LockingRequestResult(BaseModel):
    id: str = Field(default="", title="Locking id")
    status: Optional[LockingStatus] = Field(
        default="READY", title="status [READY, LOCKED, RELEASED, FAILED]"
    )
    payload: Optional[Dict[str, Any]] = Field(default=None, title="payload")


class LockingResultCount(BaseModel):
    count: int = Field(default=0, title="Locking count")
