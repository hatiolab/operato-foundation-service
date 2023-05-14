from pydantic import BaseModel, Field
from typing import List, Optional


"""
PendingQueue Type Definitions

"""

class PendingEvent(BaseModel):
    due: int = Field(default=0, title= "timestamp to be fired")
    priority: int = Field(default=0, title="queue priority")
    tag: str = Field(default="", title= "the registered timestamp")
    stuff: dict = Field(default={}, title="user data")
    id: Optional[str] = Field(default="", title="pending event id")


class PendingIdResult(BaseModel):
    id: str = Field(default="", title="pending event id")


class PendingStuff(BaseModel):
    stuff: dict