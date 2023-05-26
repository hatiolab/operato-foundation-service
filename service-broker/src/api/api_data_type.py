from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum

"""
ServiceData Type Definitions

"""

class ServiceCallbackPolicy(str, Enum):
    IGNORE = "ignore"
    RETRY = "retry"

class ServiceCallback(BaseModel):
    connection: dict = Field(
        default={}, title="Connection information(host, headers, topic, ...)"
    )
    data: dict = Field(default={}, title="User data")
    failed_policy: Optional[ServiceCallbackPolicy] = Field(
        default=ServiceCallbackPolicy.IGNORE,
        title="Task failure policy [ignore | retry]",
    )
    retry_wait: Optional[int] = Field(
        default=60,
        title="Retry wait time when a non-recurring task(date, delay, now) fails to run",
    )
    retry_count: Optional[int] = Field(
        default=0,
        title="Retry count when a non-recurring task(date, delay, now) fails to run",
    )
    max_retry_count: Optional[int] = Field(
        default=-1,
        title="Retry count limit",
    )

class ServiceInput(BaseModel):
    application: str = Field(default="", title="Client application name")
    group: str = Field(default="", title="Client group name")
    operation: str = Field(default="", title="Operation name")
    service_callback: ServiceCallback

class ServiceOutput(BaseModel):
    application: str = Field(default="", title="Client application name")
    group: str = Field(default="", title="Client group name")
    operation: str = Field(default="", title="Operation name")
    service_callback: ServiceCallback
    id: str = Field(default="", title= "service id")
    endpoint: str = Field(default="", title= "endpoint")

class ServiceEndpointResult(BaseModel):
    result: bool = Field(default=False, title= "service endpoint result")
