from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum


"""
Schedule Interface Definitions

"""


class ScheduleTaskFailurePolicy(str, Enum):
    IGNORE = "ignore"
    RETRY = "retry"


class ScheduleClient(BaseModel):
    application: Optional[str] = Field(default="", title="Client application name")
    group: Optional[str] = Field(default="", title="Client group name")
    type: Optional[str] = Field(default="", title="Resource type")
    key: Optional[str] = Field(default="", title="Data key managed by client")
    operation: Optional[str] = Field(default="", title="Operation name")


class ScheduleTask(BaseModel):
    type: str = Field(default="", title="Task type [rest | kafka | redis]")
    connection: dict = Field(
        default={}, title="Connection information(host, headers, topic, ...)"
    )
    data: dict = Field(default={}, title="User data")
    history_check: Optional[bool] = Field(
        default=False, title="Schedule status change history enable or disable"
    )
    failed_policy: Optional[ScheduleTaskFailurePolicy] = Field(
        default=ScheduleTaskFailurePolicy.IGNORE,
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


class Schedule(BaseModel):
    name: str = Field(default="", title="Schedule name")
    client: ScheduleClient
    type: str = Field(
        default="", title="Schdele type [cron, delay, delay-recur, date, now]"
    )
    schedule: str = Field(
        default="", title="Schedule information based on schedule type"
    )
    timezone: Optional[str] = Field(
        default="Asia/Seoul",
        title="Timezone, should be assigned when schedule type is one of date or cron type",
    )
    task: ScheduleTask

class ScheduleResult(BaseModel):
    name: str = Field(default="", title="Schedule name")
    client: ScheduleClient
    id: str = Field(default="", title="Schedule id")

class ScheduleResultCount(BaseModel):
    count: int = Field(default=0, title="Schedule count")


class ScheduleAdmin(BaseModel):
    user: str
    password: str
