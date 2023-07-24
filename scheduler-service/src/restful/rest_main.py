from fastapi import FastAPI
from typing import Optional

from restful.rest_method import (
    restapi_register,
    restapi_delete_schedule_with_id,
    restapi_delete_schedule_with_client,
    restapi_get_schedules_with_id,
    restapi_get_schedules_with_client,
)

from restful.rest_type import Schedule, ScheduleResult, ScheduleResultCount
from schedule.scheduler import Scheduler


fast_api = FastAPI(
    title="Scheduler Service API",
    description="This service registers schedule information and manages registered schedules.",
    contact={
        "name": "hatiolab",
        "url": "https://www.hatiolab.com",
        "email": "jinwon@hatiolab.com",
    },
)


@fast_api.on_event("startup")
async def startup_event():
    schedule_handler = Scheduler()
    schedule_handler.initialize()


@fast_api.on_event("shutdown")
async def shutdown_event():
    Scheduler.finalize()


@fast_api.post("/schedules")
async def register_schedule(inputs: Schedule) -> ScheduleResult:
    """
    register a schedule event
    """
    return restapi_register(inputs)


@fast_api.post("/schedules/{id}/update")
async def update_schedule(id: str, inputs: Schedule) -> ScheduleResult:
    """
    register a schedule event
    """
    return restapi_register(inputs)


@fast_api.delete("/schedules/{id}")
async def delete_schedule(id: str) -> ScheduleResultCount:
    """
    unregister a schedule event
    """
    return restapi_delete_schedule_with_id(id)


@fast_api.delete("/schedules")
async def delete_schedule(
    application: str = "",
    group: str = "",
    type: str = "",
    key: str = "",
    operation: str = "",
) -> ScheduleResultCount:
    """
    unregister a schedule event
    """
    return restapi_delete_schedule_with_client(operation, application, group, key, type)


@fast_api.get("/schedules")
async def get_schedules(
    application: str = "",
    group: str = "",
    type: str = "",
    key: str = "",
    operation: str = "",
) -> list:
    """
    list all registered events
    """
    return restapi_get_schedules_with_client(operation, application, group, key, type)


@fast_api.get("/schedules/{id}")
async def get_schedules_with_resp_id(id: str) -> list:
    """
    list all registered events
    """
    return restapi_get_schedules_with_id(id)
