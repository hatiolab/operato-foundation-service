from fastapi import FastAPI
from typing import Optional

from api.api_method import (
    api_register,
    api_delete_schedule,
    api_get_schedules,
    api_reset,
)

from api.api_data_type import Schedule, ScheduleAdmin
from schedule.schedule_event_handler import ScheduleEventHandler


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
    schedule_handler = ScheduleEventHandler()
    schedule_handler.initialize()


@fast_api.on_event("shutdown")
async def shutdown_event():
    pass


@fast_api.post("/schedules")
async def register_schedule(inputs: Schedule) -> dict:
    """
    register a schedule event
    """
    return api_register(inputs.dict())


@fast_api.post("/schedules/{id}/update")
async def update_schedule(id: str, inputs: Schedule) -> dict:
    """
    register a schedule event
    """
    return api_register(inputs.dict())


@fast_api.delete("/schedules/{id}")
async def delete_schedule(id: str) -> dict:
    """
    unregister a schedule event
    """
    return api_delete_schedule(id)


@fast_api.delete("/schedules")
async def delete_schedule(
    application: str = "",
    group: str = "",
    type: str = "",
    key: str = "",
    operation: str = "",
) -> dict:
    """
    unregister a schedule event
    """
    return api_delete_schedule(None, operation, application, group, key, type)


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
    return api_get_schedules(None, operation, application, group, key, type)


@fast_api.get("/schedules/{id}")
async def get_schedules_with_resp_id(id: str) -> list:
    """
    list all registered events
    """
    return api_get_schedules(id)


# @fast_api.post("/admin/reset")
# async def reset(admin_info: ScheduleAdmin) -> dict:
#     """
#     reset all queues
#     """
#     return api_reset(admin_info.dict())
