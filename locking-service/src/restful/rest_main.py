from fastapi import FastAPI
from typing import Optional

from restful.rest_method import (
    restapi_register,
    restapi_delete_with_id,
    restapi_delete_with_name,
    restapi_get_with_id,
    restapi_get_with_name,
)

from restful.rest_type import Locking, LockingRequestResult, LockingResultCount
from locking.locking_manager import LockingManager


fast_api = FastAPI(
    title="Locking Service API",
    description="This service registers locking requests and manages registered lockings.",
    contact={
        "name": "hatiolab",
        "url": "https://www.hatiolab.com",
        "email": "jinwon@hatiolab.com",
    },
)


# @fast_api.on_event("startup")
# async def startup_event():
#     pass


# @fast_api.on_event("shutdown")
# async def shutdown_event():
#     Scheduler.finalize()


@fast_api.post("/locking")
async def register_schedule(inputs: Locking) -> LockingRequestResult:
    """
    register a schedule event
    """
    return restapi_register(inputs)


@fast_api.delete("/lockings/{id}")
async def delete_schedule(id: str) -> LockingResultCount:
    """
    delete a locking event with id
    """
    return restapi_delete_with_id(id)


@fast_api.delete("/lockings")
async def delete_schedule(name: str = "") -> LockingResultCount:
    """
    delete a locking event with name
    """
    return restapi_delete_with_name(name)


@fast_api.get("/lockings")
async def get_schedules(name: str) -> list:
    """
    get a locking event with name
    """
    return restapi_get_with_name(name)


@fast_api.get("/lockings/{id}")
async def get_schedules_with_resp_id(id: str) -> list:
    """
    get a locking event with id
    """
    return restapi_get_with_id(id)
