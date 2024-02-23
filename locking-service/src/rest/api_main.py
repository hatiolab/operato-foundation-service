from fastapi import FastAPI
from typing import Optional

from rest.api_impl import (
    restapi_register,
    restapi_call,
    restapi_release,
    restapi_delete_with_id,
    restapi_delete_with_name,
    restapi_get_with_id,
    restapi_get_with_name,
)

from rest.api_type import (
    Locking,
    LockingId,
    LockingRegisterInput,
    LockingRequestResult,
    LockingResultCount,
)


fast_api = FastAPI(
    title="Locking Service API",
    description="This service registers locking requests and manages registered lockings.",
    contact={
        "name": "hatiolab",
        "url": "https://www.hatiolab.com",
        "email": "jinwon@hatiolab.com",
    },
)


@fast_api.post("/locking")
async def register_locking(inputs: LockingRegisterInput) -> LockingRequestResult:
    """
    register a schedule eventㅋ
    """
    return restapi_register(inputs)


@fast_api.post("/locking/call")
async def call_locking(inputs: LockingId) -> LockingRequestResult:
    """
    register a schedule event
    """
    return await restapi_call(inputs)


@fast_api.post("/locking/release")
async def release_locking(inputs: LockingId) -> LockingRequestResult:
    """
    register a schedule event
    """
    return restapi_release(inputs)


@fast_api.delete("/lockings/{id}")
async def delete_lockings(id: str) -> LockingResultCount:
    """
    delete a locking event with id
    """
    return restapi_delete_with_id(id)


@fast_api.delete("/lockings")
async def delete_lockings(name: str = "") -> LockingResultCount:
    """
    delete a locking event with name
    """
    return restapi_delete_with_name(name)


@fast_api.get("/lockings")
async def get_lockings(name: str) -> list[Locking]:
    """
    get a locking event with name
    """
    return restapi_get_with_name(name)


@fast_api.get("/lockings/{id}")
async def get_lockings_with_resp_id(id: str) -> Locking:
    """
    get a locking event with id
    """
    return restapi_get_with_id(id)
