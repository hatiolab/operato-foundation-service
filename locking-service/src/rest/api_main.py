from fastapi import FastAPI
from typing import Optional

from rest.api_impl import (
    restapi_request_locking,
    restapi_try_locking,
    restapi_release_locking,
    restapi_delete_with_id,
    restapi_get_with_id,
    restapi_get_lockings,
)

from rest.api_type import (
    Locking,
    LockingInput,
    LockingPayload,
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
async def request_locking() -> LockingRequestResult:
    """
    request a locking
    """
    return restapi_request_locking()


@fast_api.post("/locking/try")
async def try_locking(inputs: LockingInput) -> LockingPayload:
    """
    try to lock with a locking id
    """
    return await restapi_try_locking(inputs)


@fast_api.post("/locking/release")
async def release_locking(inputs: LockingInput) -> LockingInput:
    """
    release a locking with a locking id and payload

    """
    return restapi_release_locking(inputs)


@fast_api.delete("/lockings/{id}")
async def delete_lockings(id: str) -> LockingResultCount:
    """
    delete a locking with id
    """
    return restapi_delete_with_id(id)


@fast_api.get("/lockings/{id}")
async def get_lockings_with_resp_id(id: str) -> Locking:
    """
    get a locking with id
    """
    return restapi_get_with_id(id)


@fast_api.get("/lockings")
async def get_lockings() -> list[Locking]:
    """
    get all lockings registered but not released
    """
    return restapi_get_lockings()
