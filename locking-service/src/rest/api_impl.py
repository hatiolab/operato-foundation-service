from rest.api_type import (
    Locking,
    LockingRequestResult,
    LockingPayload,
    LockingInput,
)
from locking.locking_manager import LockingManager

import sys
from helper.logger import Logger


log_message = Logger.get("rest", Logger.Level.INFO, sys.stdout)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


def restapi_request_locking() -> LockingRequestResult:
    log_info(f"request registration")
    locking_manager = LockingManager()
    result = locking_manager.register()
    return {
        "id": result.id,
        "status": result.status,
    }


async def restapi_try_locking(inputs: LockingInput) -> LockingPayload:
    log_info(f"request call: {inputs.model_dump()}")
    locking_manager = LockingManager()
    result = await locking_manager.try_locking(inputs)
    return {
        "payload": result.payload,
    }


def restapi_release_locking(inputs: LockingInput) -> LockingRequestResult:
    log_info(f"request release: {inputs.model_dump()}")
    locking_manager = LockingManager()
    result = locking_manager.release_locking(inputs)
    return {
        "id": result.id,
    }


def restapi_delete_with_id(
    locking_id: str,
):
    log_info(f"request delete(id): {locking_id}")
    locking_manager = LockingManager()
    return locking_manager.delete_with_id(locking_id)


def restapi_get_with_id(req_id: str) -> Locking:
    log_info(f"request get a locking - Req. ID({req_id})")
    locking_manager = LockingManager()
    return locking_manager.get_with_id(req_id)


def restapi_get_lockings() -> list[Locking]:
    log_info(f"request get all lockings")
    locking_manager = LockingManager()
    return locking_manager.get_lockings()
