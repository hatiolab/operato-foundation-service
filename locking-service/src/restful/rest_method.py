import traceback

from restful.rest_type import (
    Locking,
    LockingRegisterInput,
    LockingRequestResult,
    LockingId,
)
from locking.locking_manager import LockingManager

import sys
from helper.logger import Logger


log_message = Logger.get("apimtd", Logger.Level.INFO, sys.stdout)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


def restapi_register(inputs: LockingRegisterInput) -> LockingRequestResult:
    log_info(f"request registration: {inputs.model_dump()}")
    locking_manager = LockingManager()
    result = locking_manager.register(inputs)
    return {
        "name": result.name,
        "id": result.id,
        "status": result.status,
    }


async def restapi_call(inputs: LockingId) -> LockingRequestResult:
    log_info(f"request call: {inputs.model_dump()}")
    locking_manager = LockingManager()
    result = await locking_manager.call(inputs)
    return {
        "name": result.name,
        "id": result.id,
        "status": result.status,
    }


def restapi_release(inputs: LockingId) -> LockingRequestResult:
    log_info(f"request release: {inputs.model_dump()}")
    locking_manager = LockingManager()
    result = locking_manager.release(inputs)
    return {
        "name": result.name,
        "id": result.id,
        "status": result.status,
    }


def restapi_delete_with_id(
    locking_id: str,
):
    log_info(f"request delete(id): {locking_id}")
    locking_manager = LockingManager()
    return locking_manager.delete_with_id(locking_id)


def restapi_delete_with_name(name: str = ""):
    log_info(f"request delete(client): {name}")
    locking_manager = LockingManager()
    return locking_manager.delete_with_name(name)


def restapi_get_with_id(req_id: str) -> Locking:
    log_info(f"request get a locking - Req. ID({req_id})")
    locking_manager = LockingManager()
    return locking_manager.get_with_id(req_id)


def restapi_get_with_name(name: str = "") -> list[Locking]:
    log_info(f"request get_with_name - name({name})")
    locking_manager = LockingManager()
    return locking_manager.get_with_name(name)
