import traceback

from restful.rest_type import Schedule
from schedule.scheduler import Scheduler

import sys
from helper.logger import Logger


log_message = Logger.get("apimtd", Logger.Level.INFO, sys.stdout)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


def restapi_register(schedule: Schedule):
    log_info(f"request registration: {schedule.dict()}")
    scheduler = Scheduler()
    return scheduler.register(schedule)


def restapi_delete_schedule_with_id(
    schedule_id: str,
):
    log_info(f"request delete(id): {schedule_id}")
    scheduler = Scheduler()
    return scheduler.delete_schedules_with_id(schedule_id)


def restapi_delete_schedule_with_client(
    client_operation: str = "",
    client_application: str = "",
    client_group: str = "",
    client_key: str = "",
    client_type: str = "",
):
    log_info(
        f"request delete(client): {client_operation}, {client_application}, {client_group}, {client_key}, {client_type}"
    )
    scheduler = Scheduler()
    return scheduler.delete_schedules_with_client(
        client_operation, client_application, client_group, client_key, client_type
    )


def restapi_get_schedules_with_id(
    resp_id: str,
) -> list:
    log_info(f"request get_schedules - resp_id({resp_id})")
    schedule_handler = Scheduler()
    return schedule_handler.get_schedules_with_id(resp_id)


def restapi_get_schedules_with_client(
    client_operation: str = "",
    client_application: str = "",
    client_group: str = "",
    client_key: str = "",
    client_type: str = "",
    dlq: bool = False,
) -> list:
    log_info(
        f"request get_schedules(client) - group({client_group}), application({client_application}), operation({client_operation}), key({client_key}, type({client_type}))"
    )
    schedule_handler = Scheduler()
    return schedule_handler.get_schedules_with_client(
        client_operation, client_application, client_group, client_key, client_type
    )
