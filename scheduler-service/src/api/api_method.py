import traceback

from api.api_data_type import Schedule
from schedule.scheduler import Scheduler

import sys
from helper.logger import Logger


log_message = Logger.get("apimtd", Logger.Level.INFO, sys.stdout)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


def api_register(schedule: Schedule):
    log_info(f"request registration: {schedule.dict()}")
    scheduler = Scheduler()
    return scheduler.register(schedule)


def api_delete_schedule_with_id(
    schedule_id: str,
):
    log_info(f"request delete: {schedule_id}")
    scheduler = Scheduler()
    return scheduler.delete_schedules_with_id(schedule_id)


def api_delete_schedule_with_client(
    client_operation: str = "",
    client_application: str = "",
    client_group: str = "",
    client_key: str = "",
    client_type: str = "",
):
    log_info(
        f"request delete: {client_operation}, {client_application}, {client_group}, {client_key}, {client_type}"
    )
    scheduler = Scheduler()
    return scheduler.delete_schedules_with_client(
        client_operation, client_application, client_group, client_key, client_type
    )


def api_update(input_req):
    log_info(f"request update: {input_req}")
    try:
        scheduler = Scheduler()
        return scheduler.update(input_req)
    except Exception as ex:
        log_error(traceback.format_exc())
        return {"error": f'{input_req["resp_id"]}: {ex}'}


def api_get_schedules_with_id(
    resp_id: str,
) -> list:
    log_info(f"request get_schedules - resp_id({resp_id})")
    schedule_handler = Scheduler()
    return schedule_handler.get_schedules_with_id(resp_id)


def api_get_schedules_with_client(
    client_operation: str = "",
    client_application: str = "",
    client_group: str = "",
    client_key: str = "",
    client_type: str = "",
    dlq: bool = False,
) -> list:
    log_info(
        f"request get_schedules - group({client_group}), application({client_application}), operation({client_operation}), key({client_key}, type({client_type}))"
    )
    schedule_handler = Scheduler()
    return schedule_handler.get_schedules_with_client(
        client_operation, client_application, client_group, client_key, client_type
    )


def api_get_groups() -> list:
    log_info(f"request api_get_groups")
    schedule_handler = Scheduler()
    return schedule_handler.get_groups()


def api_reset(admin_info) -> dict:
    log_info(f"request reset")
    schedule_handler = Scheduler()
    return schedule_handler.reset(admin_info)
