import traceback

from api.api_data_type import Schedule
from schedule.schedule_event_handler import ScheduleEventHandler

import sys
from helper.logger import Logger


log_message = Logger.get("apimtd", Logger.Level.INFO, sys.stdout)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


def api_register(schedule: Schedule):
    log_info(f"request registration: {schedule.dict()}")
    schedule_register = ScheduleEventHandler()
    return schedule_register.register(schedule)


def api_delete_schedule(
    resp_id: str,
    operation: str = "",
    application: str = "",
    group: str = "",
    key: str = "",
    client_type: str = "",
):
    log_info(f"request delete: {resp_id}")
    schedule_register = ScheduleEventHandler()
    return schedule_register.delete_schedules(
        resp_id, operation, application, group, key, client_type
    )


def api_update(input_req):
    log_info(f"request update: {input_req}")
    try:
        schedule_register = ScheduleEventHandler()
        return schedule_register.update(input_req)
    except Exception as ex:
        log_error(traceback.format_exc())
        return {"error": f'{input_req["resp_id"]}: {ex}'}


def api_get_schedules(
    resp_id: str,
    operation: str = "",
    application: str = "",
    group: str = "",
    key: str = "",
    client_type: str = "",
    dlq: bool = False,
) -> list:
    log_info(
        f"request get_schedules - resp_id({resp_id}), group({group}), application({application}), operation({operation}), key({key}, type({client_type}))"
    )
    schedule_handler = ScheduleEventHandler()
    return schedule_handler.get_schedules(
        resp_id, operation, application, group, key, client_type, dlq
    )


def api_get_groups() -> list:
    log_info(f"request api_get_groups")
    schedule_handler = ScheduleEventHandler()
    return schedule_handler.get_groups()


def api_delete_group(group_id: str) -> dict:
    log_info(f"request delete schedule")
    schedule_handler = ScheduleEventHandler()
    return schedule_handler.delete_schedules(None, group_id)


def api_reset(admin_info) -> dict:
    log_info(f"request reset")
    schedule_handler = ScheduleEventHandler()
    return schedule_handler.reset(admin_info)
