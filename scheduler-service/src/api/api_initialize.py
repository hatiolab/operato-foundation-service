import json
import traceback

from schedule.schedule_event_handler import ScheduleEventHandler

import sys
from helper.logger import Logger

log_message = Logger.get("apinit", Logger.Level.INFO, sys.stdout)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


def api_initialize():
    log_info(f"request list data")
    try:
        schedule_register = ScheduleEventHandler()
        resp = schedule_register.list()
        resp = json.dupms(resp)
        return {
            "list": resp,
        }
    except Exception as ex:
        log_error(traceback.format_exc())
        return {"error(list)": f": {ex}"}
