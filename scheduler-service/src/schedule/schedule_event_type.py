from enum import Enum
from datetime import datetime

# from croniter import croniter
import pytz

from schedule.schedule_util import calculate_cron_unit
from schedule.schedule_cron import CronTime

import sys
from helper.logger import Logger


log_message = Logger.get("schtyp", Logger.Level.INFO, sys.stdout)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


class ScheduleTaskFailurePolicy(str, Enum):
    IGNORE = "ignore"
    RETRY = "retry"


class ScheduleTaskStatus:
    IDLE = "idle"
    WAITING = "waiting"
    RETRY = "retry"
    DONE = "done"
    FAILED = "failed"


class ScheduleType:
    NOW = "now"
    DELAY = "delay"
    DATE = "date"
    CRON = "cron"
    DELAY_RECUR = "delay-recur"

    @staticmethod
    def is_available_type(schedule_type):
        return schedule_type in [
            ScheduleType.CRON,
            ScheduleType.DELAY_RECUR,
            ScheduleType.NOW,
            ScheduleType.DELAY,
            ScheduleType.DATE,
        ]

    @staticmethod
    def is_recurring(schedule_type):
        return schedule_type in [ScheduleType.CRON, ScheduleType.DELAY_RECUR]

    @staticmethod
    def get_next_and_delay(schedule_event, tz):
        base = datetime.now().astimezone(pytz.timezone(tz))
        if schedule_event["type"] == ScheduleType.CRON:
            # cron_elements = (
            #     schedule_event["schedule"].split()
            #     if type(schedule_event["schedule"]) is str
            #     else str(schedule_event["schedule"]).split()
            # )

            next_time = None
            delay = 0
            """
            TODO: check if this routine doesn't have any exception or issue
            """
            cron_time = CronTime(schedule_event["schedule"], tz)
            next_datetime = cron_time.get_next(base, "Asia/Seoul")
            next_time = next_datetime.timestamp()
            delay = next_time - datetime.timestamp(base)

            # if len(cron_elements) == 5:
            #     utc_cron = Converter(schedule_event["schedule"], tz).to_utc_cron()
            #     iter = croniter(utc_cron, base)
            #     next_time = datetime.timestamp(iter.get_next(datetime))
            #     delay = next_time - datetime.timestamp(base)
            # elif len(cron_elements) == 6:
            #     schedule_data = " ".join(cron_elements[1:])
            #     utc_cron = Converter(schedule_data, tz).to_utc_cron()
            #     iter = croniter(utc_cron, base)
            #     next_time = datetime.timestamp(iter.get_next(datetime))
            #     secs = calculate_cron_unit(cron_elements[0])
            #     delay = next_time - datetime.timestamp(base) + secs
            # else:
            #     raise Exception(f'wrong format error: {schedule_event["schedule"]}')
        elif (
            schedule_event["type"] == ScheduleType.DELAY_RECUR
            or schedule_event["type"] == ScheduleType.DELAY
        ):
            next_time = datetime.timestamp(base) + int(schedule_event["schedule"])
            delay = next_time - datetime.timestamp(base)
        elif schedule_event["type"] == ScheduleType.NOW:
            next_time = datetime.timestamp(base)
            delay = 0
        elif schedule_event["type"] == ScheduleType.DATE:
            local_dt = pytz.timezone(tz).localize(
                datetime.fromisoformat(schedule_event["schedule"])
            )
            utc_dt = local_dt.astimezone(pytz.UTC)
            next_time = utc_dt.timestamp()
            delay = next_time - datetime.timestamp(base)
        else:
            raise Exception("Invalid schedule type")

        log_info(
            f'schedule({schedule_event["name"]}), next_time({int(next_time)}), delay({delay})'
        )

        return (int(next_time), delay if delay >= 0 else 0)
