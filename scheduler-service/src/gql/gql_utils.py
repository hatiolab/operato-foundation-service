from gql.gql_types import (
    ScheduleTaskFailurePolicy,
    ScheduleTaskStatus,
    ScheduleEventInput,
    ScheduleEvent,
)


"""
****************************************************************************************************************
utils
****************************************************************************************************************
"""


def convert_schevent_to_dict(schevent: ScheduleEventInput) -> dict:
    schevent_dict = schevent.to_dict()
    schedule_task = schevent_dict["task"]
    failed_policy = schedule_task.get("failed_policy", ScheduleTaskFailurePolicy.IGNORE)
    schedule_task["failed_policy"] = failed_policy.value
    status = schedule_task.get("status", ScheduleTaskStatus.IDLE)
    schedule_task["status"] = status.value
    return schevent_dict


def covent_dict_to_schevent(schevent_dict: dict) -> ScheduleEvent:
    if schevent_dict == {}:
        return None

    schevent = ScheduleEvent.from_dict(schevent_dict)
    schevent.task.failed_policy = ScheduleTaskFailurePolicy(schevent.task.failed_policy)
    schevent.task.status = ScheduleTaskStatus(schevent.task.status)
    return schevent


def convert_schedule_event_to_dict(schedule_event):
    schedule_dict = schedule_event.to_dict()
    schedule_task = schedule_dict.get("task")
    failed_policy = schedule_task.get("failed_policy", ScheduleTaskFailurePolicy.IGNORE)
    schedule_task["failed_policy"] = failed_policy.value
    status = schedule_task.get("status", ScheduleTaskStatus.IDLE)
    schedule_task["status"] = status.value
    return schedule_dict


def convert_schedule_event_to_dict(schedule_dict):
    pass
