import strawberry
from gql.gql_types import (
    ScheduleEventInput,
    RegisteredEvent,
    UnregisterResult,
    ResetResult,
)
from gql.gql_utils import convert_schevent_to_dict

from schedule.schedule_event_handler import ScheduleEventHandler


"""
****************************************************************************************************************
Mutation
****************************************************************************************************************
"""


@strawberry.type
class Mutation:
    @strawberry.mutation
    def register_schedule(self, registration: ScheduleEventInput) -> RegisteredEvent:
        req_schedule = convert_schevent_to_dict(registration)
        schedule_register = ScheduleEventHandler()
        result = schedule_register.register(req_schedule)

        # return RegisteredEvent(name=result["name"], resp_id=result["resp_id"])
        return RegisteredEvent.from_dict(result)

    @strawberry.mutation
    def update_schedule(self, registration: ScheduleEventInput) -> RegisteredEvent:
        req_schedule = convert_schevent_to_dict(registration)
        schedule_register = ScheduleEventHandler()
        result = schedule_register.update(req_schedule)

        # return RegisteredEvent(name=result["name"], resp_id=result["resp_id"])
        return RegisteredEvent.from_dict(result)

    @strawberry.mutation
    def unregister_schedule(self, schedule_name: str) -> UnregisterResult:
        schedule_register = ScheduleEventHandler()
        result = schedule_register.unregister(schedule_name)
        return UnregisterResult.from_dict(result)

    @strawberry.mutation
    def reset_all(self) -> ResetResult:
        schedule_register = ScheduleEventHandler()
        result = schedule_register.rest()
        return ResetResult.from_dict(result)
