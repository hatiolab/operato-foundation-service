import strawberry
from gql.gql_types import ListInput, ScheduleEvent
from gql.gql_utils import covent_dict_to_schevent

from typing import List

from schedule.schedule_event_handler import ScheduleEventHandler


"""
****************************************************************************************************************
Query
****************************************************************************************************************
"""


@strawberry.type
class Query:
    @strawberry.field
    def scheduled_list(self, queue_type: ListInput) -> List[ScheduleEvent]:
        queue_type_dict = queue_type.to_dict()

        schedule_handler = ScheduleEventHandler()
        result_list = schedule_handler.list(queue_type_dict)

        scheduled_event_list = list()
        for result in result_list:
            scheduled_event_list.append(covent_dict_to_schevent(result))

        return scheduled_event_list
