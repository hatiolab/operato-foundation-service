from fastapi import FastAPI
import strawberry
from strawberry.fastapi import GraphQLRouter

from schedule.schedule_event_handler import ScheduleEventHandler

from gql.gql_mutation import Mutation
from gql.gql_query import Query


schema = strawberry.Schema(query=Query, mutation=Mutation)
graphql_app = GraphQLRouter(schema)


fast_api = FastAPI(
    title="Scheduler Service API",
    description="This service registers schedule information and manages registered schedules.",
    contact={
        "name": "hatiolab",
        "url": "https://www.hatiolab.com",
        "email": "jinwon@hatiolab.com",
    },
)
fast_api.include_router(graphql_app, prefix="/graphql")


@fast_api.on_event("startup")
async def startup_event():
    schedule_handler = ScheduleEventHandler()
    schedule_handler.initialize()


@fast_api.on_event("shutdown")
async def shutdown_event():
    pass
