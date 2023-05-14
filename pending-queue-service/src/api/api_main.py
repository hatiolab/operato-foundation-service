from fastapi import FastAPI
from typing import List

from api.api_method import (
    api_put,
    api_pick,
    api_get,
    api_delete,
    api_cancel,
    api_get_list,
    api_reset
)

from api.api_data_type import PendingEvent, PendingStuff
from pending_event.pending_event_handler import PendingEventHandler


fast_api = FastAPI(
    title="PendingEvent Serivce API",
    description="This service registers pending queues and manages registered ones.",
    contact={
        "name": "hatiolab",
        "url": "https://www.hatiolab.com",
        "email": "jinwon@hatiolab.com",
    },
)



@fast_api.on_event("startup")
async def startup_event():
    schedule_handler = PendingEventHandler()
    schedule_handler.initialize()


@fast_api.on_event("shutdown")
async def shutdown_event():
    pass


@fast_api.post("/pending-event")
async def put_pending_queue(inputs: PendingEvent) -> PendingEvent:
    """
    register a pending event
    
    """
    return api_put(inputs)

@fast_api.get("/pending-event/{id}")
async def get_pending_event(
    id: str = "",
) -> PendingEvent:
    """
    get the list of pending events with a specific tag
    
    """
    return api_get(id)

@fast_api.delete("/pending-event/{id}")
async def delete_pending_evebt(
    id: str = "",
) -> PendingEvent:
    """
    get the list of pending events with a specific tag
    
    """
    return api_delete(id)


@fast_api.get("/pending-events")
async def get_pending_list(
    tag: str = "",
) -> List[PendingEvent]:
    """
    get the list of pending events with a specific tag
    
    """
    return api_get_list(tag)


@fast_api.post("/pending-queue/pick")
async def pick_pending_queue(
    tag: str = "",
) -> PendingEvent:
    """
    pick a pending event considering both due and priority
    
    """
    return api_pick(tag)


@fast_api.post("/pending-queue/cancel")
async def cacnel_pending_queues(pending_stuff: PendingStuff) -> PendingEvent:
    """
    delete pending events
    """
    cancel_input = pending_stuff.dict()
    return api_cancel(cancel_input["stuff"])


@fast_api.post("/pending-queue/reset")
async def reset_pending_queues(tag: str) -> List[PendingEvent]:
    """
    clear pending events with the specific tag
    """
    return api_reset(tag)




