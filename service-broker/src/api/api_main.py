from fastapi import FastAPI
from typing import List

from api.api_method import (
    api_post,
    api_get_services,
    api_get_service_with_id,
    api_delete_services,
    api_delete_service_with_id,
    api_handle_endpoint
)

from api.api_data_type import ServiceInput, ServiceOutput
from service_broker.service_broker_handler import ServiceBrokerHandler


fast_api = FastAPI(
    title="Service Broker API",
    description="This service broker manages the callback endpoints for things-factory platform.",
    contact={
        "name": "hatiolab",
        "url": "https://www.hatiolab.com",
        "email": "jinwon@hatiolab.com",
    },
)



@fast_api.on_event("startup")
async def startup_event():
    service_broker_handler = ServiceBrokerHandler()
    service_broker_handler.initialize()


@fast_api.on_event("shutdown")
async def shutdown_event():
    pass


@fast_api.post("/service")
async def post_service(service: ServiceInput) -> ServiceOutput:
    """
    register a service
    
    """
    return api_post(service)

@fast_api.get("/services")
async def get_services(application: str = "", group: str = "", operation: str = "") -> List[ServiceOutput]:
    """
    get services with selectable filters(application, group, operation)
    
    """
    return api_get_services(application, group, operation)

@fast_api.get("/service/{id}")
async def get_service(id: str) -> ServiceOutput:
    """
    get a service with id
    
    """
    return api_get_service_with_id(id)

@fast_api.delete("/services")
async def get_services(service: ServiceInput) -> List[ServiceOutput]:
    """
    delete services with selectable filters(application, group, operation)
    
    """
    return api_delete_services(service)

@fast_api.delete("/service/{id}")
async def delete_service(id: str) -> ServiceOutput:
    """
    delete a service with id
    
    """
    return api_delete_service_with_id(id)


@fast_api.post("/endpoint/{application}/{group}/{operation}/{id}")
async def handle_endpoints(application: str, group: str, operation: str, id: str, body: dict) -> dict:
    """
    handle a callback endpoint with selectable filters(application, group, operation, id)
    
    """
    return api_handle_endpoint(application, group, operation, id, body)

