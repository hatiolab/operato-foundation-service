from typing import List
from api.api_data_type import ServiceInput, ServiceOutput, ServiceEndpointResult
from service_broker.service_broker_handler import ServiceBrokerHandler

import sys
from helper.logger import Logger


log_message = Logger.get("apimtd", Logger.Level.INFO, sys.stdout)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


def api_post(new_service: ServiceInput) -> ServiceOutput:
    log_info(f"called api_post with {new_service}")
    pending_event_handler = ServiceBrokerHandler()
    return pending_event_handler.register_service(new_service)

def api_get_services(application: str, group: str, operation: str) -> List[ServiceOutput]:
    log_info(f"called api_get_services with {application}, {group}, {operation}")
    pending_event_handler = ServiceBrokerHandler()
    return pending_event_handler.get_service_list(application, group, operation)

def api_get_service_with_id(id: str) -> ServiceOutput:
    log_info(f"called api_get_service_with_id with {id}")
    pending_event_handler = ServiceBrokerHandler()
    return pending_event_handler.get_service_list_with_id(id)

def api_delete_services(id: str) -> ServiceOutput:
    log_info(f"called api_get_service_with_id with {id}")
    pending_event_handler = ServiceBrokerHandler()
    return pending_event_handler.delete_service_list(id)

def api_delete_service_with_id(id: str) -> ServiceOutput:
    log_info(f"called api_get_service_with_id with {id}")
    pending_event_handler = ServiceBrokerHandler()
    return pending_event_handler.delete_service_with_id(id)

def api_handle_endpoint(application: str, group:str, operation:str, id:str) -> ServiceEndpointResult:
    log_info(f"called api_handle_endpoint with {application}, {group}, {operation}, {id}")
    pending_event_handler = ServiceBrokerHandler()
    return pending_event_handler.handle_endpoint(application, group, operation, id)

