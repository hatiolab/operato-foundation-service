
import uuid
from fastapi import HTTPException
import requests

from config import Config
from direct_queue.local_queue import LocalQueue
from direct_queue.pg_queue import PGQueue
from api.api_data_type import ServiceInput


import sys
from helper.logger import Logger

log_message = Logger.get("svcbrk", Logger.Level.INFO, sys.stdout)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


class ServiceBrokerHandler:
    FAILED_CONN_RETRY_COUNT = 10
    FAILED_CONN_RETRY_WAIT = 120 # 2 minutes


    # singleton constructor set
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # to be called once
        cls = type(self)
        if not hasattr(cls, "_init"):
            try:
                # TODO: need to the generalization of schedule_queue creation...
                (queue_type, queue_info) = Config.queue_info()
                if queue_type == "local":
                    self.service_db = LocalQueue(queue_info)
                elif queue_type == "pg":
                    self.service_db = PGQueue(queue_info)
                
                cls._init = True
            except Exception as ex:
                raise HTTPException(status_code=500, detail=f"Exception: {ex}")

    def initialize(self):
        try:
            log_info("initialization done..")
        except Exception as ex:
            log_error(f"Initializaiton Error: {ex}")

    def register_service(self, new_service: ServiceInput):
        try:
            # generate a service id
            service_id = uuid.uuid4()
            
            new_service_dict = new_service.dict()
            new_service_dict["id"] = f"{service_id}"
            new_service_dict["topic"] = f"{service_id}"
            new_service_dict["endpoint"] = f"/endpoint/{new_service_dict['application']}/{new_service_dict['group']}/{new_service_dict['operation']}/{service_id}"

            # store the updated schedule event
            self.service_db.put(service_id, new_service_dict)
            return new_service_dict
        except ValueError as ex:
            raise HTTPException(status_code=404, detail=f"{ex}") 
        except Exception as ex:
            raise HTTPException(status_code=500, detail=f"{ex}")

    def get_service_list(self, application: str, group: str, operation: str):
        result_list = list()
        try:
            key_value_list = self.service_db.get_key_value_list()
            for key_value in key_value_list:
                (_, service) = key_value
                if (not application or service["application"] == application) and (not group or service["group"] == group) and (not operation or service["operation"] == operation):
                    result_list.append(service)
            return result_list
        except ValueError as ex:
            raise HTTPException(status_code=404, detail=f"{ex}") 
        except Exception as ex:
            raise HTTPException(status_code=500, detail=f"{ex}")
        
    def get_service_list_with_id(self, id: str):
        found_service = None
        try:
            key_value_list = self.service_db.get_key_value_list()
            for key_value in key_value_list:
                (_, service) = key_value
                if service["id"] == id:
                    found_service = service
            if not found_service:
                raise ValueError
                    
            return found_service
        except ValueError as ex:
            raise HTTPException(status_code=404, detail=f"Exception: {ex}")
        except Exception as ex:
            raise HTTPException(status_code=500, detail=f"Exception: {ex}")
        
    def delete_service_list(self, service: ServiceInput):
        result_list = list()
        try:
            key_value_list = self.service_db.get_key_value_list()
            for key_value in key_value_list:
                (key, service) = key_value
                if service["application"] == service.application and service["group"] == service.group and service["operation"] == service.operation:
                    result_list.append(service)
                    self.service_db.pop(key)
            if not result_list:
                raise ValueError
            return result_list
        except ValueError as ex:
            raise HTTPException(status_code=404, detail=f"{ex}") 
        except Exception as ex:
            raise HTTPException(status_code=500, detail=f"{ex}")
        
    def delete_service_with_id(self, id: str):
        found_service = None
        try:
            key_value_list = self.service_db.get_key_value_list()
            for key_value in key_value_list:
                (key, service) = key_value
                if service["id"] == id:
                    found_service = service
                    self.service_db.pop(key)
            if not found_service:
                raise ValueError
            return found_service
        except ValueError as ex:
            raise HTTPException(status_code=404, detail=f"{ex}") 
        except Exception as ex:
            raise HTTPException(status_code=500, detail=f"{ex}")


    def handle_endpoint(self, application: str, group: str, operation: str, id: str, body: dict):
        log_info(f"handle_endpoint: {application}, {group}, {operation}, {id}")
        try:
            service_list = self.get_service_list(application, group, operation)
            if not service_list:
                raise ValueError
            
            found_service = None
            for service in service_list:
                if service["id"] == id:   
                    found_service = service
            if not found_service:
                raise ValueError
            
            # TODO: notificate found_service to the service here.
            client_connection = found_service["service_callback"]

            # 
            service_result = self.send_to_service(client_connection, body)

            return service_result       
        except Exception as ex:
            raise ex
        
    def send_to_service(self, callback: str, body: dict):
        try:
            res = requests.post(callback["host"], headers=callback["headers"], data=body)
            log_debug(f"result: {res.status_code}")
            if res.status_code == 200:
                return res.json()
            else:
                raise HTTPException(status_code=res.status_code, detail=f"Error: {res.status_code}")
        except HTTPException as ex:
            raise ex
        
