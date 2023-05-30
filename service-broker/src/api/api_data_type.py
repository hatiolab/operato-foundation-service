from pydantic import BaseModel, Field
from typing import Optional

"""
ServiceData Type Definitions

"""

class ServiceCallbackConnection(BaseModel):
    host: str = Field(default="", title= "host")
    headers: Optional[dict] = Field(default= {"Content-Type": "application/json"}, title= "headers")



class ServiceInput(BaseModel):
    application: str = Field(default="", title= "application")
    group: str = Field(default="", title= "group")
    operation: str = Field(default="", title= "operation")
    service_callback: ServiceCallbackConnection = Field(default=ServiceCallbackConnection(), title= "callback")
    


class ServiceOutput(BaseModel):
    application: str = Field(default="", title= "application")
    group: str = Field(default="", title= "group")
    operation: str = Field(default="", title= "operation")
    service_callback: ServiceCallbackConnection = Field(default=ServiceCallbackConnection(), title= "callback")
    id: str = Field(default="", title= "service id")
    topic: str = Field(default="", title= "topic")
    endpoint: str = Field(default="", title= "endpoint")


class ServiceEndpointResult(BaseModel):
    result: bool = Field(default=False, title= "service endpoint result")
