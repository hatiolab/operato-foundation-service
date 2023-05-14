from pydantic import BaseModel, Field
from typing import Optional

"""
ServiceData Type Definitions

"""

class ServiceInput(BaseModel):
    application: str = Field(default="", title= "application")
    group: str = Field(default="", title= "group")
    operation: str = Field(default="", title= "operation")

class ServiceOutput(BaseModel):
    application: str = Field(default="", title= "application")
    group: str = Field(default="", title= "group")
    operation: str = Field(default="", title= "operation")
    id: str = Field(default="", title= "service id")
    topic: str = Field(default="", title= "topic")
    endpoint: str = Field(default="", title= "endpoint")


class ServiceEndpointResult(BaseModel):
    result: bool = Field(default=False, title= "service endpoint result")
