import asyncio
import uuid
from datetime import datetime, timezone
from fastapi import HTTPException
import threading

from datetime import datetime
from time import sleep, time

from restful.rest_type import Locking
from config import Config
from locking_queue.locking_queue import LockingQueue

import sys
from helper.logger import Logger

log_message = Logger.get(
    "scheduler",
    Logger.Level.DEBUG if Config.locking_debug() else Logger.Level.INFO,
    sys.stdout,
)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


class LockingManager:
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
                # TODO: need to the generalization of locking_queue creation...
                database_info = Config.database()
                self.locking_queue = LockingQueue(database_info)
                self.finalized = False
                cls._init = True
            except Exception as ex:
                raise ex

    def register(self, locking: Locking):
        try:
            if locking is None:
                raise HTTPException(
                    status_code=400, detail="Bad Request(No valid input parameters)"
                )

            if locking.name is None or locking.name == "":
                raise HTTPException(
                    status_code=400, detail="Bad Request(No valid input parameters)"
                )

            locking.status = "READY"
            locking.wait_until
            locking.id = str(uuid.uuid4())

            self.locking_queue.insert(
                locking.id, locking.name, locking.status, locking.wait_until
            )

        except Exception as ex:
            raise HTTPException(status_code=500, detail=f"Error: {ex}")

        return locking

    async def call(self, locking: Locking):
        wait_unitl = locking.wait_until

        await self.locking_queue.listen_trigger_async()

        locking.status = "Released"

        return locking

    def delete_with_id(
        self,
        schedule_id: str,
    ):
        try:
            if type(schedule_id) is not str or schedule_id == "":
                raise HTTPException(
                    status_code=400, detail="Bad Request(No valid input parameters)"
                )

            deleted_list = self.locking_queue.delete_with_id(schedule_id)

            found_count = len(deleted_list)

            if found_count == 0:
                raise HTTPException(status_code=404, detail="Item not found")

        except Exception as ex:
            raise ex

        return {"count": found_count}

    def delete_with_name(self, name: str):
        try:
            if type(name) is not str or name == "":
                raise HTTPException(
                    status_code=400, detail="Bad Request(No valid input parameters)"
                )

            delete_list = self.locking_queue.delete_with_name(name)

            found_count = len(delete_list)

            if found_count == 0:
                raise HTTPException(status_code=404, detail="Item not found")

        except Exception as ex:
            raise ex

        return {"count": found_count}

    def get_with_id(
        self,
        locking_id: str,
    ):
        try:
            found_schedule = self.locking_queue.get_with_id(locking_id)

            if len(found_schedule) == 0:
                raise HTTPException(status_code=404, detail="Item not found")

        except Exception as ex:
            log_error(f"Exception: {ex}")
            raise ex

        return found_schedule

    def get_with_name(self, name: str):
        found_schedules = list()

        try:
            found_schedules = self.locking_queue.get_with_name(name)

            if len(found_schedules) == 0:
                raise HTTPException(status_code=404, detail="Item not found")

        except Exception as ex:
            log_error(f"Exception: {ex}")
            raise ex

        return found_schedules
