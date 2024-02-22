import uuid
from fastapi import HTTPException

from restful.rest_type import Locking, LockingRegisterInput, LockingId
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

    def register(self, locking_register_input: LockingRegisterInput) -> Locking:
        locking = Locking(
            name=locking_register_input.name,
            wait_until=locking_register_input.wait_until,
            status="READY",
            id="",
        )
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
            locking.id = self.locking_queue.insert(
                locking.name, locking.status, locking.wait_until
            )

        except Exception as ex:
            raise HTTPException(status_code=500, detail=f"Error: {ex}")

        return locking

    async def call(self, locking_id: LockingId) -> Locking:
        get_result = self.locking_queue.get_with_id(locking_id.id)

        if len(get_result) == 0:
            raise HTTPException(status_code=404, detail="Item not found")

        locking = Locking(
            name=get_result[0][1],
            wait_until=str(get_result[0][3]),
            status=get_result[0][2],
            id=get_result[0][0],
        )

        wait_unitl = locking.wait_until

        self.locking_queue.update(locking.name, "LOCKED", int(locking.wait_until))

        notified = await self.locking_queue.wait_for_status_released(
            locking.name, int(wait_unitl)
        )

        locking.status = "RELEASED" if notified else "FAILED"

        return locking

    def release(self, locking_id: LockingId) -> Locking:
        get_result = self.locking_queue.get_with_id(locking_id.id)

        if len(get_result) == 0:
            raise HTTPException(status_code=404, detail="Item not found")

        locking = Locking(
            name=get_result[0][1],
            wait_until=str(get_result[0][3]),
            status=get_result[0][2],
            id=get_result[0][0],
        )

        self.locking_queue.update(locking.name, "RELEASED", locking.wait_until)
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
    ) -> Locking:
        locking = Locking(
            name="",
            wait_until="",
            status="READY",
            id=locking_id,
        )

        try:
            found_lockings = self.locking_queue.get_with_id(locking_id)

            if len(found_lockings) == 0:
                raise HTTPException(status_code=404, detail="Item not found")

        except Exception as ex:
            log_error(f"Exception: {ex}")
            raise ex

        assert len(found_lockings) == 1

        locking.name = found_lockings[0][1]
        locking.wait_until = found_lockings[0][3]
        locking.status = found_lockings[0][2]

        return locking

    def get_with_name(self, name: str) -> list[Locking]:
        founds = list()

        try:
            founds = self.locking_queue.get_with_name(name)

            if len(founds) == 0:
                raise HTTPException(status_code=404, detail="Item not found")

        except Exception as ex:
            log_error(f"Exception: {ex}")
            raise ex

        lockings = list()
        for found in founds:
            locking = Locking(
                name=found[1],
                wait_until=str(found[3]),
                status=found[2],
                id=found[0],
            )
            lockings.append(locking)

        return lockings
