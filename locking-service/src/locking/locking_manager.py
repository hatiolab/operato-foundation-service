import uuid
from fastapi import HTTPException

from rest.api_type import Locking, LockingRegisterInput, LockingId
from config import Config
from locking.locking_queue import LockingQueue

import sys
from helper.logger import Logger

log_message = Logger.get(
    "locking",
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
            wait_until=locking_register_input.wait_until,
            status="READY",
            id="",
        )
        try:
            if locking is None:
                raise HTTPException(
                    status_code=400, detail="Bad Request(No valid input parameters)"
                )

            locking.status = "READY"
            locking.id = self.locking_queue.insert(locking.status, locking.wait_until)

        except Exception as ex:
            raise HTTPException(status_code=500, detail=f"Error: {ex}")

        return locking

    async def try_locking(self, locking_id: LockingId) -> Locking:
        get_result = self.locking_queue.get_with_id(locking_id.id)

        if len(get_result) == 0:
            raise HTTPException(status_code=404, detail="Item not found")

        locking = Locking(
            wait_until=str(get_result[0][2]),
            status=get_result[0][1],
            id=get_result[0][0],
        )

        wait_unitl = locking.wait_until

        self.locking_queue.update(locking.id, "LOCKED", int(locking.wait_until))

        notified = await self.locking_queue.wait_for_status_released(
            locking.id, int(wait_unitl)
        )

        locking.status = "RELEASED" if notified else "FAILED"

        return locking

    def release_lock(self, locking_id: LockingId) -> Locking:
        get_result = self.locking_queue.get_with_id(locking_id.id)

        if len(get_result) == 0:
            raise HTTPException(status_code=404, detail="Item not found")

        locking = Locking(
            wait_until=str(get_result[0][2]),
            status=get_result[0][1],
            id=get_result[0][0],
        )

        self.locking_queue.update(locking.id, "RELEASED", locking.wait_until)
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

    def get_with_id(
        self,
        locking_id: str,
    ) -> Locking:
        locking = Locking(
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

        locking.wait_until = found_lockings[0][2]
        locking.status = found_lockings[0][1]

        return locking

    def get_lockings(
        self,
    ) -> list[Locking]:
        try:
            found_lockings = self.locking_queue.get_locks()

            if len(found_lockings) == 0:
                raise HTTPException(status_code=404, detail="Item not found")

        except Exception as ex:
            log_error(f"Exception: {ex}")
            raise ex

        locking_list: list[Locking] = []
        for found_locking in found_lockings:
            locking = Locking(
                wait_until=str(found_locking[2]),
                status=found_locking[1],
                id=found_locking[0],
            )
            locking_list.append(locking)

        return locking_list
