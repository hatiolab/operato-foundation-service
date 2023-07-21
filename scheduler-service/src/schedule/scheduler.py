import asyncio
import uuid
import json
from datetime import datetime, timezone
from fastapi import HTTPException
import threading
from datetime import datetime

from api.api_data_type import Schedule
from config import Config
from schedule_queue.pg_schedule_queue import PGScheduleQueue
from schedule.schedule_type import ScheduleType, ScheduleTaskStatus
from task.task_mgr import TaskManager
from task.task_import import TASK_ACTIVE_MODULE_LIST

from history.db_engine import initialize_global_database, get_session
from history.tables.table_schedule_history import ScheduleEventHistory


import sys
from helper.logger import Logger

log_message = Logger.get("schevt", Logger.Level.INFO, sys.stdout)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


class Scheduler:
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
                database_info = Config.database()
                self.schedule_queue = PGScheduleQueue(database_info)
                self.running_schedules = dict()
                self.finalized = False
                cls._init = True
            except Exception as ex:
                raise ex

    @classmethod
    def process_schedule(cls):
        print(f"called process_schedule: {str(datetime.now())}")

        # 1. pop the schedule with the lowest next_schedule value
        scheduler = cls()
        # schedule = scheduler.schedule_queue.pop()
        # print(f"schedule: {schedule}")

        # TODO: 0.5(500ms) is a part of this application configuration.
        if not scheduler.finalized:
            threading.Timer(0.5, Scheduler.process_schedule).start()

    def start_schedule(self):
        Scheduler.process_schedule()

    def stop_schedule(self):
        self.finalized = True

    def initialize(self):
        try:
            self.start_schedule()
            log_info("initialization done..")
        except Exception as ex:
            log_error(f"Initializaiton Error: {ex}")

    @classmethod
    def finalize(cls):
        scheduler = cls()
        scheduler.stop_schedule()

    def is_task_available(self, type: str, connection_info: dict) -> bool:
        """
        check if this task is available

        """
        if type == "rest":
            if not connection_info.get("host", ""):
                raise Exception(f"{type} type must have 'host'.")
        elif type == "kafka" or type == "redis":
            if not connection_info.get("topic", "") or not connection_info.get(
                "data", ""
            ):
                raise Exception(f"{type} type must have 'topic' and 'data'")
        else:
            pass

    def create_client_unique_name(self, client_info: dict) -> str:
        """
        create a unique key for the client
        -> {applicaiton name},{group name},{client type},{client key},{operation name}

        """
        return ",".join(
            [
                client_info.get("application", ""),
                client_info.get("group", ""),
                client_info.get("type", ""),
                client_info.get("key", ""),
                client_info.get("operation", ""),
            ]
        )

    def register(self, schedule: Schedule):
        try:
            schedule_event = schedule.dict()

            client_info = schedule_event.get("client")
            schedule_unique_name = self.create_client_unique_name(client_info)

            # 1. check if unique key(operation) is duplicated.
            (id, _, _, _) = self.schedule_queue.get_with_name(schedule_unique_name)
            schedule_id = id if id else str(uuid.uuid4())

            # 2. get the next timestamp and the delay based on schedule format
            tz = schedule_event.get("timezone", "Asia/Seoul")
            (next_time, _) = ScheduleType.get_next_and_delay(schedule_event, tz)

            # TODO: add some additional key-values (status, ...)
            new_schedule_task = schedule_event["task"]
            # 4.1 check if task type is available
            if not new_schedule_task["type"] in TaskManager.all():
                raise Exception("task type unavailable.")
            # 4.2 check if task connection is available
            input_task_connection = new_schedule_task["connection"]
            self.is_task_available(new_schedule_task["type"], input_task_connection)
            # 4.3 update some additional data like next and retry
            new_schedule_task["retry_count"] = 0
            new_schedule_task["next"] = next_time
            new_schedule_task["status"] = ScheduleTaskStatus.IDLE
            new_schedule_task["iteration"] = 0
            new_schedule_task["lastRun"] = None

            # 5. store the updated schedule event
            self.schedule_queue.put(
                schedule_id, schedule_unique_name, next_time, schedule_event
            )
            self.put_event_to_history_db("register", schedule_event)

            # 7. return the result with the response id
            return {
                "name": schedule_event["name"],
                "client": {
                    "application": client_info["application"],
                    "group": client_info["group"],
                    "type": client_info["type"],
                    "key": client_info["key"],
                    "operation": client_info["operation"],
                },
                "id": schedule_id,
            }
        except Exception as ex:
            print(f"Exception: {ex}")
            raise ex

    ##########################################################################################
    # TODO: 기존에는 별도의 id를 생성했으나, resp_id를 등록 시에 생성되는 레코드 id로 사용하도록 변경
    ##########################################################################################
    def unregister(self, schedule_id: str):
        try:
            if type(schedule_id) is not str or schedule_id == "":
                raise Exception(f"schedule_id is not available.. {schedule_id}")

            (_, schedule) = self.schedule_queue.delete(schedule_id)
            if schedule:
                self.put_event_to_history_db("unregister", schedule)

        except Exception as ex:
            raise ex

        return {"count": 1 if schedule else 0}

    def delete_schedules_with_id(
        self,
        schedule_id: str,
    ):
        try:
            if type(schedule_id) is not str or schedule_id == "":
                raise HTTPException(
                    status_code=400, detail="Bad Request(No valid input parameters)"
                )

            deleted_list = self.schedule_queue.delete(schedule_id)

            if len(deleted_list) == 0:
                raise HTTPException(status_code=404, detail="Item not found")

        except Exception as ex:
            raise ex

        return {"count": len(deleted_list)}

    def delete_schedules_with_client(
        self,
        client_operation: str,
        client_application: str,
        client_group: str,
        client_key: str,
        client_type: str,
    ):
        try:
            if (
                (type(operation) is not str or operation == "")
                and (type(application) is not str or application == "")
                and (type(group) is not str or group == "")
                and (type(key) is not str or key == "")
                and (type(client_type) is not str or client_type == "")
            ):
                raise HTTPException(
                    status_code=400, detail="Bad Request(No valid input parameters)"
                )

            delete_list = self.schedule_queue.delete_with_client(
                client_operation,
                client_application,
                client_group,
                client_key,
                client_type,
            )

            found_count = len(delete_list)

            if found_count == 0:
                raise HTTPException(status_code=404, detail="Item not found")

        except Exception as ex:
            raise ex

        return {"count": found_count}

    def get_schedules_with_id(
        self,
        schedule_id: str,
    ):
        try:
            found_schedule = self.schedule_queue.get_with_id(schedule_id)

        except Exception as ex:
            print(f"Exception: {ex}")
            raise ex

        return found_schedule

    def get_schedules_with_client(
        self,
        client_operation: str,
        client_application: str,
        client_group: str,
        client_key: str,
        client_type: str,
    ):
        result_list = list()

        try:
            result_list = self.schedule_queue.get_with_client(
                client_operation,
                client_application,
                client_group,
                client_key,
                client_type,
            )

        except Exception as ex:
            print(f"Exception: {ex}")
            raise ex

        return result_list

    def get_groups(self):
        group_list = list()
        try:
            key_value_events = self.schedule_db.get_key_value_list(False)
            for _, value in key_value_events:
                client_info = value["client"]
                if not client_info["group"] in group_list:
                    group_list.append(client_info["group"])

        except Exception as ex:
            raise ex

        return group_list

    def retry_non_recur_event(self, schedule_event):
        task_info = schedule_event["task"]
        client_info = schedule_event["client"]
        schedule_unique_name = self.create_client_unique_name(client_info)
        base = datetime.now(timezone.utc)

        retry_count = task_info.get("retry_count", 0)
        max_retry_count = task_info.get("max_retry_count", -1)
        if (max_retry_count != -1) and (retry_count >= max_retry_count):
            log_info(
                f"Retry count({retry_count}) is over max_retry_count({max_retry_count})."
            )
            self.schedule_db.pop(schedule_unique_name)
            return

        retry_wait = schedule_event.get("retry_wait", 60)
        schedule_event["next"] = datetime.timestamp(base) + retry_wait

        # set the evetn to the localqueue
        self.schedule_db.put(schedule_unique_name, schedule_event)

        # run event handler
        self.run_event_handler(schedule_unique_name, retry_wait, schedule_event)
        # handle_event_future = asyncio.run_coroutine_threadsafe(
        #     self.handle_delay(retry_wait),
        #     asyncio.get_event_loop(),
        # )
        # self.running_schedules[schedule_unique_name] = handle_event_future

    def register_next(self, schedule_event):
        try:
            client_info = schedule_event["client"]
            tz = schedule_event.get("timezone", "Asia/Seoul")
            schedule_unique_name = self.create_client_unique_name(client_info)

            # 1. check if scheluer event type is recurrring..
            if ScheduleType.is_recurring(schedule_event["type"]):
                # 1.1. calculate the next timestamp and delay based on schedule_event
                (next_time, delay) = ScheduleType.get_next_and_delay(schedule_event, tz)

                new_schedule_task = schedule_event["task"]

                # 1.2. set the next timestamp
                new_schedule_task["next"] = next_time

                new_schedule_task["status"] = ScheduleTaskStatus.IDLE

                # 1.3. set the evetn to the localqueue
                self.schedule_db.put(schedule_unique_name, schedule_event)

                # 1.4. run handle_event
                self.run_event_handler(schedule_unique_name, delay, schedule_event)
                # handle_event_future = asyncio.run_coroutine_threadsafe(
                #     self.handle_delay(delay),
                #     asyncio.get_event_loop(),
                # )
                # self.running_schedules[schedule_unique_name] = handle_event_future
            # 2. check if scheluer event type is non-recurring
            else:
                task_info = schedule_event.get("task")
                # 2.1 check if the result of the previos task run is successful
                if task_info.get("status") == ScheduleTaskStatus.DONE:
                    # 2.1.1 pop this schedule event if it is not ono of recurrring schedule types.
                    self.schedule_db.pop(schedule_unique_name)
                    self.running_schedules.pop(schedule_unique_name, None)
                # 2.2 in case of failed...
                else:
                    # 2.2.1 run this task again though the type is non-recurring
                    self.retry_non_recur_event(schedule_event)

        except Exception as ex:
            raise ex

    def put_event_to_history_db(self, event: str, schedule_event: dict):
        try:
            task_info = schedule_event.get("task", {})
            client_info = schedule_event.get("client", {})
            if task_info:
                hischeck = task_info.get("history_check", False)
                db_connection_info = Config.history()
                if hischeck and db_connection_info:
                    (host, port, id, pw, db) = db_connection_info
                    initialize_global_database(id, pw, host, port, db)
                    with get_session() as db_session:
                        schevt_history = ScheduleEventHistory(
                            event=event,
                            name=client_info.get("name"),
                            key=client_info.get("key"),
                            group=client_info.get("group"),
                            type=schedule_event.get("type"),
                            schedule=schedule_event.get("schedule"),
                            task_type=task_info.get("type"),
                            task_connection=task_info.get("connection"),
                            task_data=task_info.get("data"),
                        )

                        db_session.add(schevt_history)
                        db_session.commit()
        except Exception as ex:
            log_error(f"can't save event to db - {ex}")

    def reset(self, admin_info: dict) -> dict:
        try:
            reset_result = dict()

            # TODO: should modify this authentication process by extenral modules like credentials,
            if (
                admin_info["user"] != "admin"
                and admin_info["password"] != "1qazxsw23edc"
            ):
                raise HTTPException(status_code=401, detail="Unauthorized")

            # reset the schedule queue
            count = 0
            key_value_list = self.schedule_db.get_key_value_list(False)
            for key, value in key_value_list:
                self.put_event_to_history_db("deleted", value)
                self.schedule_db.pop(key, False)
                future_event = self.running_schedules.pop(key, None)
                future_event.cancel() if future_event else None
                count += 1
            reset_result["schevt"] = count

            # reset the dead letter queue(dlq)
            key_list = self.schedule_db.get_key_list(True)
            count = 0
            for key in key_list:
                self.schedule_db.pop(key, True)
                count += 1
            reset_result["dlq"] = count
        except Exception as ex:
            raise ex

        return reset_result

    async def handle_event(self, key, schedule_event, delay):
        try:
            log_info(f'handle_event start: {schedule_event["name"]}')

            # 1. put it into the qeueue with the status 'waiting'
            task_info = schedule_event["task"]
            task_info["status"] = ScheduleTaskStatus.WAITING
            self.schedule_db.put(key, schedule_event)

            # 2. sleep with the input delay
            log_info(
                f'***{key} about to apply the delay({delay}, {datetime.fromtimestamp(task_info["next"])})'
            )
            await asyncio.sleep(delay)

            log_info(f'*** start this task({task_info["type"]}, {key})')

            # 3. run a task based on task parameters
            task_cls = TaskManager.get(task_info["type"])
            task = task_cls()

            task.connect(**task_info)
            res = await task.run(**schedule_event)
            log_debug(f"handle_event done: {key}, res: {str(res)}")

            # 4. set the lastRun
            task_info["lastRun"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

            # 5. update the status of this task
            history_db_status = ""
            if res:
                # 5.1 put it into the queue with status 'Done'
                task_info["status"] = ScheduleTaskStatus.DONE
                task_info["iteration"] += 1
                task_info["retry_count"] = 0
                self.schedule_db.put(key, schedule_event)
                history_db_status = "done"
            else:
                # 5.2 put it into the queue with status 'Failed'
                task_info["status"] = ScheduleTaskStatus.FAILED
                task_info["retry_count"] += 1
                self.schedule_db.put(key, schedule_event)
                history_db_status = "retry"
            self.put_event_to_history_db(history_db_status, schedule_event)

            # 6. put the next schedule
            self.register_next(schedule_event)

        except Exception as ex:
            log_error(f"Exception: {ex}")

    def run_event_handler(
        self, schedule_unique_name, delay, schedule_event=None
    ) -> None:
        # start a schedule event task
        handle_event_future = asyncio.run_coroutine_threadsafe(
            self.handle_event(schedule_unique_name, schedule_event, delay),
            asyncio.get_event_loop(),
        )

        # add the name of the current schedlue event to event_name list.
        # to check the duplicated schedule name later
        self.running_schedules[schedule_unique_name] = handle_event_future
        return handle_event_future

    async def handle_delay(self, delay):
        try:
            log_info(f"handle_event start: after {delay}")

            await asyncio.sleep(delay)

            # 1. get the schedule with the timestamp before now
            now = datetime.now().timestamp()
            key_value_list = self.schedule_db.get_key_value_list(False)
            print("**** key_value_list: ", len(key_value_list))

            for key, schedule_event in self.schedule_db.get_key_value_list(False):
                task_info = schedule_event["task"]
                print(f"*#* ({key}) entry: ", task_info["status"])
                if (
                    task_info["next"] <= now
                    and task_info["status"] != ScheduleTaskStatus.PROCESSING
                ):
                    try:
                        # ...
                        task_info["status"] = ScheduleTaskStatus.PROCESSING
                        self.schedule_db.put(key, schedule_event)

                        # 3. run a task based on task parameters
                        task_cls = TaskManager.get(task_info["type"])
                        task = task_cls()

                        task.connect(**task_info)
                        print(f"task run: {task_info}")
                        res = await task.run(**schedule_event)
                        log_debug(f"handle_event done: {key}, res: {str(res)}")
                        # 4. set the lastRun
                        task_info["lastRun"] = datetime.now().strftime(
                            "%Y-%m-%dT%H:%M:%S"
                        )

                        # 5. update the status of this task
                        history_db_status = ""
                        if res:
                            # 5.1 put it into the queue with status 'Done'
                            task_info["status"] = ScheduleTaskStatus.DONE
                            task_info["iteration"] += 1
                            task_info["retry_count"] = 0
                            self.schedule_db.put(key, schedule_event)
                            history_db_status = "done"
                        else:
                            # 5.2 put it into the queue with status 'Failed'
                            task_info["status"] = ScheduleTaskStatus.FAILED
                            task_info["retry_count"] += 1
                            self.schedule_db.put(key, schedule_event)
                            history_db_status = "retry"

                        self.put_event_to_history_db(history_db_status, schedule_event)

                        # 6. put the next schedule
                        self.register_next(schedule_event)
                    except Exception as ex:
                        log_error(f"Exception: {ex}")

        except Exception as ex:
            log_error(f"Exception: {ex}")
