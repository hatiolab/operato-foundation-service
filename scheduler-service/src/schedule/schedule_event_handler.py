import os
import asyncio
import uuid
import json
from datetime import datetime, timezone
from fastapi import HTTPException

from api.api_data_type import Schedule
from config import Config
from direct_queue.local_queue import LocalQueue
from direct_queue.pg_queue import PGQueue
from schedule.schedule_event_type import ScheduleType, ScheduleTaskStatus
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


class ScheduleEventHandler:
    TASK_RETRY_MAX = 3
    DEFAULT_LOCAL_POD_NAME = "local-scheduler"

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
                    self.schedule_db = LocalQueue(queue_info)
                elif queue_type == "pg":
                    self.schedule_db = PGQueue(queue_info)

                self.running_schedules = dict()
                self.instance_name = ScheduleEventHandler.DEFAULT_LOCAL_POD_NAME
                cls._init = True
            except Exception as ex:
                raise ex

    def initialize(self):
        try:
            # get the current instance name
            self.instance_name = os.environ.get(
                "POD_NAME", ScheduleEventHandler.DEFAULT_LOCAL_POD_NAME
            )
            if self.instance_name == ScheduleEventHandler.DEFAULT_LOCAL_POD_NAME:
                log_warning(
                    "cannot find environment variable POD_NAME, so assume that th only one instance is running."
                )

            # LOCALQUEUE: find a schedule with the specific "instance" key
            key_value_events = self.schedule_db.get_key_value_list()
            for key, value in key_value_events:
                assert (type(key) is str) and (type(value) is dict)
                schedule_event = value
                if schedule_event["instance"] == self.instance_name:
                    log_info(f'registering {schedule_event["name"]}')
                    schedule = Schedule(**schedule_event)
                    self.register(schedule, application_init=True)

            log_info("initialization done..")
        except Exception as ex:
            log_error(f"Initializaiton Error: {ex}")

    def is_connection_available(self, type: str, connection_info: dict) -> bool:
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

    def get_localdb_key(self, client_info: dict):
        return ",".join(
            [
                client_info.get("application", ""),
                client_info.get("group", ""),
                client_info.get("type", ""),
                client_info.get("key", ""),
                client_info.get("operation", ""),
            ]
        )

    def register(self, schedule: Schedule, application_init: bool = False):
        try:
            schedule_event = schedule.dict()

            client_info = schedule_event.get("client")
            local_queue_key = self.get_localdb_key(client_info)

            # 1. check if unique key(operation) is duplicated.
            if local_queue_key in self.running_schedules:
                schedule_event = self.handle_duplicated_event(schedule_event)

            # 1.1. set the insance name
            schedule_event["instance"] = self.instance_name

            # 2. store this schedule_event to timastamp db with response id
            if schedule_event.get("id", "") == "":
                resp_id = uuid.uuid4()
                schedule_event["id"] = str(resp_id)

            # 3. get the next timestamp and the delay based on schedule format
            tz = schedule_event.get("timezone", "Asia/Seoul")
            (next_time, delay) = ScheduleType.get_next_and_delay(schedule_event, tz)

            # TODO: add some additional key-values (status, ...)
            input_data_task = schedule_event["task"]
            # 4.1 check if task type is available
            if not input_data_task["type"] in TaskManager.all():
                raise Exception("task type unavailable.")
            # 4.2 check if task connection is available
            input_task_connection = input_data_task["connection"]
            self.is_connection_available(input_data_task["type"], input_task_connection)
            # 4.3 update some additional data like next and retry
            input_data_task["retry_count"] = 0
            input_data_task["next"] = next_time
            input_data_task["status"] = ScheduleTaskStatus.IDLE
            input_data_task["iteration"] = 0
            input_data_task["lastRun"] = None

            # 5. store the updated schedule event
            self.schedule_db.put(local_queue_key, schedule_event)
            self.save_schevt_to_db("register", schedule_event)

            # 6. start a schedule event task
            self.run_event_handler(local_queue_key, delay, schedule_event)
            # handle_event_future = asyncio.run_coroutine_threadsafe(
            #     self.handle_delay(delay),
            #     asyncio.get_event_loop(),
            # )

            # # 7. add the name of the current schedlue event to event_name list.
            # #  to check the duplicated schedule name later
            # self.running_schedules[local_queue_key] = handle_event_future

            # 8. return the result with the response id
            return {
                "name": schedule_event["name"],
                "client": {
                    "application": client_info["application"],
                    "group": client_info["group"],
                    "type": client_info["type"],
                    "key": client_info["key"],
                    "operation": client_info["operation"],
                },
                "id": str(schedule_event["id"]),
            }
        except Exception as ex:
            print(f"Exception: {ex}")
            raise ex

    def update_schedule_with_other(self, src_schedule: dict, dst_schedule: dict):
        dst_schedule["name"] = src_schedule["name"]
        dst_schedule["client"] = src_schedule["client"]
        dst_schedule["type"] = src_schedule["type"]
        dst_schedule["schedule"] = src_schedule["schedule"]
        dst_schedule["timezone"] = src_schedule["timezone"]
        dst_schedule["task"] = src_schedule["task"]

    def handle_duplicated_event(self, schedule_event: dict):
        log_info(f"started to handle the duplicated event: {schedule_event}")
        # 0. get an unique key
        client_info = schedule_event["client"]
        local_key = self.get_localdb_key(client_info)

        # 1. delete the previous event and cancel the current event
        future_event = self.running_schedules.pop(local_key, None)
        future_event.cancel() if future_event else None

        existed_schedule = self.schedule_db.pop(local_key)
        existed_schedule = json.loads(existed_schedule.decode("utf-8"))

        self.update_schedule_with_other(schedule_event, existed_schedule)

        # 2. update the record for the current schedule event
        self.save_schevt_to_db("changed", existed_schedule)

        return existed_schedule

    def unregister(self, input_resp_id: str):
        try:
            if type(input_resp_id) is not str or input_resp_id == "":
                raise Exception(f"input_resp_id is not available.. {input_resp_id}")

            # 1. get all key-value data in the localqueue and find the specified name using for-iteration
            # LOCALQUEUE: search with filter "ID"
            key_value_events = self.schedule_db.get_key_value_list()
            log_debug(f"*** get_key_value_list: {key_value_events}")

            found_count = 0
            if len(key_value_events):
                for key, registered_event in key_value_events:
                    # 2. pop the event from localqueue if found
                    resp_id = registered_event["id"]

                    if resp_id == input_resp_id:
                        self.schedule_db.pop(key)
                        self.save_schevt_to_db("unregister", registered_event)
                        # 3. remove it from running_schedules and cancel it
                        future_event = self.running_schedules.pop(key, None)
                        future_event.cancel()
                        found_count += 1

                        log_info(f"handle_event unregistered: {key}({resp_id})")

        except Exception as ex:
            raise ex

        return {"count": found_count}

    def delete_schedules(
        self,
        resp_id: str,
        operation: str,
        application: str,
        group: str,
        key: str,
        client_type: str,
        dlq: bool = False,
    ):
        try:
            if (
                (type(resp_id) is not str or resp_id == "")
                and (type(operation) is not str or operation == "")
                and (type(application) is not str or application == "")
                and (type(group) is not str or group == "")
                and (type(key) is not str or key == "")
                and (type(client_type) is not str or client_type == "")
            ):
                raise HTTPException(
                    status_code=400, detail="Bad Request(No valid input parameters)"
                )

            # 1. get all key-value data in the localqueue and find the specified name using for-iteration
            # LOCALQUEUE: search a schedule with "ID" and "client" elementes
            key_value_events = self.schedule_db.get_key_value_list()
            log_debug(f"*** get_key_value_list: {key_value_events}")

            found_count = 0
            if len(key_value_events):
                for key, registered_event in key_value_events:
                    # 2. pop the event from localqueue if found
                    fetched_resp_id = registered_event["id"]
                    client_info = registered_event["client"]

                    if (
                        (resp_id and (fetched_resp_id == resp_id))
                        or (group and (group == client_info["group"]))
                        or (application and (application == client_info["application"]))
                        or (operation and (operation == client_info["operation"]))
                        or (key and (key == client_info["key"]))
                        or (client_type and (client_type == client_info["type"]))
                    ):
                        self.schedule_db.pop(key)
                        self.save_schevt_to_db("unregister", registered_event)
                        # 3. remove it from running_schedules and cancel it
                        future_event = self.running_schedules.pop(key, None)
                        if future_event:
                            future_event.cancel()
                        found_count += 1

                        log_info(f"handle_event unregistered: {key}({fetched_resp_id})")

            if found_count == 0:
                raise HTTPException(status_code=404, detail="Item not found")

        except Exception as ex:
            raise ex

        return {"count": found_count}

    def get_schedules(
        self,
        resp_id: str,
        operation: str,
        application: str,
        group: str,
        key: str,
        client_type: str,
        dlq: bool = False,
    ):
        list_item = list()

        try:
            # 2. gather all key-value data from the localqueue
            key_value_events = self.schedule_db.get_key_value_list(dlq)
            list_item = [
                schedule
                for _, schedule in key_value_events
                if (not application or application == schedule["client"]["application"])
                and (not group or group == schedule["client"]["group"])
                and (not operation or operation == schedule["client"]["operation"])
                and (not key or key == schedule["client"]["key"])
                and (not resp_id or resp_id == schedule["id"])
                and (not client_type or client_type == schedule["client"]["type"])
            ]
        except Exception as ex:
            print(f"Exception: {ex}")
            raise ex

        return list_item

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
        local_queue_key = self.get_localdb_key(client_info)
        base = datetime.now(timezone.utc)

        retry_count = task_info.get("retry_count", 0)
        max_retry_count = task_info.get("max_retry_count", -1)
        if (max_retry_count != -1) and (retry_count >= max_retry_count):
            log_info(
                f"Retry count({retry_count}) is over max_retry_count({max_retry_count})."
            )
            self.schedule_db.pop(local_queue_key)
            return

        retry_wait = schedule_event.get("retry_wait", 60)
        schedule_event["next"] = datetime.timestamp(base) + retry_wait

        # set the evetn to the localqueue
        self.schedule_db.put(local_queue_key, schedule_event)

        # run event handler
        self.run_event_handler(local_queue_key, retry_wait, schedule_event)
        # handle_event_future = asyncio.run_coroutine_threadsafe(
        #     self.handle_delay(retry_wait),
        #     asyncio.get_event_loop(),
        # )
        # self.running_schedules[local_queue_key] = handle_event_future

    def register_next(self, schedule_event):
        try:
            client_info = schedule_event["client"]
            tz = schedule_event.get("timezone", "Asia/Seoul")
            local_queue_key = self.get_localdb_key(client_info)

            # 1. check if scheluer event type is recurrring..
            if ScheduleType.is_recurring(schedule_event["type"]):
                # 1.1. calculate the next timestamp and delay based on schedule_event
                (next_time, delay) = ScheduleType.get_next_and_delay(schedule_event, tz)

                input_data_task = schedule_event["task"]

                # 1.2. set the next timestamp
                input_data_task["next"] = next_time

                input_data_task["status"] = ScheduleTaskStatus.IDLE

                # 1.3. set the evetn to the localqueue
                self.schedule_db.put(local_queue_key, schedule_event)

                # 1.4. run handle_event
                self.run_event_handler(local_queue_key, delay, schedule_event)
                # handle_event_future = asyncio.run_coroutine_threadsafe(
                #     self.handle_delay(delay),
                #     asyncio.get_event_loop(),
                # )
                # self.running_schedules[local_queue_key] = handle_event_future
            # 2. check if scheluer event type is non-recurring
            else:
                task_info = schedule_event.get("task")
                # 2.1 check if the result of the previos task run is successful
                if task_info.get("status") == ScheduleTaskStatus.DONE:
                    # 2.1.1 pop this schedule event if it is not ono of recurrring schedule types.
                    self.schedule_db.pop(local_queue_key)
                    self.running_schedules.pop(local_queue_key, None)
                # 2.2 in case of failed...
                else:
                    # 2.2.1 run this task again though the type is non-recurring
                    self.retry_non_recur_event(schedule_event)

        except Exception as ex:
            raise ex

    def save_schevt_to_db(self, event: str, schedule_event: dict):
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
                self.save_schevt_to_db("deleted", value)
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
            self.save_schevt_to_db(history_db_status, schedule_event)

            # 6. put the next schedule
            self.register_next(schedule_event)

        except Exception as ex:
            log_error(f"Exception: {ex}")

    def run_event_handler(self, local_queue_key, delay, schedule_event=None) -> None:
        # start a schedule event task
        handle_event_future = asyncio.run_coroutine_threadsafe(
            self.handle_event(local_queue_key, schedule_event, delay),
            asyncio.get_event_loop(),
        )

        # add the name of the current schedlue event to event_name list.
        # to check the duplicated schedule name later
        self.running_schedules[local_queue_key] = handle_event_future
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

                        self.save_schevt_to_db(history_db_status, schedule_event)

                        # 6. put the next schedule
                        self.register_next(schedule_event)
                    except Exception as ex:
                        log_error(f"Exception: {ex}")

        except Exception as ex:
            log_error(f"Exception: {ex}")
