import asyncio
import uuid
import json
from datetime import datetime, timezone
from fastapi import HTTPException
import threading
from datetime import datetime

from restful.rest_type import Schedule
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
    def do_periodic_process(cls, async_process_loop):
        print(f"called do_periodic_process: {str(datetime.now())}")

        # 1. pop the schedule with the lowest next_schedule value
        scheduler = cls()
        schedules = scheduler.schedule_queue.pop()
        print(f"schedules: {schedules}")

        if len(schedules) > 0:
            scheduler.run_process_schedules(schedules, async_process_loop)

        # TODO: 0.5(500ms) is a part of this application configuration.
        if not scheduler.finalized:
            threading.Timer(
                0.5, Scheduler.do_periodic_process, args=(async_process_loop,)
            ).start()

    def start_schedule(self):
        Scheduler.do_periodic_process(asyncio.get_event_loop())

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
            (id, _, _, _) = self.schedule_queue.get_with_name(
                schedule_unique_name, True
            )
            # 1.1 if duplicated, delete the previous one and register new one with the same id
            if id:
                self.schedule_queue.delete_with_id(id)
                schedule_id = id

            # 1.2 if not duplicated, create a new id
            else:
                schedule_id = str(uuid.uuid4())

            # 2. calculate next timestamp and delay based on schedule_event
            tz = schedule_event.get("timezone", "Asia/Seoul")
            (next_time, _) = ScheduleType.get_next_and_delay(schedule_event, tz)

            # 3. arrnage the task parameters of this schedule
            new_schedule_task = schedule_event["task"]
            # 3.1 check if task type is available
            if not new_schedule_task["type"] in TaskManager.all():
                raise Exception("task type unavailable.")
            # 3.2 check if task connection is available
            input_task_connection = new_schedule_task["connection"]
            self.is_task_available(new_schedule_task["type"], input_task_connection)
            # 3.3 update some additional parameters of this task like status, next, etc.
            new_schedule_task["retry_count"] = 0
            new_schedule_task["next"] = next_time
            new_schedule_task["status"] = ScheduleTaskStatus.IDLE
            new_schedule_task["iteration"] = 0

            # 5. store the updated schedule event
            self.schedule_queue.put(
                schedule_id, schedule_unique_name, next_time, schedule_event
            )
            # self.put_event_to_history_db("register", schedule_event)

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

    def unregister(self, schedule_id: str):
        try:
            if type(schedule_id) is not str or schedule_id == "":
                raise Exception(f"schedule_id is not available.. {schedule_id}")

            (_, schedule) = self.schedule_queue.delete_with_id(schedule_id)
            # if schedule:
            #     self.put_event_to_history_db("unregister", schedule)

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

            deleted_list = self.schedule_queue.delete_with_id(schedule_id)

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
                (type(client_operation) is not str or client_operation == "")
                and (type(client_application) is not str or client_application == "")
                and (type(client_group) is not str or client_group == "")
                and (type(client_key) is not str or client_key == "")
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

            if len(found_schedule) == 0:
                raise HTTPException(status_code=404, detail="Item not found")

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
        found_schedules = list()

        try:
            found_schedules = self.schedule_queue.get_with_client(
                client_operation,
                client_application,
                client_group,
                client_key,
                client_type,
            )

            if len(found_schedules) == 0:
                raise HTTPException(status_code=404, detail="Item not found")

        except Exception as ex:
            print(f"Exception: {ex}")
            raise ex

        return found_schedules

    def process_retry_non_recur_event(self, schedule_id, schedule_name, schedule):
        task_info = schedule["task"]
        base = datetime.now(timezone.utc)

        # 1. check if the current retry count is over the max retry count
        retry_count = task_info.get("retry_count", 0)
        max_retry_count = task_info.get("max_retry_count", -1)
        if (max_retry_count != -1) and (retry_count >= max_retry_count):
            log_info(
                f"Retry count({retry_count}) is over max_retry_count({max_retry_count})."
            )
            return

        # 2. calculate the next timestamp and delay based on schedule
        retry_wait = schedule.get("retry_wait", 60)
        schedule["next"] = datetime.timestamp(base) + retry_wait

        # 3. put this schedule to queue
        self.schedule_queue.put(schedule_id, schedule_name, schedule["next"], schedule)

    def run_process_schedules(self, schedules: list, async_process_loop):
        # start a schedule event task
        handle_event_future = asyncio.run_coroutine_threadsafe(
            self.process_schedules(schedules),
            async_process_loop,
        )

        # add the name of the current schedlue event to event_name list.
        # to check the duplicated schedule name later
        self.running_schedules[id] = handle_event_future
        return handle_event_future

    async def process_schedules(self, schedules: list):
        try:
            for schedule in schedules:
                id = schedule["id"]
                name = schedule["name"]
                task_info = schedule["task"]

                try:
                    # set the status of this task to 'processing'
                    task_info["status"] = ScheduleTaskStatus.PROCESSING

                    # 3. run a task based on task parameters
                    task_cls = TaskManager.get(task_info["type"])
                    task = task_cls()

                    task.connect(**task_info)
                    print(f"task run: {task_info}")
                    res = await task.run(**schedule)
                    log_debug(f"handle_event done: {name}, res: {str(res)}")

                    client_info = schedule["client"]
                    schedule_name = self.create_client_unique_name(client_info)

                    # 4. put the next schedule
                    self.postprocess_schedule(res, id, schedule_name, schedule)
                except Exception as ex:
                    log_error(f"Exception: {ex}")

        except Exception as ex:
            pass

    def postprocess_schedule(
        self,
        process_result: bool,
        schedule_id: str,
        schedule_name: str,
        schedule: dict,
    ):
        """
        스케줄 내에 태스크 정보를 기반으로 태스크가 정상적으로 실행되거나 혹은 종료된 후,
        스케줄의 상태를 업데이트하고, 다음 스케줄을 스케줄 큐에 등록한다.

        """
        try:
            task_info = schedule["task"]
            # 1. update the status of the task in this schedule
            if process_result:
                # 1.1 if the result of task run is successful, update the status of this task
                task_info["status"] = ScheduleTaskStatus.DONE
                task_info["iteration"] += 1
                task_info["retry_count"] = 0
            else:
                # 1.1 if the result of task run is successful, update the status of this task
                task_info["status"] = ScheduleTaskStatus.FAILED
                task_info["retry_count"] += 1

            # 2. get the schedule timezone
            tz = schedule.get("timezone", "Asia/Seoul")

            """
            기본적으로 일정한 주기를 가지는 스케줄이 태스크 동작을 실패할 경우,
            동일한 태스크가 다음 주기에 수행되도록 스케줄링된다.
            해당 태스크는 최대 재시도 횟수(max_retry_count) 만큼 수행되고 실패할 경우, 
            더 이상 스케줄링에 포함되지 않는다.

            주기를 별도로 가지고 있지 않는 1회성 스케줄의 경우,
            기본 설정된 최대 재시도 횟수(max_retry_count) 외에 재시작 대기 시간(retry_wait, 기본값: 60초)을 가지고 있어,
            실패한 시점부터 재시작 대기 시간(retry_wait) 이후에 다시 수행된다.
            최대 재시도 횟수(max_retry_count) 만큼 수행되고 실패할 경우, 
            더 이상 스케줄링에 포함되지 않는다.

            """
            # 3. check if schedule event type is recurring
            if ScheduleType.is_recurring(schedule["type"]):
                # 3.1. calculate the next timestamp and delay based on schedule
                (next_time, delay) = ScheduleType.get_next_and_delay(schedule, tz)
                new_schedule_task = schedule["task"]
                new_schedule_task["next"] = next_time

                # 3.2 update the status of this schedule task
                new_schedule_task["status"] = ScheduleTaskStatus.IDLE

                # 3.3. set the evetn to the localqueue
                self.schedule_queue.put(schedule_id, schedule_name, next_time, schedule)
            # 4. check if scheluer event type is non-recurring
            else:
                # 4.1 check if the result of the previous task run is successful
                task_info = schedule.get("task")
                if task_info.get("status") == ScheduleTaskStatus.DONE:
                    pass
                # 4.2 in case of failed...
                else:
                    # 4.2.1 run this task again though the type is non-recurring
                    self.process_retry_non_recur_event(
                        schedule_id, schedule_name, schedule
                    )

        except Exception as ex:
            raise ex

    def put_event_to_history_db(self, event: str, schedule_event: dict):
        """
        변경된 스케줄큐 구조에서 이력을 별도의 테이블로 관리하는 것에 대한 고려가 필요함.
        별도의 테이블 없이 스케줄큐 테이블을 통해서 별도의 스케줄 처리에 대한 이력(스케줄 시작 여부, 상태 등)을
        관리할 수 있다면, 구조를 단순화하는 차원에서 하나의 테이블로 관리하는 것이 좋을 것 같음.

        """
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
