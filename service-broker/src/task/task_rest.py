import jwt
import json
import sys
import httpx

# from task.task_mgr import TaskManager
from task.task_abstract import Task

from helper.logger import Logger

log_message = Logger.get("tkrest", Logger.Level.INFO, sys.stdout)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


class TaskRest(Task):
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=None,
            limits=httpx.Limits(max_keepalive_connections=None, keepalive_expiry=None),
        )
        self.host = ""
        self.headers = dict()
        self.data = dict()

    def get_name(self):
        return "rest"

    def connect(self, **callback_info):
        try:
            connection_info = callback_info["connection"]
            self.host = connection_info["host"]
            self.headers = connection_info.get("headers", {})
            self.data = callback_info.get("data", {})
        except Exception as ex:
            log_error(f"Exception: {ex}")
            raise ex

    async def run(self, **callback_info) -> bool:
        result = False
        try:
            # like {"Accept": "application/json", "Content-Type": "application/json"}
            client_info = callback_info["client"]

            # add some the specific key-value in headers
            headers = self.headers or {}
            # headers["clientKey"] = client_info["key"]

            # TODO: ...
            # headers["token"] = jwt.encode(callback_info, callback_info["resp_id"], algorithm="HS256")

            data = json.dumps(callback_info)
            res = await self.client.post(
                self.host,
                headers=headers,
                data=data,
                timeout=httpx.Timeout(timeout=None),
            )
            log_debug(f"result: {res.status_code}")
            result = True if res.status_code == 200 else False

        except Exception as ex:
            log_error(
                f"Exception: {ex}",
            )

        return result


# TaskManager.register("rest", TaskRest)
