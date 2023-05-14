import json
import httpx

from task.task_abstract import Task
from task.task_mgr import TaskManager


class TaskSlack(Task):
    def __init__(self):
        self.client = httpx.AsyncClient()

    def get_name(self):
        return "slack"

    def connect(self, **kargs):
        pass

    async def run(self, **kargs) -> bool:
        try:
            slack_url = kargs["url"]
            slack_msg = dict()
            slack_msg["channel"] = kargs["channel"]
            slack_msg["username"] = "ENS Backend"
            slack_msg["text"] = kargs["text"]
            slack_msg["icon_emoji"] = ":heavy-check-mark:"

            headers = {
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            }
            res = await self.client.post(
                slack_url, headers=headers, data=json.dumps(self.slack_msg)
            )
        except Exception as ex:
            print("Exception: ", ex)
        finally:
            pass

        return res


TaskManager.register("slack", TaskSlack)
