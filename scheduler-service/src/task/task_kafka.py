import json
from aiokafka import AIOKafkaProducer

from task.task_mgr import TaskManager
from task.task_abstract import Task

import sys
from helper.logger import Logger

log_message = Logger.get("tkafka", Logger.Level.INFO, sys.stdout)
log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error


class TaskKafka(Task):
    def __init__(self):
        pass

    def get_name(self):
        return "kafka"

    def connect(self, **kargs):
        pass

    async def run(self, **kargs) -> bool:
        try:
            task_info = kargs["task"]
            connection = task_info["connection"]
            topic = connection["topic"]
            produce_data = (
                json.dumps(task_info["data"])
                if type(task_info["data"]) is dict
                else str(task_info["data"])
            ).encode()

            log_debug(f"connection: {connection}")
            producer = AIOKafkaProducer(bootstrap_servers=connection["host"])
            # Get cluster layout and initial topic/partition leadership information
            await producer.start()
            try:
                log_debug(f"topic: {topic}, data: {produce_data}")
                # Produce message
                await producer.send_and_wait(topic, produce_data)
            finally:
                # Wait for all pending messages to be delivered or expire.
                await producer.stop()

        except Exception as ex:
            log_error("Exception: ", ex)
        finally:
            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            await producer.stop()

        return True


TaskManager.register("kafka", TaskKafka)
