from task.task_kafka import TaskKafka
from task.task_redis import TaskRedis
from task.task_rest import TaskRest
from task.task_slack import TaskSlack

TASK_ACTIVE_MODULE_LIST = [TaskKafka, TaskRedis, TaskRest, TaskSlack]
