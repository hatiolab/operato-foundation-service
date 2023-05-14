from task.task_abstract import Task


class TaskManager:
    REGISTRED_TASK_INFO = dict()

    @staticmethod
    def register(type: str, backend_cls: Task):
        TaskManager.REGISTRED_TASK_INFO[type] = backend_cls

    @staticmethod
    def get(type: str):
        task = None
        try:
            task = TaskManager.REGISTRED_TASK_INFO[type]
        except KeyError as keyerr:
            print(f"not support this type: {keyerr}")

        return task

    @staticmethod
    def get_module_list():
        return TaskManager.TASK_MODULE_LIST

    @staticmethod
    def all():
        return TaskManager.REGISTRED_TASK_INFO
