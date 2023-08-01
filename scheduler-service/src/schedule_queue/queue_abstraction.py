from abc import *


class ScheduleQueue(metaclass=ABCMeta):
    @abstractmethod
    def put(self, id, name, next_schedule, payload):
        pass

    @abstractmethod
    def pop(self):
        pass
