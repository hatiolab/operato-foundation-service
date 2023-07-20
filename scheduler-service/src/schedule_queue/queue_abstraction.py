from abc import *


class ScheduleQueue(metaclass=ABCMeta):
    @abstractmethod
    def put(self, tstamp, tdata, dlq=False):
        pass

    @abstractmethod
    def pop(self, key, dlq=False):
        pass
