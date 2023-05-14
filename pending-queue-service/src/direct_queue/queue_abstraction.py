from abc import *


class Queue(metaclass=ABCMeta):
    @abstractmethod
    def put(self, tstamp, tdata, dlq=False):
        pass

    @abstractmethod
    def pop(self, key, dlq=False):
        pass

    @abstractmethod
    def get_key_list(self, dlq=False) -> list:
        pass

    @abstractmethod
    def get_key_value_list(self, dlq=False) -> list:
        pass
