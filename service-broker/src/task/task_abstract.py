from abc import *


class Task(metaclass=ABCMeta):
    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def connect(self, **kargs):
        pass

    @abstractmethod
    async def run(self, **kargs) -> bool:
        pass
