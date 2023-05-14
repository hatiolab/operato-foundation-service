import sys
import logging


class LoggerObject:
    def __init__(self, name, level=logging.INFO, stream=sys.stdout):
        # create logger
        self.logger = logging.getLogger(name)

        # set level
        self.level = level
        self.logger.setLevel(level)

        # create console handler with level
        self.console_handler = logging.StreamHandler()
        self.console_handler.setLevel(level)

        # create formatter
        self.formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.console_handler.setFormatter(self.formatter)

        # add console handler to logger
        self.logger.addHandler(self.console_handler)

    @property
    def level(self):
        return self.__real_attr

    @level.setter
    def level(self, level):
        self.__real_attr = level
        self.logger.setLevel(level)

    def debug(self, message):
        return self.logger.debug(message)

    def info(self, message):
        return self.logger.info(message)

    def warning(self, message):
        return self.logger.warning(message)

    def error(self, message):
        return self.logger.error(message, exc_info=True)

    def critical(self, message):
        return self.logger.critical(message)


class Logger:
    LOGGER_DICT = dict()

    class Level:
        DEBUG = logging.DEBUG
        INFO = logging.INFO
        WARNING = logging.WARNING
        ERROR = logging.ERROR
        CRITICAL = logging.CRITICAL

    @staticmethod
    def get(name, level=logging.INFO, stream=sys.stdout):
        try:
            logger = Logger.LOGGER_DICT[name]
        except KeyError:
            logger = LoggerObject(name, level, stream)
            Logger.LOGGER_DICT[name] = logger

        return logger


if __name__ == "__main__":
    cl = LoggerObject("classifier")

    cl.debug("test")
    cl.info("test")
    cl.warning("test")
    cl.error("test")
    cl.critical("test")

    dl = LoggerObject("detector", logging.ERROR)
    dl.debug("test")
    dl.info("test")
    dl.warning("test")
    dl.error("test")
    dl.critical("test")

    edgeDebug = Logger.get("edge").debug
    edgeDebug("hi")

    edgeDebug = Logger.get("edge").debug
    edgeDebug("hi twice")
