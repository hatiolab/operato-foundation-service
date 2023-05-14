import yaml

import sys

from helper.logger import Logger

log_message = Logger.get("config", Logger.Level.INFO, sys.stdout)

log_debug = log_message.debug
log_info = log_message.info
log_warning = log_message.warning
log_error = log_message.error

from enum import Enum


class ConfigDBType(Enum):
    MSSQL = "mssql"
    POSTGRES = "postgres"


class AppConfig:
    CONFIG_DATA: dict = {}

    def __init__(self, config_yaml_file=None, config_data=None):
        AppConfig.CONFIG_DATA: dict = {}
        try:
            if config_yaml_file:
                with open(config_yaml_file) as config_file:
                    AppConfig.CONFIG_DATA = yaml.load(
                        config_file, Loader=yaml.FullLoader
                    )
            elif config_data:
                AppConfig.CONFIG_DATA = config_data
            else:
                raise Exception("No configuration data")

        except Exception as ex:
            AppConfig.CONFIG_DATA = {}
            raise Exception("No configuration data found")

    @classmethod
    def get(cls, key):
        return cls.CONFIG_DATA.get(key, None)

    @classmethod
    def db_info(cls, db_type: ConfigDBType):
        try:
            config_data = cls.CONFIG_DATA.get("db-connection").get(db_type.value, None)
        except Exception as ex:
            log_error(f"invalid configuration: \n{ex}")
            config_data = None

        return (
            (
                config_data["host"],
                config_data["port"],
                config_data["id"],
                config_data["pw"],
                config_data["db"],
            )
            if config_data != None
            else None
        )


if __name__ == "__main__":
    conf = AppConfig("/Users/jinwonchoi/github/ms2postgres/config/config.yaml")
    print(AppConfig.get("mssql"))
