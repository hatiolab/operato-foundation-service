import yaml
import os


class Config:
    CONFIG_DATA: dict = {}

    def __init__(self, config_yaml_file=None, config_data=None):
        Config.CONFIG_DATA: dict = {}
        try:
            if config_yaml_file:
                if not os.path.isfile(config_yaml_file):
                    config_yaml_file = "../config/config.yaml"

                with open(config_yaml_file) as config_file:
                    Config.CONFIG_DATA = yaml.load(config_file, Loader=yaml.FullLoader)
            elif config_data:
                Config.CONFIG_DATA = config_data
            else:
                raise Exception("No configuration data")

        except Exception as ex:
            Config.CONFIG_DATA = {}
            raise Exception("No configuration data found")

    @classmethod
    def get(cls, key):
        return cls.CONFIG_DATA.get(key, None)

    @classmethod
    def database(cls):
        try:
            config_data = cls.CONFIG_DATA.get("database", None)
            if not config_data:
                return None
            assert (
                config_data["host"]
                and config_data["port"]
                and config_data["id"]
                and config_data["pw"]
                and config_data["db"]
            )
        except Exception as ex:
            print(f"invalid configuration(pg): \n{ex}")
            config_data = None

        return config_data

    @classmethod
    def history_table(cls):
        try:
            assert cls.CONFIG_DATA.get("history")
            history_table = cls.CONFIG_DATA.get("history").get(
                "table", "schedule_history"
            )
        except Exception as ex:
            print(f"invalid configuration(history): \n{ex}")
            print("using default table name(schedule_history)")
            history_table = "schedule_history"
        return history_table

    @classmethod
    def locking_table(cls):
        try:
            assert cls.CONFIG_DATA.get("locking")
            locking_table = cls.CONFIG_DATA.get("locking").get("table", "locking_queue")
        except Exception as ex:
            print(f"invalid configuration(locking): \n{ex}")
            print("using default table name(schedule_queue)")
            locking_table = "locking_queue"
        return locking_table

    @classmethod
    def locking_info(cls):
        try:
            assert cls.CONFIG_DATA.get("locking")
            locking = cls.CONFIG_DATA.get("locking")
        except Exception as ex:
            print(f"invalid configuration(locking): \n{ex}")
            print("using default table name(schedule_queue)")
            locking = {}
        return locking

    @classmethod
    def locking_debug(cls) -> bool:
        try:
            assert cls.CONFIG_DATA.get("locking")
            debug_enable = cls.CONFIG_DATA.get("locking").get("debug", False)
        except Exception as ex:
            print(f"invalid configuration(locking): \n{ex}")
            print("using default flag(False)")
            debug_enable = False
        return debug_enable


if __name__ == "__main__":
    conf = Config("/Users/jinwonchoi/github/ms2postgres/config/config.yaml")
    print(Config.get("mssql"))
