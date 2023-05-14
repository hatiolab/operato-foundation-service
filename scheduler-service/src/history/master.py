import psycopg2 as pg2

"""
TODO: 
    - enhace the result handling of all funcitons
"""


class MasterData:
    MASTER_TABLE_NAME = "schedule_master"
    SQL_GET_ALL = f"SELECT * FROM {MASTER_TABLE_NAME};"
    SQL_CREATE_TABLE = f"CREATE TABLE {MASTER_TABLE_NAME} (id SERIAL PRIMARY KEY, name VARCHAR(128), type VARCHAR(16), schedule VARCHAR(256), data text);"
    SQL_CHECK_TABLE_EXISTS = f"SELECT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME={MASTER_TABLE_NAME}"
    SQL_INSERT_RECORD = f"INSERT INTO {MASTER_TABLE_NAME} (NAME, TYPE, SCHEDULE, DATA)"
    SQL_CHECK_TABLE = f"SELECT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME={MASTER_TABLE_NAME})"

    def __init__(self, conn_info: dict):
        self._initialized = False

        assert (
            conn_info["user"]
            and conn_info["password"]
            and conn_info["host"]
            and conn_info["port"]
            and conn_info["database"]
        )

        try:
            self.pgconn = pg2.connect(
                user=conn_info["user"],
                password=conn_info["password"],
                host=conn_info["host"],
                port=conn_info["port"],
                database=conn_info["database"],
            )
        except Exception as ex:
            print(f"Exception: {ex}")
            raise ex

    def create_table(self):
        try:
            with self.pgconn.cursor() as cursor:
                cursor.execute(MasterData.SQL_CREATE_TABLE)
                self.pgconn.commit()
        except Exception as ex:
            print(ex)

    def check_table_exists(self):
        with self.pgconn.cursor() as cursor:
            cursor.execute(MasterData.SQL_CHECK_TABLE_EXISTS)

    def get_all(self):
        with self.pgconn.cursor() as cursor:
            cursor.execute(MasterData.SQL_GET_ALL)
            schedule_master_data = cursor.fetchall()
        return schedule_master_data

    def insert_record(self, input):
        with self.pgconn.cursor() as cursor:
            cursor.execute(
                MasterData.SQL_INSERT_RECORD,
                (input["name"], input["type"], input["schedule"], input["data"]),
            )

    def close(self):
        if self.pgconn:
            self.pgconn.close()
