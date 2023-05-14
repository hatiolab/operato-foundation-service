import sys
import time
import json

import psycopg2 as pg2


# from helper.logger import Logger
# from helper.util import print_elasped_time

from direct_queue.queue_abstraction import Queue
from helper.util import convert_bytearray_to_dict


class PGQueue(Queue):
    TSDB_NAME = "schedules"
    DLQDB_NAME = "deadletters"

    def __init__(self, db_config):
        self.initialized: bool = False
        try:

            self.dbconn = pg2.connect(
                user=db_config.get("id", "postgres"),
                password=db_config.get("pw", "abcd1234"),
                host=db_config.get("host", "localhost"),
                port=db_config.get("port", 5432),
                database=db_config.get("db", "schedules"),
            )

            # CREATE IF EXISTS
            """
            CREATE TABLE schedules (  
                schid serial NOT NULL,
                name VARCHAR(255) NOT NULL
                value jsonb
            );

            CREATE TABLE deadletters (  
                dlid serial NOT NULL,
                name VARCHAR(255) NOT NULL
                value jsonb
            );
            """

            with self.dbconn.cursor() as cursor:
                cursor.execute(
                    f"""
                        CREATE TABLE IF NOT EXISTS schedules (  
                            name VARCHAR(255) NOT NULL PRIMARY KEY,
                            data JSONB
                        );
                        """
                )

                cursor.execute(
                    f"""
                        CREATE TABLE IF NOT EXISTS deadletters (  
                            name VARCHAR(255) NOT NULL PRIMARY KEY,
                            data JSONB
                        );
                        """
                )

            self.dbconn.commit()
        except Exception as ex:
            print(ex, file=sys.stderr)
            raise ex

        self.initialized = True

    def close(self):
        self.dbconn.close()

    # TODO: convert the wrapout code
    def check_database_initialized(self):
        if not self.initialized:
            raise Exception("Database is not initialized.")

    @staticmethod
    def convert_to_bin(indata):
        return (
            indata.encode()
            if type(indata) == str
            else json.dumps(indata).encode()
            if type(indata) == dict
            else str(indata).encode()
        )

    @staticmethod
    def convert_to_dict(indata):
        return json.loads(indata.decode("utf-8"))

    def put(self, tstamp, tdata, dlq=False):
        self.check_database_initialized()
        table_name = PGQueue.TSDB_NAME if not dlq else PGQueue.DLQDB_NAME
        tdata_str = json.dumps(tdata) if type(tdata) == dict else tdata

        sql_put = f"""
        INSERT INTO {table_name} (name, data)
            VALUES (%s, %s)
            ON CONFLICT (name)
            DO UPDATE SET data = EXCLUDED.data;
        """

        with self.dbconn.cursor() as cursor:
            cursor.execute(sql_put, (tstamp, tdata_str))

        self.dbconn.commit()

    def pop(self, key, dlq=False):
        self.check_database_initialized()
        table_name = PGQueue.TSDB_NAME if not dlq else PGQueue.DLQDB_NAME

        sql_pop = f"""
        DELETE FROM {table_name} where name = '{key}';
        """

        with self.dbconn.cursor() as cursor:
            cursor.execute(sql_pop)

        self.dbconn.commit()

    def get_key_list(self, dlq=False):
        self.check_database_initialized()
        table_name = PGQueue.TSDB_NAME if not dlq else PGQueue.DLQDB_NAME

        sql_list = f"""
        SELECT name FROM {table_name};
        """

        key_list = list()
        with self.dbconn.cursor() as cursor:
            cursor.execute(sql_list)
            key_list = cursor.fetchall()

        return [key[0] for key in key_list]

    def get_key_value_list(self, dlq=False):
        self.check_database_initialized()
        table_name = PGQueue.TSDB_NAME if not dlq else PGQueue.DLQDB_NAME

        sql_list = f"""
        SELECT name, data FROM {table_name};
        """

        key_list = list()
        with self.dbconn.cursor() as cursor:
            cursor.execute(sql_list)
            key_list = cursor.fetchall()

        return [(key[0], key[1]) for key in key_list]
