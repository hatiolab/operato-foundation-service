import sys
import time
import json

import psycopg2 as pg2
from datetime import datetime

from schedule_queue.queue_abstraction import ScheduleQueue


class PGScheduleQueue(ScheduleQueue):
    SCHEDULE_TABLE = "schedule_queue"

    def __init__(self, db_config):
        self.initialized: bool = False
        try:
            self.dbconn = None
            while self.dbconn is None:
                try:
                    self.dbconn = pg2.connect(
                        user=db_config.get("id", "postgres"),
                        password=db_config.get("pw", "abcd1234"),
                        host=db_config.get("host", "localhost"),
                        port=db_config.get("port", 5432),
                        database=db_config.get("db", "scheduler"),
                    )
                except pg2.OperationalError:
                    print("Unable to connect. Retrying...")
                    time.sleep(5)

            # CREATE IF EXISTS
            """
            CREATE TABLE schedule_queue (  
                id UUID NOT NULL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                next_schedule bigint,
                created_at TIMESTAMP NOT NULL,
                processing_started_at TIMESTAMP,
                payload TEXT
            );
            """

            with self.dbconn.cursor() as cursor:
                cursor.execute(
                    f"""
                        CREATE TABLE IF NOT EXISTS schedule_queue (
                            id UUID NOT NULL PRIMARY KEY,
                            name VARCHAR(255) NOT NULL UNIQUE,
                            next_schedule bigint,
                            created_at TIMESTAMP NOT NULL,
                            processing_started_at TIMESTAMP,
                            payload TEXT
                        );

                        CREATE INDEX IF NOT EXISTS ix_schedule_queue_pop on schedule_queue (next_schedule ASC)
                        INCLUDE (id)
                        WHERE processing_started_at IS NULL;

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

    def put(self, id, name, next_schedule, payload):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = PGScheduleQueue.SCHEDULE_TABLE

        payload_str = json.dumps(payload) if type(payload) == dict else payload

        sql_put = f"""
        INSERT INTO {table_name} (id, name, next_schedule, created_at, payload)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (name)
            DO UPDATE SET (next_schedule, payload) = (EXCLUDED.next_schedule, EXCLUDED.payload);
        """

        with self.dbconn.cursor() as cursor:
            cursor.execute(
                sql_put, (id, name, next_schedule, str(datetime.utcnow()), payload_str)
            )
        self.dbconn.commit()

    def delete_with_id(self, id):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = PGScheduleQueue.SCHEDULE_TABLE

        sql_delete = f"""
        DELETE FROM {table_name} WHERE id = %s RETRUNING id, payload
        
        """

        with self.dbconn.cursor() as cursor:
            cursor.execute(sql_delete, (id))
            fetch_results = cursor.fetchall()

        self.dbconn.commit()

        assert len(fetch_results) <= 1

        # TODO: convert payload to dict like 'json.loads(existed_schedule.decode("utf-8"))'

        return fetch_results[0] if len(fetch_results) == 1 else (None, None)

    def delete_with_client(
        self,
        client_operation: str,
        client_application: str,
        client_group: str,
        client_key: str,
        client_type: str,
    ):
        self.check_database_initialized()

        results = self.get_with_client(
            client_application,
            client_application,
            client_group,
            client_key,
            client_type,
        )

        for result in results:
            self.delete_with_id(result[0])

        return results

    def get_with_id(self, id):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = PGScheduleQueue.SCHEDULE_TABLE

        sql_get = f"""
        SELECT id, name, next_schedule, payload from {table_name} WHERE id = %s
        
        """

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_get, (id,))
                fetch_results = cursor.fetchall()

            self.dbconn.commit()
        except Exception as ex:
            print(ex, file=sys.stderr)
            fetch_results = None

        assert len(fetch_results) <= 1

        # TODO: convert payload to dict like 'json.loads(existed_schedule.decode("utf-8"))'

        return fetch_results[0] if len(fetch_results) == 1 else (None, None, None, None)

    def get_with_name(self, name):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = PGScheduleQueue.SCHEDULE_TABLE

        sql_get = f"""
        SELECT id, name, next_schedule, payload from {table_name} WHERE name = %s
        
        """

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_get, (name,))
                fetch_results = cursor.fetchall()

            self.dbconn.commit()
        except Exception as ex:
            print(ex, file=sys.stderr)
            fetch_results = None

        assert len(fetch_results) <= 1

        # TODO: convert payload to dict like 'json.loads(existed_schedule.decode("utf-8"))'

        return fetch_results[0] if len(fetch_results) == 1 else (None, None, None, None)

    def get_with_client(
        self,
        client_operation: str,
        client_application: str,
        client_group: str,
        client_key: str,
        client_type: str,
    ):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = PGScheduleQueue.SCHEDULE_TABLE

        sql_get = f"""
        SELECT id, name, payload from {table_name};
        
        """

        result_list = list()

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_get)
                fetch_results = cursor.fetchall()

                for fetch_result in fetch_results:
                    fetched_name = fetch_result[1]
                    [
                        fetched_operation,
                        fetched_application,
                        fetched_group,
                        fetched_key,
                        fetched__type,
                    ] = fetched_name.split(",")

                result_list = [
                    fetch_result
                    for fetch_result in fetch_results
                    if (not client_operation or fetched_operation == client_operation)
                    and (
                        not client_application
                        or fetched_application == client_application
                    )
                    and (not client_group or fetched_group == client_group)
                    and (not client_key or fetched_key == client_key)
                    and (not client_type or fetched__type == client_type)
                ]

            self.dbconn.commit()
        except Exception as ex:
            print(ex, file=sys.stderr)
            result_list = None

        return result_list

    def pop(self):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = PGScheduleQueue.SCHEDULE_TABLE

        sql_pop = f"""
        UPDATE {table_name} as sq SET processing_started_at = '{str(datetime.utcnow())}'
        WHERE sq.id = (
            SELECT sqInner.id FROM {table_name} sqInner
            WHERE sqInner.processing_started_at IS NULL AND sqInner.next_schedule <= {int(time.time())}
            ORDER By sqInner.next_schedule ASC
            LIMIT 1
            FOR UPDATE
        )
        RETURNING sq.id, sq.name, sq.next_schedule, sq.payload
        """

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_pop)
                pop_results = cursor.fetchall()

            self.dbconn.commit()
        except Exception as ex:
            print(ex, file=sys.stderr)
            pop_results = None

        return pop_results
