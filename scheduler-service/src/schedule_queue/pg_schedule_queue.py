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
                name VARCHAR(255) NOT NULL,
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

    def _generate_schedule_dict(self, id: str, payload: str) -> dict:
        payload_dict = json.loads(payload) if type(payload) == str else payload
        payload_dict["id"] = id
        return payload_dict

    def put(self, id, name, next_schedule, payload):
        self.check_database_initialized()

        # delete the existed schedule with the same id
        (dup_id, _, _, _) = self.get_with_name(name, True)
        dup_id and self.delete_with_id(dup_id)

        # TODO: need to configure this table name
        table_name = PGScheduleQueue.SCHEDULE_TABLE

        payload_str = json.dumps(payload) if type(payload) == dict else payload

        sql_put = f"""
        INSERT INTO {table_name} (id, name, next_schedule, created_at, processing_started_at, payload)
            VALUES (%s, %s, %s, %s, NULL, %s)
            ON CONFLICT (name)
            DO UPDATE SET (next_schedule, payload) = (EXCLUDED.next_schedule, EXCLUDED.payload);
        """

        with self.dbconn.cursor() as cursor:
            cursor.execute(
                sql_put,
                (id, name, next_schedule, str(datetime.utcnow()), payload_str),
            )
        self.dbconn.commit()

    def delete_with_id(self, id):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = PGScheduleQueue.SCHEDULE_TABLE

        sql_delete = f"""
        DELETE FROM {table_name} WHERE id = %s RETURNING id, payload
        
        """

        with self.dbconn.cursor() as cursor:
            cursor.execute(sql_delete, (id,))
            fetch_results = cursor.fetchall()

        self.dbconn.commit()

        assert len(fetch_results) <= 1

        # TODO: convert payload to dict like 'json.loads(existed_schedule.decode("utf-8"))'
        results = list()
        for fetch_result in fetch_results:
            if len(fetch_result) > 0:
                results.append(
                    self._generate_schedule_dict(fetch_result[0], fetch_result[1])
                )
        return results

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
            client_operation,
            client_application,
            client_group,
            client_key,
            client_type,
        )

        for result in results:
            self.delete_with_id(result["id"])

        return results

    def get_with_id(self, id):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = PGScheduleQueue.SCHEDULE_TABLE

        sql_get = f"""
        SELECT id, name, next_schedule, payload from {table_name} WHERE id = %s AND processing_started_at IS NULL
        
        """

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_get, (id,))
                fetch_results = cursor.fetchall()
        except Exception as ex:
            print(ex, file=sys.stderr)
            fetch_results = None

        assert len(fetch_results) <= 1

        # TODO: convert payload to dict like 'json.loads(existed_schedule.decode("utf-8"))'

        return [
            self._generate_schedule_dict(fetch_result[0], fetch_result[3])
            for fetch_result in fetch_results
        ]

    def get_with_name(self, name: str, with_already_processed: bool = False):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = PGScheduleQueue.SCHEDULE_TABLE

        sql_get = (
            f"""
        SELECT id, name, next_schedule, payload from {table_name} WHERE name = %s AND processing_started_at IS NULL
        
        """
            if not with_already_processed
            else f"""
        SELECT id, name, next_schedule, payload from {table_name} WHERE name = %s
        
        """
        )

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_get, (name,))
                fetch_results = cursor.fetchall()
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
        SELECT id, name, payload from {table_name} WHERE processing_started_at IS NULL
        
        """

        results = list()

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_get)
                fetch_results = cursor.fetchall()

                for fetch_result in fetch_results:
                    fetched_name = fetch_result[1]
                    [
                        fetched_application,
                        fetched_group,
                        fetched__type,
                        fetched_key,
                        fetched_operation,
                    ] = fetched_name.split(",")

                results = [
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
        except Exception as ex:
            print(ex, file=sys.stderr)
            results = None

        return [
            self._generate_schedule_dict(result[0], result[2]) for result in results
        ]

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
            pop_results = []

        return [
            self._generate_schedule_dict(pop_result[0], pop_result[3])
            for pop_result in pop_results
        ]
