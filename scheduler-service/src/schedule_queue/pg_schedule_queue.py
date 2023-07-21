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

    def pop(self):
        self.check_database_initialized()
        table_name = PGScheduleQueue.SCHEDULE_TABLE

        sql_pop = f"""
        UPDATE {table_name} as sq SET processing_started_at = '{str(datetime.utcnow())}'
        WHERE sq.id = (
            SELECT sqInner.id FROM {table_name} sqInner
            WHERE sqInner.processing_started_at IS NULL
            ORDER By sqInner.next_schedule ASC
            LIMIT 1
            FOR UPDATE
        )
        RETURNING sq.id, sq.name, sq.next_schedule, sq.payload
        """

        with self.dbconn.cursor() as cursor:
            cursor.execute(sql_pop)
            pop_result = cursor.fetchall()
            print(pop_result)

        self.dbconn.commit()
