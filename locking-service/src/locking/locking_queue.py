import select
import sys
import time
import asyncio
import uuid
import json

import psycopg2 as pg2
import psycopg2.extensions
from datetime import datetime

from config import Config


class LockingQueue:
    LOCKINGQUEUE_TABLE = Config.locking_table() or "locking_queue"

    def __init__(self, db_config):
        self.initialized: bool = False
        self.connected: bool = False
        try:
            self.dbconn = None
            while self.dbconn is None:
                try:
                    self.dbconn = pg2.connect(
                        user=db_config.get("id", "postgres"),
                        password=db_config.get("pw", "abcd1234"),
                        host=db_config.get("host", "localhost"),
                        port=db_config.get("port", 5432),
                        database=db_config.get("db", "locking_queue"),
                    )
                except pg2.OperationalError:
                    print("Unable to connect. Retrying...")
                    time.sleep(5)

            self.dbconn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )

            self.connected = True
            self.db_config = db_config.copy()

            # CREATE IF EXISTS
            """
            CREATE TABLE locking_queue (  
                id UUID NOT NULL PRIMARY KEY,
                status VARCHAR(255) NOT NULL,
                payload TEXT,
                created_at TIMESTAMP NOT NULL
            );
            """

            with self.dbconn.cursor() as cursor:
                cursor.execute(
                    f"""
                        CREATE TABLE IF NOT EXISTS {LockingQueue.LOCKINGQUEUE_TABLE} (
                            id UUID NOT NULL PRIMARY KEY,
                            status VARCHAR(255) NOT NULL,
                            payload TEXT,
                            created_at TIMESTAMP NOT NULL
                        );

                        CREATE OR REPLACE FUNCTION notify_locking()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            NOTIFY locking_channel, 'locking_event_occurred';
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;

                        DO $$
                        BEGIN
                            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'locking_trigger') THEN
                                EXECUTE 'CREATE TRIGGER locking_trigger
                                        AFTER UPDATE ON locking_queue
                                        FOR EACH ROW EXECUTE FUNCTION notify_locking();';
                            END IF;
                        END$$;
                    """
                )

            # with self.dbconn.cursor() as cursor:
            #     cursor.execute(
            #         f"""
            #             CREATE OR REPLACE FUNCTION notify_locking()
            #             RETURNS TRIGGER AS $$
            #             BEGIN
            #                 NOTIFY locking_channel, 'locking_event_occurred';
            #                 RETURN NEW;
            #             END;
            #             $$ LANGUAGE plpgsql;

            #             CREATE OR REPLACE TRIGGER locking_trigger
            #             AFTER INSERT OR UPDATE OR DELETE ON locking_queue
            #             FOR EACH ROW EXECUTE FUNCTION notify_locking();
            #         """
            #     )

            self.dbconn.commit()
        except Exception as ex:
            print(ex, file=sys.stderr)
            self.dbconn.rollback()
            raise ex

        self.initialized = True

    def close(self):
        self.dbconn.close()

    def check_connection(self):
        return True if self.dbconn and (self.dbconn.closed == 0) else False

    def check_and_reconnect(self):
        while not self.check_connection():
            try:
                self.dbconn = pg2.connect(
                    user=self.db_config.get("id", "postgres"),
                    password=self.db_config.get("pw", "abcd1234"),
                    host=self.db_config.get("host", "localhost"),
                    port=self.db_config.get("port", 5432),
                    database=self.db_config.get("db", "scheduler"),
                )
            except pg2.OperationalError:
                self.dbconn and self.dbconn.close()
                self.dbconn = None

                print("Unable to connect. Retrying...")
                time.sleep(5)

        self.connected = True

    def check_database_initialized(self):
        if not self.initialized:
            raise Exception("Database is not initialized.")

    def insert(self, status):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = LockingQueue.LOCKINGQUEUE_TABLE

        sql_insert = f"""
        INSERT INTO {table_name} (id, status, created_at)
            VALUES (%s, %s, %s)
        """

        # sql_insert = f"""
        # INSERT INTO {table_name} (id, name, status, wait_until, created_at)
        #     VALUES (%s, %s, %s, %s, %s)
        #     ON CONFLICT (name)
        #     DO UPDATE SET (status, wait_until) = (EXCLUDED.status, EXCLUDED.wait_until);
        # """

        new_id = str(uuid.uuid4())
        print(f"insert: {new_id}, {status}")

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(
                    sql_insert,
                    (new_id, status, str(datetime.utcnow())),
                )
            self.dbconn.commit()
        except Exception as ex:
            print(ex, file=sys.stderr)
            self.dbconn.rollback()

        return new_id

    def update(self, locking_id, status, payload=None):
        self.check_database_initialized()

        payload_str = json.dumps(payload) if type(payload) == dict else payload

        sql_update = f"""
        UPDATE {LockingQueue.LOCKINGQUEUE_TABLE} SET status = %s, payload = %s WHERE id = %s
        """

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_update, (status, payload_str or "", locking_id))
            self.dbconn.commit()
        except Exception as ex:
            print(ex, file=sys.stderr)
            self.dbconn.rollback()

    def delete_with_id(self, id):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = LockingQueue.LOCKINGQUEUE_TABLE

        sql_delete = f"""
        DELETE FROM {table_name} WHERE id = %s RETURNING id
        
        """

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_delete, (id,))
                fetch_results = cursor.fetchall()
            self.dbconn.commit()
        except Exception as ex:
            print(ex, file=sys.stderr)
            self.dbconn.rollback()

        # TODO: check if fetch_results is available or not
        return fetch_results

    def get_with_id(self, id) -> list:
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = LockingQueue.LOCKINGQUEUE_TABLE

        sql_get = f"""
        SELECT id, status, payload from {table_name} WHERE id = %s
        
        """

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_get, (id,))
                fetch_results = cursor.fetchall()
        except Exception as ex:
            print(ex, file=sys.stderr)
            fetch_results = []

        # TODO: check if fetch_results is available or not
        return fetch_results

    def get_locks(self) -> list:
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = LockingQueue.LOCKINGQUEUE_TABLE

        sql_get = f""" 
        SELECT id, status, payload from {table_name}

        """

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_get)
                fetch_results = cursor.fetchall()
        except Exception as ex:
            print(ex, file=sys.stderr)
            fetch_results = None

        # TODO: check if fetch_results is available or not
        return fetch_results

    def listen_trigger(self, timeout=None):
        # 알림 수신 대기
        try:
            cur = self.dbconn.cursor()
            cur.execute("LISTEN my_channel;")
            print("Waiting for notifications on channel 'my_channel'")

            if select.select([self.dbconn], [], [], timeout) == ([], [], []):
                print("Timeout")
            else:
                self.dbconn.poll()
                while self.dbconn.notifies:
                    notify = self.dbconn.notifies.pop(0)
                    print("Got NOTIFY:", notify.pid, notify.channel, notify.payload)

            print("Done")

        except Exception as ex:
            print(ex, file=sys.stderr)
            fetch_results = None

    def is_released(self, id):
        self.check_database_initialized()

        if not id:
            return False

        lockings = self.get_with_id(id)
        if lockings:
            return (lockings[0][1] == "RELEASED", lockings[0][2])
        else:
            return (True, {})

    # TODO: check if this locknig function is available or not later
    async def listen_triggers_async(self, id, sleep_interval=10):
        """
        wait for the database notification for the status update
        this nofication avoid the repetitive database access
        """

        try:
            cur = self.dbconn.cursor()
            cur.execute("LISTEN locking_channel;")
            print("Waiting for notifications on channel 'locking_channel'")

            notified = False
            while not notified:
                # call dbconn.poll() to receive the notification
                await asyncio.get_running_loop().run_in_executor(None, self.dbconn.poll)
                # check if the notification is available
                while self.dbconn.notifies:
                    notify = self.dbconn.notifies.pop(0)
                    print("Got NOTIFY:", notify.pid, notify.channel, notify.payload)

                    if self.is_released(id):
                        notified = True
                        break
                #
                await asyncio.sleep(sleep_interval)
            print("Done")

        except Exception as ex:
            print(ex, file=sys.stderr)

    async def wait_for_status_released(self, id):
        sleep_interval = 3
        try:
            notified = False
            start_time = time.time()
            payload = None
            while not notified:
                (is_released, payload) = self.is_released(id)

                if is_released:
                    notified = True
                    break

                current_time = time.time()

                # TODO: 300 seconds is the maximum waiting time
                if current_time - start_time > 300:
                    break

                await asyncio.sleep(sleep_interval)
            print("Done")
        except Exception as ex:
            print(ex, file=sys.stderr)

        return (notified, payload)
