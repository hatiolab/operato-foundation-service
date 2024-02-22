import select
import sys
import time
import asyncio
import uuid

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
                name VARCHAR(255) NOT NULL UNIQUE,
                status VARCHAR(255) NOT NULL,
                wait_until INT NOT NULL,
                created_at TIMESTAMP NOT NULL,
            );
            """

            with self.dbconn.cursor() as cursor:
                cursor.execute(
                    f"""
                        CREATE TABLE IF NOT EXISTS {LockingQueue.LOCKINGQUEUE_TABLE} (
                            id UUID NOT NULL PRIMARY KEY,
                            name VARCHAR(255) NOT NULL UNIQUE,
                            status VARCHAR(255) NOT NULL,
                            wait_until INT NOT NULL,
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

    def insert(self, name, status, wait_until):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = LockingQueue.LOCKINGQUEUE_TABLE

        sql_insert = f"""
        INSERT INTO {table_name} (id, name, status, wait_until, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """

        # sql_insert = f"""
        # INSERT INTO {table_name} (id, name, status, wait_until, created_at)
        #     VALUES (%s, %s, %s, %s, %s)
        #     ON CONFLICT (name)
        #     DO UPDATE SET (status, wait_until) = (EXCLUDED.status, EXCLUDED.wait_until);
        # """

        new_id = str(uuid.uuid4())
        print(f"insert: {new_id}, {name}, {status}, {wait_until}")

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(
                    sql_insert,
                    (new_id, name, status, wait_until, str(datetime.utcnow())),
                )
            self.dbconn.commit()
        except Exception as ex:
            print(ex, file=sys.stderr)
            self.dbconn.rollback()

        return new_id

    def update(self, name, status, wait_until=10):
        self.check_database_initialized()

        sql_update = f"""
        UPDATE {LockingQueue.LOCKINGQUEUE_TABLE} SET status = %s, wait_until = %s WHERE name = %s
        """

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_update, (status, wait_until, name))
            self.dbconn.commit()
        except Exception as ex:
            print(ex, file=sys.stderr)
            self.dbconn.rollback()

    def delete_with_id(self, id):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = LockingQueue.LOCKINGQUEUE_TABLE

        sql_delete = f"""
        DELETE FROM {table_name} WHERE id = %s RETURNING id, name
        
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

    def delete_with_name(self, name):
        self.check_database_initialized()

        if not name:
            return None

        # TODO: need to configure this table name
        table_name = LockingQueue.LOCKINGQUEUE_TABLE

        sql_delete = f"""
        DELETE FROM {table_name} WHERE name = %s RETURNING id, name
        
        """

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_delete, (name,))
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
        SELECT id, name, status, wait_until from {table_name} WHERE id = %s
        
        """

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_get, (id,))
                fetch_results = cursor.fetchall()
        except Exception as ex:
            print(ex, file=sys.stderr)
            fetch_results = None

        # TODO: check if fetch_results is available or not
        return fetch_results

    def get_with_name(self, name: str) -> list:
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = LockingQueue.LOCKINGQUEUE_TABLE

        sql_get = (
            f"""
        SELECT id, name, status, wait_until from {table_name} WHERE name = %s
        
        """
            if name
            else f""" 
        SELECT id, name, status, wait_until from {table_name}

        """
        )

        try:
            with self.dbconn.cursor() as cursor:
                if name:
                    cursor.execute(sql_get, (name,))
                else:
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

    def is_released(self, name):
        self.check_database_initialized()

        if not name:
            return False

        lockings = self.get_with_name(name)
        if lockings:
            return lockings[0][2] == "RELEASED"

    # TODO: check if this locknig function is available or not later
    async def listen_triggers_async(self, name, sleep_interval=10):
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

                    if self.is_released(name):
                        notified = True
                        break
                #
                await asyncio.sleep(sleep_interval)
            print("Done")

        except Exception as ex:
            print(ex, file=sys.stderr)

    async def wait_for_status_released(self, name, wait_until=10):
        sleep_interval = 3
        try:
            notified = False
            start_time = time.time()
            while not notified:
                if self.is_released(name):
                    notified = True
                    break

                current_time = time.time()
                if current_time - start_time > wait_until:
                    break

                await asyncio.sleep(sleep_interval)
            print("Done")
        except Exception as ex:
            print(ex, file=sys.stderr)

        return notified
