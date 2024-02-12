import select
import sys
import time
import json
import asyncio

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
                    """
                )

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

    def insert(self, id, name, status, wait_until):
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

        print(f"insert: {id}, {name}, {status}, {wait_until}")

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(
                    sql_insert,
                    (id, name, status, wait_until, str(datetime.utcnow())),
                )
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

    def get_with_id(self, id):
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

    def get_with_name(self, name: str):
        self.check_database_initialized()

        # TODO: need to configure this table name
        table_name = LockingQueue.LOCKINGQUEUE_TABLE

        sql_get = f"""
        SELECT id, name, status, wait_until from {table_name} WHERE name = %s
        
        """

        try:
            with self.dbconn.cursor() as cursor:
                cursor.execute(sql_get, (name,))
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

    async def listen_trigger_async(self, timeout=None):
        # 알림 수신 대기
        try:
            cur = self.dbconn.cursor()
            cur.execute("LISTEN my_channel;")
            print("Waiting for notifications on channel 'my_channel'")

            notified = False
            while not notified:
                await asyncio.get_running_loop().run_in_executor(None, self.dbconn.poll)
                while self.dbconn.notifies:
                    notify = self.dbconn.notifies.pop(0)
                    print("Got NOTIFY:", notify.pid, notify.channel, notify.payload)
                    notified = True
                await asyncio.sleep(1)

            print("Done")

        except Exception as ex:
            print(ex, file=sys.stderr)
            fetch_results = None
