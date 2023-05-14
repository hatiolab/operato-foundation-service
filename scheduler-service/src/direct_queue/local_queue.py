import sys
import time
import lmdb
import json

# from helper.logger import Logger
# from helper.util import print_elasped_time

from direct_queue.queue_abstraction import Queue
from helper.util import convert_bytearray_to_dict


class LocalQueue(Queue):
    TSDB_NAME = "tsdb"
    DLQDB_NAME = "dlqdb"
    DB_MAP_SIZE = 512 * 1024 * 1024  # default map isze : 250M

    def __init__(self, path):
        self.initialized: bool = False
        try:
            self._imdb_env = lmdb.open(
                path, create=True, map_size=LocalQueue.DB_MAP_SIZE, max_dbs=2
            )
            # if duplicated key is allowed, dupsort is set to True
            self.tsdb = self._imdb_env.open_db(
                LocalQueue.TSDB_NAME.encode(), dupsort=False
            )
            self.dlqdb = self._imdb_env.open_db(
                LocalQueue.DLQDB_NAME.encode(), dupsort=False
            )
        except Exception as ex:
            print(ex, file=sys.stderr)
            raise ex

        self.initialized = True

    def close(self):
        self._imdb_env.close()

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
        self._put(tstamp, tdata, db=self.tsdb if not dlq else self.dlqdb)

    def _put(self, tstamp, tdata, db):
        self.check_database_initialized()
        # convert input key & value data to bytearray
        tstamp = LocalQueue.convert_to_bin(tstamp)
        tdata = LocalQueue.convert_to_bin(tdata)
        with self._imdb_env.begin(db=db, write=True) as txn:
            txn.put(tstamp, tdata, db=db)

    def pop(self, key, dlq=False):
        key_bytes = LocalQueue.convert_to_bin(key)
        return self._pop(key_bytes, db=self.tsdb if not dlq else self.dlqdb)

    def _pop(self, key, db):
        self.check_database_initialized()
        value = None
        with self._imdb_env.begin(db=db, write=True) as txn:
            cursor = txn.cursor()
            value = cursor.pop(key)
        return value

    def get_key_list(self, dlq=False):
        self.check_database_initialized()
        key_list = list()
        with self._imdb_env.begin(db=self.tsdb if not dlq else self.dlqdb) as txn:
            key_list = [key.decode("utf-8") for key, _ in txn.cursor()]
        return key_list

    def get_key_value_list(self, dlq=False):
        self.check_database_initialized()
        key_value_list = list()
        with self._imdb_env.begin(db=self.tsdb if not dlq else self.dlqdb) as txn:
            key_value_list = [
                (key.decode("utf-8"), convert_bytearray_to_dict(value))
                for key, value in txn.cursor()
            ]
        return key_value_list
