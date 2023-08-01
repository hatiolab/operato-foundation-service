from unittest import TestCase, main

from uuid import uuid4
import random
import time

from schedule_queue.pg_schedule_queue import PGScheduleQueue


def push_test():
    db_config = {
        "user": "postgres",
        "password": "abcd1234",
        "host": "127.0.0.1",
        "port": 55432,
        "database": "scheduler",
    }

    pg_schedule_queue = PGScheduleQueue(db_config)

    test_payload = dict()
    test_payload["test"] = "test"

    for i in range(1000):
        name = f"test{i}"
        schedule_next = random.randint(1234500000, 1234999999)
        pg_schedule_queue.put(str(uuid4()), name, schedule_next, test_payload)

    # print(pg_schedule_queue.pop())

    # self.assertEqual(len(tdb.get_key_value_list()), 0)
    # self.assertEqual(len(tdb.get_key_value_list(True)), 0)

    pg_schedule_queue.close()


def pop_test():
    db_config = {
        "user": "postgres",
        "password": "abcd1234",
        "host": "127.0.0.1",
        "port": 55432,
        "database": "scheduler",
    }

    pg_schedule_queue = PGScheduleQueue(db_config)

    for i in range(1000):
        print(pg_schedule_queue.pop())
        time.sleep(0.1)

    pg_schedule_queue.close()


if __name__ == "__main__":
    # push_test()
    pop_test()
