from unittest import TestCase, main

from direct_queue.local_queue import LocalQueue


class LocalQueueTest(TestCase):
    DB_NAME = "../db/unittest_lmdb"

    def test_localqueue_all_pop(self):
        tdb = LocalQueue(LocalQueueTest.DB_NAME)

        for key in tdb.get_key_list():
            tdb.pop(key)

        print(len(tdb.get_key_value_list()))

        self.assertEqual(len(tdb.get_key_value_list()), 0)

        tdb.put("abc1", {"name": "11"})
        self.assertEqual(len(tdb.get_key_value_list()), 1)

        tdb.put("abc2", {"name": "22"})
        tdb.put("abc3", {"name": "33"})
        self.assertEqual(len(tdb.get_key_value_list()), 3)

    def test_localqueue_put(self):
        tdb = LocalQueue(LocalQueueTest.DB_NAME)

        for key in tdb.get_key_list():
            tdb.pop(key)

        key1 = "key1"
        event_data = {
            "name": "delay-test",
            "type": "delay",
            "schedule": "1",
            "task": {
                "type": "rest",
                "connection": {
                    "host": "http://localhost:3000/api/unstable/run-scenario/DELAY-TEST",
                    "headers": {
                        "Content-Type": "application/json",
                        "accept": "*/*",
                    },
                },
                "data": {"instanceName": "delay-test", "variables": {}},
            },
        }
        tdb.put(key1, event_data)

        key_value_list = tdb.get_key_value_list()
        self.assertEqual(1, len(key_value_list))

        (key, value) = key_value_list[0]

        self.assertEqual(key, "key1")
        task = value["task"]
        self.assertEqual(task["type"], "rest")
        self.assertEqual(task["data"], {"instanceName": "delay-test", "variables": {}})

    def test_localqueue_dlq(self):
        tdb = LocalQueue(LocalQueueTest.DB_NAME)

        for key in tdb.get_key_list(dlq=True):
            tdb.pop(key, dlq=True)

        print(len(tdb.get_key_value_list(dlq=True)))

        self.assertEqual(len(tdb.get_key_value_list(dlq=True)), 0)

        tdb.put("abc1", {"name": "11"}, dlq=True)
        self.assertEqual(len(tdb.get_key_value_list(dlq=True)), 1)

        tdb.put("abc2", {"name": "22"}, dlq=True)
        tdb.put("abc3", {"name": "33"}, dlq=True)
        self.assertEqual(len(tdb.get_key_value_list(dlq=True)), 3)


if __name__ == "__main__":
    main()
