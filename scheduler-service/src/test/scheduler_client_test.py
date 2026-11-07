"""
Client lookups, empty answers and update by id - against a real postgres.

The tests make their own database on the server and drop it at the end, so they never
touch the scheduler's own data. Without SCHEDULER_TEST_PG_HOST they are skipped.

    cd src
    SCHEDULER_TEST_PG_HOST=127.0.0.1 SCHEDULER_TEST_PG_PORT=55432 \\
        python -m unittest discover -s test -p scheduler_client_test.py -v

SCHEDULER_TEST_PG_USER (postgres) and SCHEDULER_TEST_PG_PW (abcd1234) are optional.
"""

import json
import os
import uuid
from unittest import TestCase, SkipTest

import psycopg2 as pg2

from config import Config

APPLICATION = "operato-schedule"
TYPE = "operato-office"
KEYS = [
    "office-license-expiry",
    "office-employee-profile-request",
    "office-weekly-report-assign",
]

PG = None
TEST_DB = None
client = None
scheduler = None


def admin_connect():
    conn = pg2.connect(
        host=PG["host"], port=PG["port"], user=PG["id"], password=PG["pw"], database="postgres"
    )
    conn.autocommit = True
    return conn


def setUpModule():
    global PG, TEST_DB, client, scheduler

    host = os.environ.get("SCHEDULER_TEST_PG_HOST")
    if not host:
        raise SkipTest("SCHEDULER_TEST_PG_HOST is not set")

    PG = {
        "host": host,
        "port": int(os.environ.get("SCHEDULER_TEST_PG_PORT", "5432")),
        "id": os.environ.get("SCHEDULER_TEST_PG_USER", "postgres"),
        "pw": os.environ.get("SCHEDULER_TEST_PG_PW", "abcd1234"),
    }
    TEST_DB = f"scheduler_test_{uuid.uuid4().hex[:8]}"
    conn = admin_connect()
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE {TEST_DB}")
    conn.close()

    Config(
        config_data={
            "database": {**PG, "db": TEST_DB},
            "scheduler": {"debug": False, "fetch_interval": 0.5, "fetch_count": 1, "table": "schedule_queue"},
        }
    )

    # imported after Config so the scheduler connects to the test database
    from fastapi.testclient import TestClient
    from restful.rest_main import fast_api
    from schedule.scheduler import Scheduler

    # no `with` - the startup hook (which starts the fetch thread) does not run
    client = TestClient(fast_api)
    scheduler = Scheduler()


def tearDownModule():
    if scheduler is not None:
        scheduler.schedule_queue.close()
    if TEST_DB:
        conn = admin_connect()
        with conn.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        conn.close()


def schedule_body(key, group, schedule="0 9 * * *", data=None):
    return {
        "name": f"{key} · test",
        "client": {
            "application": APPLICATION,
            "group": group,
            "type": TYPE,
            "key": key,
            "operation": "run",
        },
        "type": "cron",
        "schedule": schedule,
        "timezone": "Asia/Seoul",
        "task": {
            "type": "rest",
            "connection": {"host": "http://office:3000/callback", "method": "POST"},
            "data": data if data is not None else {"taskKey": key, "domainId": group},
        },
    }


def client_name(key, group):
    return ",".join([APPLICATION, group, TYPE, key, "run"])


class SchedulerClientTest(TestCase):
    def setUp(self):
        queue = scheduler.schedule_queue
        with queue.dbconn.cursor() as cursor:
            cursor.execute("TRUNCATE schedule_queue")
        queue.dbconn.commit()

    def register(self, key, group, **kwargs):
        res = client.post("/schedules", json=schedule_body(key, group, **kwargs))
        self.assertEqual(res.status_code, 200, res.text)
        return res.json()["id"]

    def get(self, **params):
        res = client.get("/schedules", params=params)
        self.assertEqual(res.status_code, 200, res.text)
        return res.json()

    def row(self, schedule_id):
        conn = pg2.connect(
            host=PG["host"], port=PG["port"], user=PG["id"], password=PG["pw"], database=TEST_DB
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT name, next_schedule, payload FROM schedule_queue WHERE id = %s",
                    (schedule_id,),
                )
                found = cursor.fetchall()
        finally:
            conn.close()
        if not found:
            return None
        (name, next_schedule, payload) = found[0]
        return name, next_schedule, json.loads(payload)

    # empty answers

    def test_empty_scheduler_answers_empty_list(self):
        self.assertEqual(self.get(application=APPLICATION), [])
        self.assertEqual(
            self.get(application=APPLICATION, group="g1", type=TYPE, key=KEYS[0], operation="run"),
            [],
        )

    def test_no_match_answers_empty_list(self):
        self.register(KEYS[0], "g1")
        self.assertEqual(self.get(application=APPLICATION, group="g1", key=KEYS[1]), [])
        self.assertEqual(self.get(application="someone-else"), [])

    def test_delete_with_client_nothing_matched_is_count_zero(self):
        res = client.delete("/schedules", params={"application": APPLICATION, "key": KEYS[0]})
        self.assertEqual(res.status_code, 200, res.text)
        self.assertEqual(res.json(), {"count": 0})

    # several schedules in one group

    def test_each_key_finds_its_own_schedule(self):
        ids = {}
        for group in ["g1", "g2"]:
            for key in KEYS:
                ids[(key, group)] = self.register(key, group)
        self.assertEqual(len(set(ids.values())), 6)

        for (key, group), schedule_id in ids.items():
            found = self.get(application=APPLICATION, group=group, type=TYPE, key=key, operation="run")
            self.assertEqual([one["id"] for one in found], [schedule_id], (key, group))
            self.assertEqual(found[0]["client"]["key"], key)
            self.assertEqual(found[0]["client"]["group"], group)

        self.assertEqual(len(self.get(application=APPLICATION, group="g1")), 3)
        self.assertEqual(len(self.get(application=APPLICATION)), 6)
        self.assertEqual(
            sorted(one["id"] for one in self.get(key=KEYS[2])),
            sorted([ids[(KEYS[2], "g1")], ids[(KEYS[2], "g2")]]),
        )

    def test_delete_with_client_removes_only_its_key(self):
        for key in KEYS:
            self.register(key, "g1")
        res = client.delete("/schedules", params={"application": APPLICATION, "group": "g1", "key": KEYS[1]})
        self.assertEqual(res.json(), {"count": 1})
        left = sorted(one["client"]["key"] for one in self.get(application=APPLICATION, group="g1"))
        self.assertEqual(left, sorted([KEYS[0], KEYS[2]]))

    # update

    def test_update_replaces_payload_name_and_next_run(self):
        from schedule.schedule_type import ScheduleType

        # a placeholder far in the future under another key - like the one seeded in production
        placeholder = self.register(
            "placeholder", "g1", schedule="0 0 1 2 *", data={"placeholder": True}
        )
        other = self.register(KEYS[0], "g1")
        (_, placeholder_next, _) = self.row(placeholder)

        body = schedule_body(KEYS[2], "g1", schedule="0 9 * * *")
        body["id"] = placeholder
        expected_before = ScheduleType.get_next_and_delay(body, "Asia/Seoul")[0]
        res = client.post(f"/schedules/{placeholder}/update", json=body)
        expected_after = ScheduleType.get_next_and_delay(body, "Asia/Seoul")[0]
        self.assertEqual(res.status_code, 200, res.text)
        self.assertEqual(res.json()["id"], placeholder)
        self.assertEqual(res.json()["client"]["key"], KEYS[2])

        (name, next_schedule, payload) = self.row(placeholder)
        self.assertEqual(name, client_name(KEYS[2], "g1"))
        self.assertIn(next_schedule, (expected_before, expected_after))
        self.assertLess(next_schedule, placeholder_next)
        self.assertEqual(payload["client"]["key"], KEYS[2])
        self.assertEqual(payload["schedule"], "0 9 * * *")
        self.assertEqual(payload["task"]["data"], {"taskKey": KEYS[2], "domainId": "g1"})
        self.assertNotIn("placeholder", payload["task"]["data"])

        # the lookups follow the new name; the other schedule is untouched
        self.assertEqual([one["id"] for one in self.get(application=APPLICATION, group="g1", key=KEYS[2])], [placeholder])
        self.assertEqual(self.get(application=APPLICATION, group="g1", key="placeholder"), [])
        self.assertEqual([one["id"] for one in self.get(application=APPLICATION, group="g1", key=KEYS[0])], [other])
        self.assertEqual(len(self.get(application=APPLICATION, group="g1")), 2)

    def test_update_same_client_changes_schedule(self):
        schedule_id = self.register(KEYS[1], "g1", schedule="0 9 * * *")
        res = client.post(
            f"/schedules/{schedule_id}/update",
            json=schedule_body(KEYS[1], "g1", schedule="30 10 * * *", data={"v": 2}),
        )
        self.assertEqual(res.status_code, 200, res.text)
        (name, _, payload) = self.row(schedule_id)
        self.assertEqual(name, client_name(KEYS[1], "g1"))
        self.assertEqual(payload["schedule"], "30 10 * * *")
        self.assertEqual(payload["task"]["data"], {"v": 2})

    def test_update_unknown_id_is_404(self):
        for schedule_id in [str(uuid.uuid4()), "undefined"]:
            res = client.post(f"/schedules/{schedule_id}/update", json=schedule_body(KEYS[0], "g1"))
            self.assertEqual(res.status_code, 404, res.text)
        self.assertEqual(self.get(application=APPLICATION), [])

    def test_update_onto_a_client_another_schedule_holds_is_409(self):
        first = self.register(KEYS[0], "g1")
        second = self.register(KEYS[1], "g1")
        res = client.post(f"/schedules/{second}/update", json=schedule_body(KEYS[0], "g1"))
        self.assertEqual(res.status_code, 409, res.text)
        self.assertEqual(self.row(second)[0], client_name(KEYS[1], "g1"))
        self.assertEqual(self.row(first)[0], client_name(KEYS[0], "g1"))

    # what @things-factory/scheduled-task does at each boot

    def test_reconcile_twice_keeps_one_schedule_per_key(self):
        groups = ["g1", "g2"]
        for _boot in range(2):
            for group in groups:
                for key in KEYS:
                    live = self.get(application=APPLICATION, group=group, type=TYPE, key=key, operation="run")
                    body = schedule_body(key, group)
                    if live:
                        res = client.post(f"/schedules/{live[0]['id']}/update", json={**body, "id": live[0]["id"]})
                    else:
                        res = client.post("/schedules", json=body)
                    self.assertEqual(res.status_code, 200, res.text)
                    for duplicate in live[1:]:
                        client.delete(f"/schedules/{duplicate['id']}")

        everything = self.get(application=APPLICATION)
        self.assertEqual(len(everything), 6)
        self.assertEqual(
            sorted((one["client"]["group"], one["client"]["key"]) for one in everything),
            sorted((group, key) for group in groups for key in KEYS),
        )
