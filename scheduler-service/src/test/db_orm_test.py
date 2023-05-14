from unittest import TestCase, main

from history.db_engine import initialize_global_database, get_session
from history.tables.table_schedule_history import ScheduleEventHistory

from config import Config

Config("config.yaml")


class DBOrmTest(TestCase):
    def test_db_initialization(self):
        (host, port, id, pw, db) = Config.history()
        initialize_global_database(id, pw, host, port, db)

        db_session = get_session()

        self.assertIsNotNone(db_session)

    def test_db_add_record(self):
        (host, port, id, pw, db) = Config.history()
        initialize_global_database(id, pw, host, port, db)
        db_session = get_session()

        schevt_history = ScheduleEventHistory(
            event="register",
            name="unit_testcase",
            type="cron",
            schedule="*/1 * * * *",
            task_type="rest",
            task_connection={"host": "abc.com"},
            task_data={"data": "hahahaah"},
        )

        db_session.add(schevt_history)
        db_session.commit()

    def test_db_delete_record(self):
        (host, port, id, pw, db) = Config.history()
        initialize_global_database(id, pw, host, port, db)
        db_session = get_session()

        delete_test_all = (
            db_session.query(ScheduleEventHistory).filter_by(name="unit_testcase").all()
        )

        for delete_test in delete_test_all:
            db_session.delete(delete_test)
        db_session.commit()


if __name__ == "__main__":
    main()
