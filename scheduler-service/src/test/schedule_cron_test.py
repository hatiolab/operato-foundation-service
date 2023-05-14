from unittest import TestCase, main

from datetime import datetime, timedelta
import pytz
from schedule.schedule_cron import CronTime


class ScheduleCronTest(TestCase):

    """
    CRON FORMAT VALIDATION

    """

    def test_no_exceptions_1(self):
        try:
            CronTime("* * * * * *")
            CronTime("0 * * * * *")
            CronTime("08 * * * * *")
            CronTime("08 8 8 8 8 5")
            CronTime("* * * * *")
            CronTime("0-10 * * * * *")
            CronTime("0-10 0-10 * * * *")
            CronTime("0-10 0-10 1-10 1-10 0-6 0-1")
            CronTime("0,10 0,10 * * * *")
            CronTime("0,10 0,10 1,10 1,10 0,6 0,1")
        except Exception as ex:
            self.fail(f"test_no_exceptions_1 raised Exception: {ex}")

    def test_no_exceptions_2(self):
        try:
            CronTime("* * * * jan *")
            CronTime("* * * * jan,feb *")
            CronTime("* * * * jan,feb mon,tue")

        except Exception as ex:
            self.fail(f"test_no_exceptions_2 raised Exception: {ex}")

    def test_raise_exception_1(self):
        with self.assertRaises(Exception):
            CronTime("* * * * jar 1")

        with self.assertRaises(Exception):
            CronTime("* * * * j *")

        with self.assertRaises(Exception):
            CronTime("* * * *")

        with self.assertRaises(Exception):
            CronTime("* * * * * * *")

        with self.assertRaises(Exception):
            CronTime("* * * * 1234")

        with self.assertRaises(Exception):
            CronTime("* * * * 0*")

        with self.assertRaises(Exception):
            CronTime("* * * 1/0 *")

        with self.assertRaises(Exception):
            CronTime("* 2-1 * * *")

        with self.assertRaises(Exception):
            CronTime("* * /4 * * *")

    def test_raise_exception_2(self):
        with self.assertRaises(Exception):
            CronTime("* * * L * *")

        with self.assertRaises(Exception):
            CronTime("* * * * * L")

    """
    COMPARISON TEST
    
    """

    def test_standard_cron_format(self):
        standard = CronTime("8 8 8 8 5", "Asia/Seoul", None)
        extended = CronTime("0 8 8 8 8 5", "Asia/Seoul", None)

        self.assertEqual(standard.time_units, extended.time_units)

    """
    DAY ROLL-OVER
    
    """

    def test_day_roll_over(self):
        num_hours = 24
        ct = CronTime("0 0 17 * * *", "UTC")

        for hr in range(num_hours):
            start = datetime(2012, 3, 16, hr, 30, 30)
            start = start.astimezone(pytz.timezone("UTC"))
            next = ct.get_next(start)
            diff = next - start
            self.assertLess(diff.seconds * 100, 24 * 60 * 60 * 1000)
            self.assertGreater(next.timestamp(), start.timestamp())

    """
    GET NEXT
    
    """

    def test_get_next_1(self):
        cron_time = CronTime("0 * * * * *", "Asia/Seoul", None)
        next_datetime = cron_time.get_next(datetime(2018, 6, 3, 0, 0), "Asia/Seoul")
        self.assertEqual(next_datetime.year, 2018)
        self.assertEqual(next_datetime.month, 6)
        self.assertEqual(next_datetime.day, 3)
        self.assertEqual(next_datetime.hour, 0)
        self.assertEqual(next_datetime.minute, 1)
        self.assertEqual(next_datetime.second, 0)

    def test_get_next_2(self):
        cron_time = CronTime("0 0 */4 * * *", "Asia/Seoul", None)
        next_datetime = cron_time.get_next(datetime(2018, 6, 3, 0, 0), "Asia/Seoul")
        self.assertEqual(next_datetime.year, 2018)
        self.assertEqual(next_datetime.month, 6)
        self.assertEqual(next_datetime.day, 3)
        self.assertEqual(next_datetime.hour, 4)
        self.assertEqual(next_datetime.minute, 0)
        self.assertEqual(next_datetime.second, 0)

    def test_get_next_3(self):
        base = datetime.fromisoformat("2018-03-29T23:15")

        cron_time = CronTime("30 0 * * 5", "Asia/Amman", None)
        next_datetime = cron_time.get_next(base, "Asia/Amman")
        print(next_datetime)
        self.assertEqual(next_datetime.year, 2018)
        self.assertEqual(next_datetime.month, 3)
        self.assertEqual(next_datetime.day, 30)
        self.assertEqual(next_datetime.hour, 1)
        self.assertEqual(next_datetime.minute, 0)
        self.assertEqual(next_datetime.second, 0)

        base = next_datetime
        next_datetime = cron_time.get_next(base, "Asia/Amman")
        self.assertEqual(next_datetime.year, 2018)
        self.assertEqual(next_datetime.month, 4)
        self.assertEqual(next_datetime.day, 6)
        self.assertEqual(next_datetime.hour, 0)
        self.assertEqual(next_datetime.minute, 30)
        self.assertEqual(next_datetime.second, 0)

        base = next_datetime
        next_datetime = cron_time.get_next(base, "Asia/Amman")
        self.assertEqual(next_datetime.year, 2018)
        self.assertEqual(next_datetime.month, 4)
        self.assertEqual(next_datetime.day, 13)
        self.assertEqual(next_datetime.hour, 0)
        self.assertEqual(next_datetime.minute, 30)
        self.assertEqual(next_datetime.second, 0)

    def test_get_next_4(self):
        ct = CronTime("0 0 */4 * * *")

        nextDate = datetime.now()
        nextDate = nextDate.astimezone(pytz.timezone("UTC"))
        nextdt = ct.get_next(nextDate)

        self.assertGreater(nextdt.timestamp(), nextDate.timestamp())
        self.assertEqual(nextdt.hour % 4, 0)

    """
    PRESETS
    
    """

    def test_presets_1(self):
        cron_time_1 = CronTime("@secondly")
        cron_time_2 = CronTime("* * * * * *")
        self.assertEqual(cron_time_1.time_units, cron_time_2.time_units)

        cron_time_1 = CronTime("@minutely")
        cron_time_2 = CronTime("0 * * * * *")
        self.assertEqual(cron_time_1.time_units, cron_time_2.time_units)

        cron_time_1 = CronTime("@hourly")
        cron_time_2 = CronTime("0 0 * * * *")
        self.assertEqual(cron_time_1.time_units, cron_time_2.time_units)

        cron_time_1 = CronTime("@daily")
        cron_time_2 = CronTime("0 0 0 * * *")
        self.assertEqual(cron_time_1.time_units, cron_time_2.time_units)

        cron_time_1 = CronTime("@weekly")
        cron_time_2 = CronTime("0 0 0 * * 0")
        self.assertEqual(cron_time_1.time_units, cron_time_2.time_units)

        cron_time_1 = CronTime("@weekdays")
        cron_time_2 = CronTime("0 0 0 * * 1,2,3,4,5")
        self.assertEqual(cron_time_1.time_units, cron_time_2.time_units)

        cron_time_1 = CronTime("@weekends")
        cron_time_2 = CronTime("0 0 0 * * 0,6")
        self.assertEqual(cron_time_1.time_units, cron_time_2.time_units)

        cron_time_1 = CronTime("@monthly")
        cron_time_2 = CronTime("0 0 0 1 * *")
        self.assertEqual(cron_time_1.time_units, cron_time_2.time_units)

        cron_time_1 = CronTime("@yearly")
        cron_time_2 = CronTime("0 0 0 1 0 *")
        self.assertEqual(cron_time_1.time_units, cron_time_2.time_units)

    """
    STRIP OFF MILLISECOND
    
    """

    def test_strip_off_millisecond_1(self):
        base = pytz.timezone("Asia/Seoul").localize(
            datetime.fromisoformat("2018-08-10T02:20:00.999")
        )

        cron_time = CronTime("0 */10 * * * *")
        x = cron_time.get_next(base, "Asia/Seoul")

        self.assertEqual(
            x.timestamp(),
            datetime.fromisoformat("2018-08-10T02:30:00.000")
            .astimezone(pytz.timezone("Asia/Seoul"))
            .timestamp(),
        )

    def test_strip_off_millisecond_2(self):
        base = pytz.timezone("Asia/Seoul").localize(
            datetime.fromisoformat("2018-08-10T02:19:59.999")
        )

        cron_time = CronTime("0 */10 * * * *")
        x = cron_time.get_next(base, "Asia/Seoul")

        self.assertEqual(
            x.timestamp(),
            datetime.fromisoformat("2018-08-10T02:20:00.000")
            .astimezone(pytz.timezone("Asia/Seoul"))
            .timestamp(),
        )

    """
    NEXT VERIFICATION
        
    """

    def test_next_every_minute(self):
        cron_time = CronTime("* * * * *")
        min = 60
        previous_date = datetime(2018, 5, 3, 0, 0, tzinfo=pytz.timezone("UTC"))

        for idx in range(25):
            next_date = cron_time.get_next(previous_date, "UTC")
            self.assertEqual(next_date.timestamp(), previous_date.timestamp() + min)
            previous_date = next_date

    def test_next_15_mintue(self):
        cron_time = CronTime("*/15 * * * *")
        min = 60 * 15
        previous_date = datetime(2016, 6, 3, 0, 0, tzinfo=pytz.timezone("UTC"))

        for idx in range(25):
            next_date = cron_time.get_next(previous_date, "UTC")
            self.assertEqual(next_date.timestamp(), previous_date.timestamp() + min)
            previous_date = next_date

    def test_change_timezone_1(self):
        dd = datetime(2018, 10, 7)
        cron_time = CronTime("0 0 9 4 * *")
        next_date = cron_time.get_next(dd, "America/Sao_Paulo")
        self.assertEqual(
            next_date.timestamp(),
            datetime.fromisoformat("2018-11-04T09:00:00.000-02:00").timestamp(),
        )

    def test_change_timezone_2(self):
        curr_date = pytz.timezone("Asia/Amman").localize(
            datetime.fromisoformat("2018-10-25T23:00")
        )

        cron_time = CronTime("0 0 * * *")
        next_date = cron_time.get_next(curr_date, "Asia/Amman")
        expected_date = datetime.fromisoformat("2018-10-26T00:00+03:00")
        expected_date = expected_date.astimezone(pytz.timezone("Asia/Amman"))
        self.assertEqual(next_date - expected_date, timedelta(0))

    def test_crontimer_internal_1(self):
        curr_date = pytz.timezone("Asia/Amman").localize(
            datetime.fromisoformat("2018-10-25T23:00")
        )

        cron_time = CronTime("* * * * *")
        for idx in range(100):
            next_date = cron_time.get_next(curr_date, "Asia/Amman")
            self.assertEqual(next_date - curr_date, timedelta(seconds=60))

    def test_crontimer_internal_2(self):
        curr_date = pytz.timezone("Asia/Amman").localize(
            datetime.fromisoformat("2018-03-29T23:15")
        )

        cron_time = CronTime("30 0 * * 5")
        next_date = cron_time.get_next(curr_date, "Asia/Amman")
        self.assertEqual(next_date - curr_date, timedelta(seconds=60 * 45))

        curr_date = next_date
        next_date = cron_time.get_next(curr_date, "Asia/Amman")
        self.assertEqual(
            next_date - curr_date, timedelta(seconds=3600 * 24 * 7 - 60 * 30)
        )

        curr_date = next_date
        next_date = cron_time.get_next(curr_date, "Asia/Amman")
        self.assertEqual(next_date - curr_date, timedelta(seconds=3600 * 24 * 7))

    def test_crontimer_internal_3(self):
        curr_date = pytz.timezone("Asia/Amman").localize(
            datetime.fromisoformat("2018-03-29T23:45")
        )

        cron_time = CronTime("30 0 * * *")
        next_date = cron_time.get_next(curr_date, "Asia/Amman")
        self.assertEqual(next_date - curr_date, timedelta(seconds=60 * 15))

        curr_date = next_date
        next_date = cron_time.get_next(curr_date, "Asia/Amman")
        self.assertEqual(next_date - curr_date, timedelta(seconds=3600 * 24 - 60 * 30))

        curr_date = next_date
        next_date = cron_time.get_next(curr_date, "Asia/Amman")
        self.assertEqual(next_date - curr_date, timedelta(seconds=3600 * 24))

    def test_crontimer_internal_4(self):
        curr_date = pytz.timezone("Asia/Amman").localize(
            datetime.fromisoformat("2018-03-29T23:45")
        )

        cron_time = CronTime("30 * * * *")
        next_date = cron_time.get_next(curr_date, "Asia/Amman")
        self.assertEqual(next_date - curr_date, timedelta(seconds=60 * 15))

        curr_date = next_date
        next_date = cron_time.get_next(curr_date, "Asia/Amman")
        self.assertEqual(next_date - curr_date, timedelta(seconds=60 * 30))

        curr_date = next_date
        next_date = cron_time.get_next(curr_date, "Asia/Amman")
        self.assertEqual(next_date - curr_date, timedelta(seconds=3600))

    def test_crontimer_internal_5(self):
        curr_date = pytz.timezone("Asia/Amman").localize(
            datetime.fromisoformat("2018-03-29T23:59")
        )

        cron_time = CronTime("* * * * *")
        next_date = cron_time.get_next(curr_date, "Asia/Amman")
        self.assertEqual(next_date - curr_date, timedelta(seconds=60))

        curr_date = next_date
        next_date = cron_time.get_next(curr_date, "Asia/Amman")
        self.assertEqual(next_date - curr_date, timedelta(seconds=60))

    def test_crontimer_internal_6(self):
        cron_time = CronTime("* * * * *")
        tz = pytz.timezone("Asia/Amman")

        maybeBadDate = pytz.timezone("Asia/Amman").localize(
            datetime.fromisoformat("2018-03-31T00:59:00")
        )
        cron_time.find_previous_dst_jump(maybeBadDate, tz)

        with self.assertRaises(Exception):
            maybeBadDate = pytz.timezone("Asia/Amman").localize(
                datetime.fromisoformat("2018-03-31T01:00:00")
            )
            cron_time.find_previous_dst_jump(maybeBadDate, tz)

    def test_crontimer_internal_7(self):
        tz = pytz.timezone("Europe/Amsterdam")
        end_date = pytz.timezone("Europe/Amsterdam").localize(
            datetime.fromisoformat("2023-01-01T16:00:00")
        )

        start_date = end_date - timedelta(minutes=30, seconds=1)
        start_date = start_date.astimezone(tz)
        cron_time = CronTime("5 16 * * *")

        job_in_range = cron_time.check_time_in_skipped_range(start_date, end_date)
        self.assertEqual(job_in_range, False)

        end_date = end_date + timedelta(minutes=30)
        end_date = end_date.astimezone(tz)
        job_in_range = cron_time.check_time_in_skipped_range(start_date, end_date)
        self.assertEqual(job_in_range, True)

    def test_crontimer_internal_8(self):
        tz = pytz.timezone("Europe/Amsterdam")
        end_date = pytz.timezone("Europe/Amsterdam").localize(
            datetime.fromisoformat("2023-01-01T16:00:00")
        )

        start_date = end_date - timedelta(hours=1, seconds=1)
        start_date = start_date.astimezone(tz)
        cron_time = CronTime("1 5 16 * * *")

        job_in_range = cron_time.check_time_in_skipped_range(start_date, end_date)
        self.assertEqual(job_in_range, False)

        start_date = end_date - timedelta(hours=1, minutes=1, seconds=1)
        start_date = start_date.astimezone(tz)
        job_in_range = cron_time.check_time_in_skipped_range(start_date, end_date)
        self.assertEqual(job_in_range, False)

    def test_crontimer_internal_9(self):
        tz = pytz.timezone("Europe/Amsterdam")
        end_date = pytz.timezone("Europe/Amsterdam").localize(
            datetime.fromisoformat("2023-01-01T16:00:00")
        )

        start_date = end_date - timedelta(hours=1, seconds=1)
        start_date = start_date.astimezone(tz)
        cron_time = CronTime("5 16 * * *")

        job_in_range = cron_time.check_time_in_skipped_range(start_date, end_date)
        self.assertEqual(job_in_range, False)

        end_date = end_date + timedelta(hours=1)
        start_date = end_date - timedelta(hours=1, seconds=1)

        job_in_range = cron_time.check_time_in_skipped_range(start_date, end_date)
        self.assertEqual(job_in_range, True)

    def test_crontimer_internal_10(self):
        tz = pytz.timezone("Europe/Amsterdam")
        end_date = pytz.timezone("Europe/Amsterdam").localize(
            datetime.fromisoformat("2023-01-01T16:15:00")
        )

        start_date = end_date - timedelta(minutes=45, seconds=1)
        start_date = start_date.astimezone(tz)
        cron_time = CronTime("30 16 * * *")

        job_in_range = cron_time.check_time_in_skipped_range(start_date, end_date)
        self.assertEqual(job_in_range, False)

        end_date = end_date + timedelta(minutes=30)
        start_date = end_date - timedelta(minutes=50, seconds=1)

        job_in_range = cron_time.check_time_in_skipped_range(start_date, end_date)
        self.assertEqual(job_in_range, True)

    def test_crontimer_internal_11(self):
        tz = pytz.timezone("Europe/Amsterdam")
        end_date = pytz.timezone("Europe/Amsterdam").localize(
            datetime.fromisoformat("2023-01-01T16:15:00")
        )

        start_date = end_date - timedelta(hours=3, minutes=45, seconds=1)
        start_date = start_date.astimezone(tz)
        cron_time = CronTime("30 16 * * *")

        job_in_range = cron_time.check_time_in_skipped_range(start_date, end_date)
        self.assertEqual(job_in_range, False)

        end_date = end_date + timedelta(minutes=30)
        start_date = end_date - timedelta(hours=3, minutes=45, seconds=1)

        job_in_range = cron_time.check_time_in_skipped_range(start_date, end_date)
        self.assertEqual(job_in_range, True)

    def test_crontimer_internal_12(self):
        with self.assertRaises(Exception):
            cron_time = CronTime("30 16 * * *")
            cron_time.check_time_in_skipped_range_multi_hour(15, 0, 15, 30)

    def test_crontimer_internal_13(self):
        cron_time = CronTime("* * * * *")
        curr_date = datetime.now(tz=pytz.timezone("Asia/Seoul"))
        curr_date = curr_date.replace(second=0, microsecond=0)
        for idx in range(100):
            next_date = cron_time.get_next(curr_date, "Asia/Seoul")
            self.assertEqual(next_date - curr_date, timedelta(seconds=60))
            curr_date = next_date

    def test_crontimer_internal_14(self):
        cron_time = CronTime("0 0 9 * * *")
        curr_date = datetime.now(tz=pytz.timezone("UTC"))
        curr_date = curr_date.replace(hour=9, minute=0, second=0, microsecond=0)

        for idx in range(100):
            next_date = cron_time.get_next(curr_date, "UTC")
            self.assertEqual(next_date - curr_date, timedelta(seconds=60 * 60 * 24))
            curr_date = next_date

    def test_crontimer_internal_15(self):
        cron_time = CronTime("0 * * * *")
        curr_date = pytz.timezone("America/Sao_Paulo").localize(
            datetime.fromisoformat("2018-11-02T23:00")
        )
        curr_date = curr_date.replace(second=0, microsecond=0)

        for idx in range(25):
            next_date = cron_time.get_next(curr_date, "America/Sao_Paulo")
            self.assertEqual(next_date - curr_date, timedelta(seconds=60 * 60))
            curr_date = next_date

    def test_crontimer_internal_16(self):
        cron_time = CronTime("*/3 * * * *")
        curr_date = pytz.timezone("America/Sao_Paulo").localize(
            datetime.fromisoformat("2018-11-02T23:00")
        )
        curr_date = curr_date.replace(second=0, microsecond=0)

        for idx in range(25):
            next_date = cron_time.get_next(curr_date, "America/Sao_Paulo")
            self.assertEqual(next_date - curr_date, timedelta(seconds=60 * 3))
            curr_date = next_date

    def test_crontimer_internal_17(self):
        cron_time = CronTime("*/15 * * FEB *")
        prev_date = datetime(2018, 3, 31, 0, 0, tzinfo=pytz.timezone("Asia/Seoul"))
        next_date = cron_time.get_next(prev_date, "UTC")
        self.assertEqual(
            next_date.timestamp(),
            datetime(2019, 2, 1, 0, 0, tzinfo=pytz.utc).timestamp(),
        )

    def test_crontimer_internal_18(self):
        tz = pytz.timezone("Asia/Seoul")
        cron_time = CronTime("*/15 * * 6-11 *")
        prev_date_1 = datetime(2018, 3, 31, 0, 0, tzinfo=pytz.utc)
        prev_date_1 = prev_date_1.astimezone(tz)
        next_date_1 = cron_time.get_next(prev_date_1, "UTC")
        self.assertEqual(
            next_date_1.timestamp(),
            datetime(2018, 7, 1, 0, 0, tzinfo=pytz.utc).timestamp(),
        )

        prev_date_2 = datetime(2018, 8, 31, 0, 0, tzinfo=pytz.utc)
        prev_date_2 = prev_date_2.astimezone(tz)
        next_date_2 = cron_time.get_next(prev_date_2, "UTC")
        self.assertEqual(
            next_date_2.timestamp(),
            datetime(2018, 8, 31, 0, 15, tzinfo=pytz.utc).timestamp(),
        )

    def test_crontimer_internal_19(self):
        cron_time = CronTime("0 8 1 * 4")
        prev_date = datetime(2019, 4, 21, 9, 0, tzinfo=pytz.timezone("Asia/Seoul"))
        next_date = cron_time.get_next(prev_date, "UTC")
        self.assertEqual(
            next_date.timestamp(),
            datetime(2019, 4, 25, 8, 0, tzinfo=pytz.utc).timestamp(),
        )

    def test_crontimer_internal_20(self):
        cron_time = CronTime("0 8 1 * 4")
        prev_date = datetime(2019, 4, 26, 9, 0, tzinfo=pytz.timezone("Asia/Seoul"))
        next_date = cron_time.get_next(prev_date, "UTC")
        self.assertEqual(
            next_date.timestamp(),
            datetime(2019, 5, 1, 8, 0, tzinfo=pytz.utc).timestamp(),
        )

    def test_crontimer_internal_21(self):
        cron_time = CronTime("0 8 1 * 4")
        prev_date = datetime(2019, 8, 1, 7, 59, tzinfo=pytz.utc)
        prev_date = prev_date.astimezone(pytz.timezone("Asia/Seoul"))
        next_date = cron_time.get_next(prev_date, "UTC")
        self.assertEqual(
            next_date.timestamp(),
            datetime(2019, 8, 1, 8, 0, tzinfo=pytz.utc).timestamp(),
        )


if __name__ == "__main__":
    main()
