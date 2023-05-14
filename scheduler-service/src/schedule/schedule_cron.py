import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pytz


CONSTRAINTS = [[0, 59], [0, 59], [0, 23], [1, 31], [0, 11], [0, 6]]

# support leap year...not perfect
MONTH_CONSTRAINTS = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

PARSE_DEFAULTS = ["0", "*", "*", "*", "*", "*"]

ALIASES = {
    "jan": 0,
    "feb": 1,
    "mar": 2,
    "apr": 3,
    "may": 4,
    "jun": 5,
    "jul": 6,
    "aug": 7,
    "sep": 8,
    "oct": 9,
    "nov": 10,
    "dec": 11,
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
}

TIME_UNITS = ["second", "minute", "hour", "dayOfMonth", "month", "dayOfWeek"]
TIME_UNITS_LEN = len(TIME_UNITS)

PRESETS = {
    "@yearly": "0 0 0 1 0 *",
    "@monthly": "0 0 0 1 * *",
    "@weekly": "0 0 0 * * 0",
    "@daily": "0 0 0 * * *",
    "@hourly": "0 0 * * * *",
    "@minutely": "0 * * * * *",
    "@secondly": "* * * * * *",
    "@weekdays": "0 0 0 * * 1-5",
    "@weekends": "0 0 0 * * 0,6",
}

RE_WILDCARDS = r"\*"  # global
RE_RANGE = r"^(\d+)(?:-(\d+))?(?:\/(\d+))?$"  # global


class CronTime:
    def __init__(self, source, zone="UTC", utcOffset=0):
        self.source = source

        if zone:
            dt = datetime.now(pytz.timezone(zone))
            if type(dt) == datetime:
                self.zone = zone

        if utcOffset:
            self.utcOffset = utcOffset

        self.time_units = dict()
        for time_unit in TIME_UNITS:
            self.time_units[time_unit] = {}

        if type(self.source) == datetime:
            self.real_date = True
        else:
            self.parse(self.source)
            self.verify_parse()

    def parse(self, source: str):
        source = source.lower()

        if source in PRESETS:
            source = PRESETS[source]

        source = re.sub(r"[a-z]{1,3}", lambda x: str(ALIASES[x.group()]), source)

        source = source.strip()
        units = re.split(r"\s+", source)

        if len(units) < TIME_UNITS_LEN - 1:
            raise Exception("Too few fields")

        if len(units) > TIME_UNITS_LEN:
            raise Exception("Too many fields")

        units_len = len(units)

        for idx in range(TIME_UNITS_LEN):
            cur = (
                units[idx - (TIME_UNITS_LEN - units_len)]
                if idx - (TIME_UNITS_LEN - units_len) >= 0
                else PARSE_DEFAULTS[idx]
            )
            self.parseField(cur, TIME_UNITS[idx], CONSTRAINTS[idx])

    def parseFieldDetails(self, parsed_str, groups, low, high, value, type_obj):
        (lower, upper, step) = groups

        lower = int(lower)
        upper = int(upper) if upper else None
        was_step_defined = False if step is None else True

        if step == "0":
            raise Exception(f"Field ({type}) has a step of zero")
        step = int(step) if step else 1

        if upper and lower > upper:
            raise Exception(f"Field ({type}) has an invalid range")

        out_of_range_error = (
            (lower < low) or (upper and upper > high) or (not upper and lower > high)
        )
        if out_of_range_error:
            raise Exception(f"Field value ({value}) is out of range")

        lower = min(max(low, int(abs(lower))), high)

        if upper:
            upper = min(high, int(abs(upper)))
        else:
            upper = high if was_step_defined else lower

        pointer = lower

        # describe do-while
        type_obj[pointer] = True
        pointer += step
        while pointer <= upper:
            type_obj[pointer] = True
            pointer += step

    def parseField(self, value, field_type, constraints):
        type_obj = self.time_units[field_type]
        low = constraints[0]
        high = constraints[1]

        fields = value.split(",")
        for field in fields:
            wildcard_indx = field.find("*")
            if wildcard_indx != -1 and wildcard_indx != 0:
                raise Exception(f"Field ({field}) has an invalid wildcard expression")

        value = re.sub(RE_WILDCARDS, f"{low}-{high}", value)

        all_ranges = value.split(",")

        for idx in range(len(all_ranges)):
            if re.match(RE_RANGE, all_ranges[idx]):
                re.sub(
                    RE_RANGE,
                    lambda x: self.parseFieldDetails(
                        all_ranges[idx], x.groups(), low, high, value, type_obj
                    ),
                    all_ranges[idx],
                )
            else:
                raise Exception(f"Field ({field_type}) cannot be parsed")

    def verify_parse(self):
        months = list(self.time_units["month"].keys())
        dom = list(self.time_units["dayOfMonth"].keys())
        ok = False

        last_wrong_month = None
        for idx in range(len(months)):
            m = months[idx]
            con = MONTH_CONSTRAINTS[int(m)]

            for jj in range(len(dom)):
                day = dom[jj]
                if day <= con:
                    ok = True

            if not ok:
                last_wrong_month = m
                print(f"Month '{m}' is limited to '{con}' days.")

        if not ok:
            not_ok_con = MONTH_CONSTRAINTS[int(last_wrong_month)]
            for kk in range(len(dom)):
                not_ok_day = dom[kk]
                if not_ok_day > not_ok_con:
                    self.time_units["dayOfMonth"].pop(not_ok_day)
                    fixed_day = int(not_ok_day) % not_ok_con
                    self.time_units["dayOfMonth"][fixed_day] = True

    # _getNextDateFrom function
    def get_next(self, start, zone="UTC"):
        assert type(start) == datetime

        """
        TODO:
        여기에서 입력되는 시작 시간과 타임존은 의미가 필요하다.
        본 구현에서는 시작 시간은 해당 타임존의 시간을 의미하며 별도의 변환이 필요하지 않다.
        전체 구현에서 이러한 타임존 변환에 대한 기능을 검토해야 한다.

        또한, start는 aware인지 naive인지가 중요하다.
        start가 aware라면 zone을 적용하지 않는다는 의미이며,
        naive의 경우에만 zone의 입력값이 의미를 가진다.

        여기에서, 타임존을 변경한다는 의미로 사용할 수 있다고 판단할 수도 있는데,
        이 경우, naive 타입의 경우 기능 구현이 모호해질 수 있다.
        """

        tz = pytz.timezone(zone)
        if start.tzinfo is None or start.tzinfo.utcoffset(start) is None:
            start = tz.localize(start, is_dst=None)

        date = start.astimezone(pytz.timezone(zone))

        """
        TODO: check if 'toMillis()' of node.js is like timestamp() of python
        """
        first_date = int(date.timestamp() * 1000.0)

        if date.microsecond > 0:
            date += timedelta(seconds=1)
            date = date.astimezone(tz)
            date = date.replace(microsecond=0)

        # CHECK: need to check if date is available datetime here???

        timeout = datetime.now() + timedelta(seconds=5)

        while True:
            diff = date - start

            # if datetime.now() > timeout:
            #     raise Exception(
            #         f"""
            #     Something went wrong. It took over five seconds to find the next execution time for the cron job.
            # 	Please refer to the canonical issue (https://github.com/kelektiv/node-cron/issues/467) and provide the following string if you would like to help debug:
            # 	Time Zone: {zone or '""'} - Cron String: {self.source} - UTC offset: ${date.astimezone}
            # 	- current Date: {datetime.now()}
            #     """
            #     )

            if (
                not date.month - 1 in self.time_units["month"]
                and len(self.time_units["month"].keys()) != 12
            ):
                date += relativedelta(months=1)
                date = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

                if self.fowardDSTJump(0, 0, date):
                    (done, new_date) = self.find_previous_dst_jump(date, tz)
                    date = new_date
                    if done:
                        break
                continue

            if (
                not (date.day in self.time_units["dayOfMonth"])
                and len(self.time_units["dayOfMonth"].keys()) != 31
                and not (
                    date.isoweekday() in self.time_units["dayOfWeek"]
                    and len(self.time_units["dayOfWeek"].keys()) != 7
                )
            ):
                date = date + timedelta(days=1)
                date = date.replace(hour=0, minute=0, second=0)
                date = date.astimezone(tz)

                if self.fowardDSTJump(0, 0, date):
                    (done, new_date) = self.find_previous_dst_jump(date, tz)
                    date = new_date
                    if done:
                        break
                continue

            if (
                not date.isoweekday() in self.time_units["dayOfWeek"]
                and len(self.time_units["dayOfWeek"].keys()) != 7
                and not (
                    date.day in self.time_units["dayOfMonth"]
                    and len(self.time_units["dayOfMonth"].keys()) != 31
                )
            ):
                date = date + timedelta(days=1)
                date = date.replace(hour=0, minute=0, second=0)
                date = date.astimezone(tz)
                if self.fowardDSTJump(0, 0, date):
                    (done, new_date) = self.find_previous_dst_jump(date, tz)
                    date = new_date
                    if done:
                        break
                continue

            if (
                not date.hour in self.time_units["hour"]
                and len(self.time_units["hour"].keys()) != 24
            ):
                expected_hour = (
                    0
                    if (date.hour == 23 and diff.seconds > 86400000)
                    else date.hour + 1
                )
                expected_minute = date.minute

                date = date.replace(hour=0)
                date = date + timedelta(hours=expected_hour)
                date = date.replace(minute=0, second=0)
                date = date.astimezone(tz)

                if self.fowardDSTJump(expected_hour, expected_minute, date):
                    (done, new_date) = self.find_previous_dst_jump(date, tz)
                    date = new_date
                    if done:
                        break
                continue

            if (
                not date.minute in self.time_units["minute"]
                and len(self.time_units["minute"].keys()) != 60
            ):
                expected_minute = (
                    0
                    if date.minute == 59 and diff.seconds > 3600000
                    else date.minute + 1
                )
                expected_hour = date.hour + (1 if expected_minute == 60 else 0)

                date = date.replace(minute=0)
                date = date + timedelta(minutes=expected_minute)
                date = date.replace(second=0)
                date = date.astimezone(tz)

                if self.fowardDSTJump(expected_hour, expected_minute, date):
                    (done, new_date) = self.find_previous_dst_jump(date, tz)
                    date = new_date
                    if done:
                        break
                continue

            if (
                not date.second in self.time_units["second"]
                and len(self.time_units["second"].keys()) != 60
            ):
                expected_second = (
                    0 if date.second == 59 and diff.seconds > 60000 else date.second + 1
                )
                expected_minute = date.minute + (expected_second == 60)
                expected_hour = date.hour + (1 if expected_minute == 60 else 0)

                date = date.replace(second=0)
                date += timedelta(seconds=expected_second)
                date = date.astimezone(tz)

                if self.fowardDSTJump(expected_hour, expected_minute, date):
                    (done, new_date) = self.find_previous_dst_jump(date, tz)
                    date = new_date
                    if done:
                        break
                continue

            if int(date.timestamp() * 1000.0) == first_date:
                expected_second = date.second + 1
                expected_minute = date.minute + (expected_second == 60)
                expected_hour = date.hour + (1 if expected_minute == 60 else 0)

                date = date.replace(second=0)
                date = date + timedelta(seconds=expected_second)
                date = date.astimezone(tz)

                # same as always
                if self.fowardDSTJump(expected_hour, expected_minute, date):
                    (done, new_date) = self.find_previous_dst_jump(date.tz)
                    date = new_date
                    if done:
                        break
                continue

            break

        return date

    def fowardDSTJump(self, expected_hour, expected_minute, actual_date):
        actual_hour = actual_date.hour
        actual_mintue = actual_date.minute

        hours_jumped = expected_hour % 24 < actual_hour
        minute_jumped = expected_minute % 60 < actual_mintue

        return hours_jumped or minute_jumped

    def find_previous_dst_jump(self, date: datetime, tz):
        maybe_jumping_point = date

        iteration_limit = 60 * 24
        iteration = 0

        expected_minitue = None
        actual_minute = None
        expected_hour = None
        actual_hour = None

        while (iteration == 0) or (
            expected_minitue == actual_minute and expected_hour == actual_hour
        ):
            iteration += 1
            if iteration > iteration_limit:
                raise Exception(
                    f"ERROR: This DST checking related function assumes the input DateTime ({date}) is within 24 hours of a DST jump."
                )

            expected_minitue = maybe_jumping_point.minute - 1
            expected_hour = maybe_jumping_point.hour

            if expected_minitue < 0:
                expected_minitue += 60
                expected_hour = (expected_hour + 24 - 1) % 24

            maybe_jumping_point = maybe_jumping_point - timedelta(minutes=1)
            maybe_jumping_point = maybe_jumping_point.astimezone(tz)

            actual_minute = maybe_jumping_point.minute
            actual_hour = maybe_jumping_point.hour

        after_jumping_point = maybe_jumping_point + timedelta(minutes=1)
        after_jumping_point = after_jumping_point.astimezone(tz)
        after_jumping_point = after_jumping_point.replace(second=0, microsecond=0)

        before_jumping_point = after_jumping_point - timedelta(seconds=1)
        before_jumping_point = before_jumping_point.astimezone(tz)

        if (
            date.month in self.time_units["month"]
            and date.day in self.time_units["dayOfMonth"]
            and date.isoweekday() in self.time_units["dayOfWeek"]
        ):
            return (
                self.check_time_in_skipped_range(
                    before_jumping_point, after_jumping_point
                ),
                after_jumping_point,
            )

        return (False, after_jumping_point)

    def check_time_in_skipped_range(self, before_jumping_point, after_jumping_point):
        starting_minute = (before_jumping_point.minute + 1) % 60
        startig_hour = (before_jumping_point.hour + (starting_minute == 0)) % 24

        hour_range_size = after_jumping_point.hour - startig_hour + 1
        is_hour_jump = (starting_minute == 0) and (after_jumping_point.minute == 0)

        if hour_range_size == 2 and is_hour_jump:
            return startig_hour in self.time_units["hour"]
        elif hour_range_size == 1:
            return startig_hour in self.time_units[
                "hour"
            ] and self.check_time_in_skipped_range_single_hour(
                starting_minute, after_jumping_point.minute
            )
        else:
            return self.check_time_in_skipped_range_multi_hour(
                startig_hour,
                starting_minute,
                after_jumping_point.hour,
                after_jumping_point.minute,
            )

    def check_time_in_skipped_range_single_hour(self, start_minute, end_minute):
        for minute in range(start_minute, end_minute):
            if minute in self.time_units["minute"]:
                return True
        return (
            end_minute in self.time_units["minute"] and 0 in self.time_units["second"]
        )

    def select_range(self, for_hour, start_hour, end_hour, start_minute, end_minute):
        first_hour_minute_range: list = [xx for xx in range(start_minute, 60)]
        last_hour_minute_range: list = [xx for xx in range(0, end_minute)]
        middle_hour_minute_range: list = [xx for xx in range(0, 60)]

        if for_hour == start_hour:
            return first_hour_minute_range
        elif for_hour == end_hour:
            return last_hour_minute_range
        else:
            return middle_hour_minute_range

    def check_time_in_skipped_range_multi_hour(
        self, start_hour, start_minute, end_hour, end_minute
    ):
        if start_hour >= end_hour:
            raise Exception(
                f"ERROR: This DST checking related function assumes the forward jump starting hour ({start_hour}) is less than the end hour ({end_hour})"
            )

        for hour in range(start_hour, end_hour + 1):
            if not hour in self.time_units["hour"]:
                continue

            using_range = self.select_range(
                hour, start_hour, end_hour, start_minute, end_minute
            )

            for minute in using_range:
                if minute in self.time_units["minute"]:
                    return True

        return (
            end_hour in self.time_units["hour"]
            and end_minute in self.time_units["minute"]
            and 0 in self.time_units["second"]
        )


if __name__ == "__main__":
    cron_time = CronTime("0 * * * * *", "Asia/Seoul", None)
    next_datetime = cron_time.get_next(datetime(2018, 6, 3, 0, 0), "Asia/Seoul")
    print(next_datetime)

    cron_time = CronTime("0 0 */4 * * *", "Asia/Seoul", None)
    next_datetime = cron_time.get_next(datetime(2018, 6, 3, 0, 0), "Asia/Seoul")
    print(next_datetime)

    base = datetime.fromisoformat("2018-03-29T23:15")

    cron_time = CronTime("30 0 * * 5", "Asia/Amman", None)
    next_datetime = cron_time.get_next(base, "Asia/Amman")
    print(next_datetime)

    base = next_datetime
    next_datetime = cron_time.get_next(base, "Asia/Amman")
    print(next_datetime)

    base = next_datetime
    next_datetime = cron_time.get_next(base, "Asia/Amman")
    print(next_datetime)

    cron_time = CronTime("08 * * * * *")
