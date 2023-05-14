import sched, time
from event_scheduler import EventScheduler

s = sched.scheduler(time.time, time.sleep)


def print_time(a="default"):
    print("From print_time", time.time(), a)


def print_some_times():
    print(time.time())
    s.enter(10, 1, print_time)
    s.enter(5, 2, print_time, argument=("positional",))
    # despite having higher priority, 'keyword' runs after 'positional' as enter() is relative
    s.enter(5, 1, print_time, kwargs={"a": "keyword"})
    s.enterabs(1_650_000_000, 10, print_time, argument=("first enterabs",))
    s.enterabs(1_650_000_000, 5, print_time, argument=("second enterabs",))
    s.run(False)
    print(time.time())


def print_time(a="default"):
    print("From print_time", time.time(), a)


if __name__ == "__main__":
    # test sched
    print_some_times()

    # test event scheduler
    scheduler = EventScheduler("scheduled_jobs")
    scheduler.start()
    scheduler.enter_recurring(1, 0, print_time)
    while True:
        print_some_times()
        time.sleep(30000)
