import asyncio
import threading
import random


def thr(i):
    # we need to create a new loop for the thread, and set it as the 'default'
    # loop that will be returned by calls to asyncio.get_event_loop() from this
    # thread.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(do_stuff(i))
    loop.close()


async def do_stuff(i):
    await asyncio.sleep(random.uniform(0.1, 0.5))  # NOTE if we hadn't called
    # asyncio.set_event_loop() earlier, we would have to pass an event
    # loop to this function explicitly.
    print(i)


def main():
    num_threads = 10
    threads = [threading.Thread(target=thr, args=(i,)) for i in range(num_threads)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    print("bye")


def sum(max):
    tot = 0
    for i in range(max):
        tot += i
        yield tot
    return tot


def coroutine():
    x = yield from sum(2)
    print("Total: {}".format(x))


def test():
    yield 1
    return 10


if __name__ == "__main__":
    # main()
    x = coroutine()
    print(next(x))
    print(next(x))
    print(next(x))
