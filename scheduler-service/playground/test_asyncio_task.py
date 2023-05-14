# SuperFastPython.com
# example of creating a task with loop.create_task()
import asyncio

# define a coroutine for a task
async def task_coroutine():
    # report a message
    print("executing the task")
    # block for a moment
    await asyncio.sleep(1)
    print("the task is done")


# custom coroutine
async def main():
    # report a message
    print("main coroutine started")
    # get the current event loop
    loop = asyncio.get_event_loop()
    # create and schedule the task
    task = loop.create_task(task_coroutine())
    # wait for the task to complete
    await task
    # report a final message
    print("main coroutine done")


# start the asyncio program
asyncio.run(main())
print("done")
