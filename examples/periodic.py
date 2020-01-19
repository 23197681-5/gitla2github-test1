import asyncio
from violet import JobManager


async def my_function(a, b):
    print(a + b)


def main():
    loop = asyncio.get_event_loop()
    sched = JobManager(loop=loop)
    task = sched.spawn_periodic(my_function, [2, 2], period=1, name="my_function")
    loop.run_until_complete(task)


if __name__ == "__main__":
    main()
