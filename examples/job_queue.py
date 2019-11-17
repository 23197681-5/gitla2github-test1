import os
import logging
import asyncio

import asyncpg
from violet import JobManager


async def my_function(a, b):
    print(a + b)


def main():
    logging.basicConfig(level=logging.DEBUG)

    loop = asyncio.get_event_loop()
    sched = JobManager(
        loop=loop,
        db=loop.run_until_complete(
            asyncpg.create_pool(
                host="localhost",
                port="5432",
                user=os.getenv("PSQL_USER"),
                password=os.getenv("PSQL_PASS"),
                database=os.getenv("PSQL_DB"),
            )
        ),
    )
    sched.create_job_queue(
        "my_function", args=(int, int), handler=my_function, takes=1, period=1
    )

    for num in range(5):
        loop.create_task(sched.push_queue("my_function", [num, num]))

    loop.run_until_complete(asyncio.sleep(10))


if __name__ == "__main__":
    main()
