import os
import logging
import asyncio
from typing import List

import asyncpg
from violet import JobManager


async def my_function(a, b):
    # TODO push job_id to all handlers
    # TODO update internal state on job
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
        "my_queue", args=(int, int), handler=my_function, takes=2, period=1
    )

    to_watch: List[str] = []

    for num in range(8):

        async def creator(num):
            job_id = await sched.push_queue("my_queue", [num, num])
            to_watch.append(job_id)
            logging.info("created queue job %r with args %r", job_id, [num, num])

        loop.run_until_complete(creator(num))

    async def watcher():
        while True:
            statuses = {}
            for job_id in to_watch:
                status = await sched.fetch_queue_job_status(job_id)
                statuses[job_id] = status

            for job_id, status in statuses.items():
                logging.info("job %r status %r", job_id, status.state)

            await asyncio.sleep(1)

    loop.create_task(watcher())
    loop.run_until_complete(asyncio.sleep(10))


if __name__ == "__main__":
    main()
