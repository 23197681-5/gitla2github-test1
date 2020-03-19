import os
import logging
import asyncio
from typing import List

import asyncpg
from violet import JobManager


async def my_function(ctx, a, b):
    print(a + b)
    await asyncio.sleep(0.5)
    ctx.set_start()
    await ctx.manager.set_job_state(ctx.job_id, {"note": "awoo"})
    state = await ctx.manager.fetch_job_state(ctx.job_id)
    assert state["note"] == "awoo"


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
        "my_queue",
        args=(int, int),
        handler=my_function,
        custom_start_event=True,
        workers=2,
    )

    to_watch: List[str] = []

    for num in range(8):

        async def creator(num):
            job_id = await sched.push_queue("my_queue", [num, num])
            to_watch.append(job_id)
            logging.info("created queue job %s with args %r", job_id, [num, num])

        loop.run_until_complete(creator(num))

    final_job_id = to_watch[-1]

    async def watcher():
        while True:
            statuses = {}
            for job_id in to_watch:
                status = await sched.fetch_queue_job_status(job_id)
                assert status is not None
                statuses[job_id] = status

            for job_id, status in statuses.items():
                if status.state == 3:
                    logging.error("job %s error %r", job_id, status.errors)
                else:
                    logging.info("job %s state %r", job_id, status.state)

            await asyncio.sleep(1)

    async def test_final_timeout_fail():
        try:
            await sched.wait_job(final_job_id, timeout=3)
        except asyncio.TimeoutError:
            print("======TEST TIMEOUT")

    async def test_start_timeout_fail():
        try:
            await sched.wait_job_start(final_job_id, timeout=4)
        except asyncio.TimeoutError:
            print("======TEST START TIMEOUT")

    async def signaler():
        await sched.wait_job_start(final_job_id)
        print("final job is at work!!")

        await sched.wait_job(final_job_id)
        print("final job finished!!!")

    loop.create_task(watcher())
    loop.create_task(signaler())
    loop.create_task(test_final_timeout_fail())
    loop.create_task(test_start_timeout_fail())
    try:
        loop.run_until_complete(asyncio.sleep(10))
    except KeyboardInterrupt:
        sched.stop_all()
        loop.run_until_complete(asyncio.sleep(2))


if __name__ == "__main__":
    main()
