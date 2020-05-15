import os
import logging
import asyncio
from typing import List, Tuple

import asyncpg
import violet
from hail import Flake


class ExampleJobQueue(violet.JobQueue[Tuple[int, int]]):
    queue_name = "example_queue"
    workers = 2

    # map the arguments when pushing to the columns in the
    # job queue's table
    args = ("number_a", "number_b")

    @classmethod
    def create_args(cls, row):
        return row["number_a"], row["number_b"]

    @classmethod
    async def push(cls, a: int, b: int, **kwargs) -> Flake:
        return await cls._sched.raw_push(cls, (a, b), **kwargs)

    @classmethod
    async def setup(_, ctx):
        await asyncio.sleep(0.5)

    @classmethod
    async def handle(_, ctx):
        a, b = ctx.args
        print(a, b)
        await ctx.manager.set_job_state(ctx.job_id, {"note": "awoo"})
        state = await ctx.manager.fetch_job_state(ctx.job_id)
        assert state["note"] == "awoo"


def main():
    logging.basicConfig(level=logging.DEBUG)

    loop = asyncio.get_event_loop()
    sched = violet.JobManager(
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

    sched.register_job_queue(ExampleJobQueue)

    to_watch: List[str] = []

    for num in range(8):

        async def creator(num):
            job_id = await ExampleJobQueue.push(num, num)
            to_watch.append(job_id)
            logging.info("created queue job %s with args %r", job_id, [num, num])

        loop.run_until_complete(creator(num))

    final_job_id = to_watch[-1]

    # keep showing the state of the jobs we created every second
    async def watcher():
        while True:
            statuses = {}
            for job_id in to_watch:
                status = await ExampleJobQueue.fetch_job_status(job_id)
                assert status is not None
                statuses[job_id] = status

            for job_id, status in statuses.items():
                if status.state == 3:
                    logging.error("job %s error %r", job_id, status.errors)
                else:
                    logging.info("job %s state %r", job_id, status.state)

            await asyncio.sleep(1)

    # test that the final job id didn't execute in 5 secs
    async def test_final_timeout_fail():
        try:
            await ExampleJobQueue.wait_job(final_job_id, timeout=3)
        except asyncio.TimeoutError:
            print("======TEST TIMEOUT")

    # test that the final job didn't start on time
    async def test_start_timeout_fail():
        try:
            await sched.wait_job_start(final_job_id, timeout=4)
        except asyncio.TimeoutError:
            print("======TEST START TIMEOUT")

    # test waiting for start and end of the job
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
        loop.run_until_complete(sched.stop_all())
    finally:
        loop.run_until_complete(sched.stop_all())


if __name__ == "__main__":
    main()
