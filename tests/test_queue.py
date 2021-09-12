import logging
import asyncio
from typing import List, Tuple

import violet
from hail import Flake


class ExampleJobQueue(violet.JobQueue[Tuple[int, int]]):
    name = "example_queue"
    workers = 2

    # map the arguments when pushing to the columns in the
    # job queue's table
    args = ("number_a", "number_b")

    @classmethod
    def map_persisted_row(cls, row):
        return row["number_a"], row["number_b"]

    @classmethod
    async def push(cls, a: int, b: int, **kwargs) -> Flake:
        return await cls._sched.raw_push(cls, (a, b), **kwargs)

    @classmethod
    async def setup(_, ctx):
        await asyncio.sleep(0.5)

    @classmethod
    async def handle(cls, ctx):
        a, b = ctx.args
        await cls.set_job_state(ctx.job_id, {"note": "awoo", "a": a, "b": b})
        state = await cls.fetch_job_state(ctx.job_id)
        assert state["note"] == "awoo"
        assert state["a"] == a
        assert state["b"] == b


async def fetch_all_statuses(job_ids):
    # map job ids list to job id statuses dict
    job_statuses = {
        job_id: (await ExampleJobQueue.fetch_job_status(job_id)) for job_id in job_ids
    }
    assert not any(v is None for v in job_statuses.values())
    return job_statuses


def test_job_queues(sched, event_loop):
    sched.register_job_queue(ExampleJobQueue)

    job_ids: List[str] = []

    for num in range(8):
        job_id = event_loop.run_until_complete(ExampleJobQueue.push(num, num))
        job_ids.append(job_id)
        logging.info("created queue job %s with args %r", job_id, [num, num])

    final_job_id = job_ids[-1]

    # keep track of all created jobs, stop the test once its all done!
    async def watcher():
        while True:
            job_statuses = await fetch_all_statuses(job_ids)

            # if all jobs finished up either on completed or error, return
            if all(
                status.state in (violet.JobState.Completed, violet.JobState.Error)
                for status in job_statuses.values()
            ):
                return

            await asyncio.sleep(0.8)

    async def inner_test_final_timeout_fail():
        try:
            await ExampleJobQueue.wait_job(final_job_id, timeout=3)
            assert False  # the final job finished too quickly
        except asyncio.TimeoutError:
            print("======TEST TIMEOUT")

    async def inner_test_start_timeout_fail():
        try:
            await sched.wait_job_start(final_job_id, timeout=4)
            assert False  # the final job finished too quickly
        except asyncio.TimeoutError:
            print("======TEST START TIMEOUT")

    async def inner_test_wait_final_job():
        await sched.wait_job_start(final_job_id)
        await ExampleJobQueue.wait_job(final_job_id)

        status = await ExampleJobQueue.fetch_job_status(final_job_id)
        assert status is not None
        assert status.state == violet.JobState.Completed

    event_loop.create_task(inner_test_wait_final_job())
    event_loop.create_task(inner_test_final_timeout_fail())
    event_loop.create_task(inner_test_start_timeout_fail())

    try:
        event_loop.run_until_complete(watcher())
    finally:
        event_loop.run_until_complete(sched.stop_all())

    # assert everyone is finished up
    job_statuses_after_run = event_loop.run_until_complete(fetch_all_statuses(job_ids))
    assert all(
        status.state == violet.JobState.Completed
        for status in job_statuses_after_run.values()
    )
