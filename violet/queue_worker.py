import logging
import asyncio
from typing import Dict
from .models import Queue, JobState
from .utils import fetch_with_json


async def queue_worker(manager, queue: Queue):
    log = logging.getLogger(f"violet.{queue.name}")
    # TODO fetch jobs with state = 1 and work on them before main loop

    while True:
        # TODO select columns instead of *, etc
        # TODO wrap in try/finally and make actual fetch be in a worker_tick
        rows = await fetch_with_json(
            manager.pool,
            f"""
            SELECT job_id, args
            FROM violet_jobs
            WHERE queue = $1
            ORDER BY inserted_at
            LIMIT {queue.takes}
            """,
            queue.name,
        )
        log.debug("got %d jobs", len(rows))

        # TODO update all their state to 1

        tasks: Dict[str, asyncio.Task] = {}

        async with manager.pool.acquire() as conn:
            async with conn.transaction():

                for row in rows:
                    job_id = row["job_id"]
                    task = manager.loop.create_task(queue.function(*row["args"]))
                    tasks[job_id] = task
                    await conn.execute(
                        """
                        UPDATE violet_jobs
                        SET state = 1
                        WHERE job_id = $1
                        """,
                        job_id,
                    )

        done, pending = await asyncio.wait(tasks.values())

        async with manager.pool.acquire() as conn:
            async with conn.transaction():

                for job_id, task in tasks:
                    new_state = JobState.Completed
                    new_error = ""

                    exception = task.exception()
                    if exception is not None:
                        new_state = JobState.Error

                        # TODO make this a traceback
                        new_error = repr(exception)

                    await conn.execute(
                        """
                        UPDATE violet_jobs
                        SET state = $1
                            errors = $2
                        WHERE job_id = $3
                        """,
                        new_state,
                        new_error,
                        job_id,
                    )

        await asyncio.sleep(queue.period)
