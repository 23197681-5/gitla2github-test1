# violet: An asyncio background job library
# Copyright 2019, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

import logging
import asyncio
from typing import Dict

from .models import Queue, JobState
from .utils import fetch_with_json

log = logging.getLogger(__name__)


async def acquire_jobs(manager, conn, queue, rows):
    tasks: Dict[str, asyncio.Task] = {}

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

    return tasks


async def release_tasks(conn, tasks: Dict[str, asyncio.Task]):
    for job_id, task in tasks.items():
        assert task.done()

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
            SET state = $1,
                errors = $2
            WHERE job_id = $3
            """,
            new_state,
            new_error,
            job_id,
        )


async def fetch_jobs(conn, queue: Queue, state=JobState.NotTaken) -> list:
    log.debug("querying state=%r for queue %r", state, queue.name)
    return await fetch_with_json(
        conn,
        f"""
        SELECT job_id, args
        FROM violet_jobs
        WHERE queue = $1 AND state = $2
        ORDER BY inserted_at
        LIMIT {queue.takes}
        """,
        queue.name,
        state,
    )


class StopQueueWorker(Exception):
    pass


async def run_jobs(manager, queue, state: JobState = JobState.NotTaken):
    rows = await fetch_jobs(manager.db, queue, state=state)
    log.debug("queue %r got %d jobs in state %r", queue.name, len(rows), state)

    async with manager.db.acquire() as conn:
        async with conn.transaction():
            tasks = await acquire_jobs(manager, conn, queue, rows)

    if not tasks:
        log.debug("queue %r is empty, work stop", queue.name)
        raise StopQueueWorker()

    done, pending = await asyncio.wait(tasks.values())
    assert not pending

    async with manager.db.acquire() as conn:
        async with conn.transaction():
            await release_tasks(conn, tasks)


async def queue_worker(manager, queue: Queue):
    await run_jobs(manager, queue, JobState.Taken)

    while True:
        await run_jobs(manager, queue)
        await asyncio.sleep(queue.period)
