# violet: An asyncio background job library
# Copyright 2019, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

import logging
import asyncio
from typing import Dict, List

from .models import Queue, JobState
from .utils import fetch_with_json

log = logging.getLogger(__name__)


async def _create_tasks(manager, conn, queue, rows):
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


async def _process_waited_tasks(conn, tasks: Dict[str, asyncio.Task]):
    for job_id, task in tasks.items():
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


async def _resume_queue():
    pass


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


async def queue_worker(manager, queue: Queue):
    # TODO fetch jobs with state = 1 (basic recovery)
    # and work on them before main loop

    while True:
        # TODO wrap in try/finally and make actual fetch be in a worker_tick
        # TODO determine ^ based if we actually need a finally block
        rows = await fetch_jobs(manager.db, queue)
        log.debug("queue %r: got %d jobs", queue.name, len(rows))

        async with manager.db.acquire() as conn:
            async with conn.transaction():
                tasks = await _create_tasks(manager, conn, queue, rows)

        if not tasks:
            log.debug("queue %r is empty, work stop", queue.name)
            return

        done, pending = await asyncio.wait(tasks.values())

        async with manager.db.acquire() as conn:
            async with conn.transaction():
                await _process_waited_tasks(conn, tasks)

        await asyncio.sleep(queue.period)
