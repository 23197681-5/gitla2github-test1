# violet: An asyncio background job library
# Copyright 2019, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

import logging
import asyncio
import traceback
from typing import Dict

from .models import Queue, JobState, QueueJobContext
from .utils import fetch_with_json

log = logging.getLogger(__name__)


async def acquire_jobs(manager, conn, queue, rows):
    tasks: Dict[str, asyncio.Task] = {}

    for row in rows:
        job_id = row["job_id"]
        ctx = QueueJobContext(manager, job_id)
        task = manager.loop.create_task(queue.function(ctx, *row["args"]))
        tasks[job_id] = task
        await conn.execute(
            """
            UPDATE violet_jobs
            SET state = 1, taken_at = (now() at time zone 'utc')
            WHERE job_id = $1
            """,
            job_id,
        )

    return tasks


async def release_tasks(manager, conn, tasks: Dict[str, asyncio.Task]):
    for job_id, task in tasks.items():
        assert task.done()

        if job_id in manager.events:
            manager.events[job_id].set()

        new_state = JobState.Completed
        new_error = ""

        try:
            task.result()
        except Exception:
            new_state = JobState.Error
            new_error = traceback.format_exc()
            log.error("error on job %r, '%s'", job_id, new_error)

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


async def fetch_jobs(
    conn, queue: Queue, state=JobState.NotTaken, scheduled_only: bool = False
) -> list:
    log.debug("querying state=%r for queue %r", state, queue.name)

    scheduled_where = (
        "AND (now() at time zone 'utc') >= scheduled_at" if scheduled_only else ""
    )
    return await fetch_with_json(
        conn,
        f"""
        SELECT job_id, args
        FROM violet_jobs
        WHERE
            queue = $1
        AND state = $2
        {scheduled_where}
        ORDER BY inserted_at
        LIMIT {queue.takes}
        """,
        queue.name,
        state,
    )


class StopQueueWorker(Exception):
    pass


async def run_jobs(
    manager,
    queue,
    state: JobState = JobState.NotTaken,
    *,
    raise_on_empty: bool = True,
    scheduled_only: bool = False,
):
    rows = await fetch_jobs(
        manager.db, queue, state=state, scheduled_only=scheduled_only
    )
    log.debug("queue %r got %d jobs in state %r", queue.name, len(rows), state)

    async with manager.db.acquire() as conn:
        async with conn.transaction():
            tasks = await acquire_jobs(manager, conn, queue, rows)

    if not tasks:
        actually_empty = not await manager.db.fetchval(
            """
            SELECT count(*) > 0
            FROM violet_jobs
            WHERE queue = $1
              AND (now() at time zone 'utc') <= scheduled_at
            """,
            queue.name,
        )
        log.debug(
            "queue %r without jobs, is it full empty? %r", queue.name, actually_empty
        )

        if raise_on_empty and actually_empty:
            raise StopQueueWorker()
        else:
            return

    done, pending = await asyncio.wait(tasks.values())
    assert not pending

    async with manager.db.acquire() as conn:
        async with conn.transaction():
            await release_tasks(manager, conn, tasks)


async def queue_worker(manager, queue: Queue):
    if queue.start_existing_jobs:
        await run_jobs(manager, queue, JobState.Taken, raise_on_empty=False)

    while True:
        await run_jobs(manager, queue, scheduled_only=True)
        await asyncio.sleep(queue.period)
