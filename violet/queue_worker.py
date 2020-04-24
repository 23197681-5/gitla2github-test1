# violet: An asyncio background job library
# Copyright 2019-2020, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

import logging
import asyncio
import traceback
from typing import Set

from hail import Flake
from .models import Queue, JobState, QueueJobContext, JobDetails
from .utils import fetch_with_json, fetchrow_with_json

log = logging.getLogger(__name__)


async def _queue_function_wrapper(queue, ctx, args, state=None):
    """Wrapper for the queue function call.

    This wrapper locally manages the declared fail mode of the queue.

    """
    try:
        log.debug("job %s calling with args %r", ctx.job_id, args)
        await queue.function(ctx, *args)
    except Exception as exc:
        state = state or {}
        retry = await queue.fail_mode.handle(JobDetails(ctx.job_id), exc, state)
        if retry:
            return await _queue_function_wrapper(queue, ctx, args, state=state)


async def release_job(manager, conn, task: asyncio.Task, job_id: str):
    """Release a single job. Fetches the result from it and
    updates the table."""
    assert task.done()

    new_state = JobState.Completed
    new_error = ""

    try:
        task.result()
    except Exception:
        new_state = JobState.Error
        new_error = traceback.format_exc()
        log.error("error on job %r, '%s'", job_id, new_error)

    assert new_state in (JobState.Completed, JobState.Error)

    log.debug("completed! updating job %s", job_id)
    queue_name = await conn.fetchval(
        """
        UPDATE violet_jobs
        SET state = $1,
            errors = $2
        WHERE job_id = $3
        RETURNING queue
        """,
        new_state.value,
        new_error,
        job_id,
    )
    log.debug("updated! set job %s => %r", job_id, new_state)

    assert queue_name is not None

    log.debug("check %s in %r", job_id, manager.events)
    if job_id in manager.events:
        log.debug("set end event %s", job_id)
        manager.events[job_id].set()

    try:
        manager._poller_sets[queue_name].remove(job_id)
    except KeyError:
        pass


async def fetch_jobs(
    conn,
    queue: Queue,
    state=JobState.NotTaken,
    scheduled_only: bool = False,
    all: bool = False,
) -> list:
    """Fetch a list of jobs based on search parameters."""
    log.debug("querying state=%r for queue %r", state, queue.name)

    limit_clause = ""
    if not all:
        limit_clause = f"LIMIT {queue.poller_rate[0]}"

    scheduled_where = (
        "AND (now() at time zone 'utc') >= scheduled_at" if scheduled_only else ""
    )
    return await fetch_with_json(
        conn,
        f"""
        SELECT job_id, name, args
        FROM violet_jobs
        WHERE
            queue = $1
        AND state = $2
        {scheduled_where}
        ORDER BY inserted_at
        {limit_clause}
        """,
        queue.name,
        state,
    )


# async def queue_worker(manager, queue: Queue):
#    if queue.start_existing_jobs:
#        await run_jobs(manager, queue, JobState.Taken, raise_on_empty=False)
#
#    while True:
#        await run_jobs(manager, queue, scheduled_only=True)
#        await asyncio.sleep(queue.period)


async def queue_worker_tick(manager, queue, job_id: Flake):
    """Queue Worker Tick.

    This function:
     - Locks the given job (if another worker also takes the job via the
     queue they will fail to lock it properly, it works extremely well for
     multiple workers)
     - Prepares all eventing (start and stop events)
     - Runs the queue function
        (fail modes are handled more locally at the call)
     - Fetches the result from the underlying asyncio task and updates the
        database with it.
    """
    row = await fetchrow_with_json(
        manager.db,
        """
        UPDATE violet_jobs
        SET state = $1, taken_at = (now() at time zone 'utc')
        WHERE job_id = $2 AND state = $3
        RETURNING args, name
        """,
        JobState.Taken.value,
        str(job_id),
        JobState.NotTaken.value,
    )

    if row is None:
        log.warning("job %r already locked, skipping", job_id)
        return

    ctx = QueueJobContext(manager, queue, job_id, row["name"])

    job_id_str = str(job_id)
    if not queue.custom_start_event and job_id_str in manager.start_events:
        manager.start_events[job_id_str].set()

    task = manager.loop.create_task(_queue_function_wrapper(queue, ctx, row["args"]))

    # TODO add configurable timeout for the tasks?
    await asyncio.wait_for(task, None)

    await release_job(manager, manager.db, task, str(job_id))


async def queue_worker(manager, queue: Queue, worker_id: int):
    """Main queue worker.

    This worker keeps waiting for any new job in the backing asyncio queue and
    runs a tick function. The tick function does unique
    functionality (e.g locking)
    """
    while True:
        log.debug("worker %r %d waiting...", queue.name, worker_id)
        job_id: Flake = await queue.asyncio_queue.get()

        log.debug("worker %r %d working on %s", queue.name, worker_id, job_id)
        await queue_worker_tick(manager, queue, job_id)


async def queue_poller(manager, queue: Queue):
    """Queue poller task.

    This task queries the database every second and checks all the jobs that
    are scheduled for right now, limiting itself to ``queue.poller_takes``
    jobs per second.

    There is a poller_sets attribute inside Manager related to this. Its
    purpose is to prevent the poller task from ever pushing repeated jobs.

    This problem arises when the queue function takes longer than a second
    to execute, and so the job that the poller pushed gets pushed again
    every second. If the function takes longer (or the worker crashes) the
    poller would still push new jobs to the queue.

    A set is kept at the Manager level so everyone can coordinate adding
    and removal of such job IDs for the poller. The poller pushes new IDs,
    the worker removes the IDs it worked on.
    """
    while True:
        rows = await fetch_jobs(
            manager.db, queue, state=JobState.NotTaken, scheduled_only=True
        )

        if rows:
            log.info("found %d scheduled jobs", len(rows))

        for row in rows:
            job_id = Flake.from_uuid(row["job_id"])
            as_str = str(job_id)
            poller_jobs = manager._poller_sets[queue.name]
            if as_str in poller_jobs:
                continue

            log.debug("push from scheduled: %s", job_id)
            queue.asyncio_queue.put_nowait(job_id)
            poller_jobs.add(as_str)

        await asyncio.sleep(queue.poller_rate[1])
