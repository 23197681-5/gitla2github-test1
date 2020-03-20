# violet: An asyncio background job library
# Copyright 2019-2020, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

import datetime
import uuid
import asyncio
import logging
import random
from typing import List, Any, Iterable, Dict, Optional, Union, Callable, Set
from collections import defaultdict

from hail import Flake, FlakeFactory

from violet.errors import TaskExistsError, QueueExistsError
from violet.models import Queue, QueueJobStatus, JobDetails
from violet.queue_worker import queue_worker, queue_poller
from violet.utils import execute_with_json, fetchrow_with_json
from violet.event import JobEvent
from violet.fail_modes import FailMode, LogOnly, RaiseErr

log = logging.getLogger(__name__)


class EmptyAsyncContext:
    def __init__(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, type, value, traceback):
        return None


class JobManager:
    """Manage background jobs."""

    def __init__(
        self,
        *,
        loop=None,
        db=None,
        context_function=None,
        node_id: Optional[int] = None,
    ):
        self.loop = loop or asyncio.get_event_loop()
        self.db = db
        self.tasks: Dict[str, asyncio.Task] = {}
        self.queues: Dict[str, Queue] = {}
        self.factory = FlakeFactory(node_id or random.randint(0, 65535))

        self.events: Dict[str, JobEvent] = defaultdict(JobEvent)
        self.start_events: Dict[str, JobEvent] = defaultdict(JobEvent)

        self.empty_waiters: Dict[str, asyncio.Task] = {}
        self.empty_start_waiters: Dict[str, asyncio.Task] = {}

        self.context_creator = context_function or EmptyAsyncContext
        self._poller_sets: Dict[str, Set[str]] = defaultdict(set)

    def exists(self, task_id: str) -> bool:
        """Return if a given task exists in the current running task list."""
        return task_id in self.tasks

    def _create_task(self, task_id: str, *, main_coroutine):
        """Wrapper around loop.create_task that ensures unique task ids
        internally."""
        if task_id in self.tasks:
            raise TaskExistsError(f"Task '{task_id}' already exists")

        task = self.loop.create_task(main_coroutine)
        self.tasks[task_id] = task
        return task

    async def _wrapper(self, function, args, task_id, **kwargs):
        """Wrapper for coroutines, wrapping them in try/excepts for logging"""
        try:
            log.debug("task tick: %r, state: %r", task_id, kwargs.get("_wrapper_state"))

            async with self.context_creator():
                await function(*args)

            log.debug("task done: %r", task_id)
        except asyncio.CancelledError:
            log.debug("task %r cancelled", task_id)
        except Exception as exc:
            fail_mode: FailMode = kwargs.get("fail_mode") or LogOnly()

            # state here serves as a way for failure modes to be fully
            # contained. the instantiation of a failure mode only declares
            # its configuration, its behavior becomes completely separate
            # inside the auto-genned state dict.
            state = kwargs.get("_wrapper_state") or {}
            retry = await fail_mode.handle(JobDetails(task_id), exc, state)

            if retry:
                # we need to share state for this job in some way and this is
                # the best way i found while developing.
                kwargs["_wrapper_state"] = state
                return await self._wrapper(function, args, task_id, **kwargs)
        finally:
            # TODO failure modes for single tasks
            self._remove_task(task_id)

    def spawn(self, function, args: List[Any], *, name: str, **kwargs) -> asyncio.Task:
        """Spawn the given function in the background.

        This is a wrapper around loop.create_task that gives you proper logging
        and optional recovery capabilities.

        If you wish the background task is fully recoverable even in the face
        of a crash, use a job queue.
        """

        return self._create_task(
            name, main_coroutine=self._wrapper(function, args, name, **kwargs)
        )

    def spawn_periodic(
        self, function, args: List[Any], *, period: float = 5, name: str, **kwargs
    ):
        """Spawn a function that ticks itself periodically every
        ``period`` seconds."""

        async def ticker_func():
            while True:
                await function(*args)
                await asyncio.sleep(period)

        return self._create_task(
            name, main_coroutine=self._wrapper(ticker_func, [], name, **kwargs)
        )

    def create_job_queue(
        self,
        queue_name: str,
        *,
        args: Iterable[type],
        handler: Callable[..., Any],
        workers: int = 1,
        start_existing_jobs: bool = True,
        custom_start_event: bool = False,
        fail_mode: Optional[FailMode] = None,
        poller_takes: int = 1,
    ):
        """Create a job queue.

        The job queue MUST be declared at the start of the application so
        job recovery can happen as soon as possible. It is also required to
        declare the queue before any queue operations, as they won't know about
        the queue.

        To enhance the concurrency of the queue on high error rates, the first
        consideration is fixing the error first, and the second, is to raise
        the ``workers`` count. It is not recommended to raise it to very
        high levels, as there will be a lot of clashing as workers try to lock
        the same job multiple times.

        Users of violet can wait for specific events of a job (currently, start
        and end). While the end event is static and is always called, the
        start event can be customized and set in a different point in time
        inside the job itself (instead of it being at lock time). This might
        be interesting for users that want to have a jobs' state setup after
        they hear about the job start. Set ``custom_start_event`` to enable
        this functionality.

        While creating a job queue, a single worker, called the "poller", is
        spawned. The poller checks every second for any outstanding jobs
        to run at its current point in time. An outstanding job is a job that
        was scheduled in the future, but should be run right now.

        ``poller_takes`` sets the maximum amount of jobs that will be
        taken by the poller and be inserted into the job queue.
        """
        fail_mode = fail_mode or RaiseErr(log_error=True)

        if queue_name in self.queues:
            raise QueueExistsError()

        queue = Queue(
            queue_name,
            args,
            handler,
            start_existing_jobs,
            custom_start_event,
            fail_mode,
            asyncio.Queue(),
            poller_takes,
        )

        self.queues[queue_name] = queue

        # TODO create the resumer task (fetch existing jobs on Taken state
        # and send them to asyncio_queue)

        self.spawn(queue_poller, [self, queue], name=f"queue_poller_{queue_name}")

        for worker_id in range(workers):
            self.spawn(
                self._queue_worker_wrapper,
                [queue, worker_id],
                name=f"queue_worker_{queue.name}_{worker_id}",
            )

    async def _queue_worker_wrapper(self, queue: Queue, worker_id: int):
        try:
            async with self.context_creator():
                await queue_worker(self, queue, worker_id)

        except asyncio.CancelledError:
            log.debug("queue worker for %r cancelled", queue.name)
        except Exception:
            log.exception("Queue worker for queue %r failed", queue.name)

    async def push_queue(
        self, queue_name: str, args: List[Any], *, name: Optional[str] = None, **kwargs,
    ) -> Flake:
        """Push data to a job queue."""

        try:
            queue = self.queues[queue_name]
        except KeyError:
            raise ValueError(f"Queue {queue_name} does not exist")

        log.debug("try push %r %r", queue_name, args)
        scheduled_at = kwargs.get("scheduled_at") or datetime.datetime.utcnow()

        job_id = self.factory.get_flake()
        name = name or uuid.uuid4().hex

        await execute_with_json(
            self.db,
            """
            INSERT INTO violet_jobs
                (job_id, name, queue, args, scheduled_at)
            VALUES
                ($1, $2, $3, $4, $5)
            """,
            str(job_id),
            name,
            queue_name,
            args,
            scheduled_at,
        )
        log.debug("pushed %r %r", queue_name, args)

        # only dispatch to asyncio queue if it is actually meant to be now.
        # TODO: a better heuristic would be getting the timedelta between
        # given scheduled_at and dispatching to the queue if it is less than
        # 1 second, but this already does the job.
        if not kwargs.get("scheduled_at"):
            queue.asyncio_queue.put_nowait(job_id)

        return job_id

    async def fetch_queue_job_status(
        self, job_id: Union[str, Flake]
    ) -> Optional[QueueJobStatus]:
        row = await fetchrow_with_json(
            self.db,
            """
            SELECT
                queue, state, fail_mode, errors, args, inserted_at
            FROM violet_jobs
            WHERE
                job_id = $1
            """,
            str(job_id),
        )

        if row is None:
            return None

        return QueueJobStatus(*row)

    async def set_job_state(
        self, job_id: Union[str, Flake], state: Dict[Any, Any]
    ) -> None:
        await execute_with_json(
            self.db,
            """
            UPDATE violet_jobs
            SET internal_state = $1
            WHERE
                job_id = $2
            """,
            state,
            str(job_id),
        )

    async def fetch_job_state(
        self, job_id: Union[str, Flake]
    ) -> Optional[Dict[Any, Any]]:
        row = await fetchrow_with_json(
            self.db,
            """
            SELECT internal_state AS state
            FROM violet_jobs
            WHERE
                job_id = $1
            """,
            str(job_id),
        )

        return row["state"] if row is not None else None

    def _remove_task(self, task_id: str) -> None:
        """Remove a job from the internal task list."""
        try:
            self.tasks.pop(task_id)
        except KeyError:
            pass

    def stop(self, task_id: str) -> None:
        log.debug("stopping task %r", task_id)

        try:
            task = self.tasks[task_id]
            task.cancel()
        except KeyError:
            log.warning("unknown task to cancel: %r", task_id)
        finally:
            # as a last measure, try to pop() the job
            # post-cancel. if that fails, the job probably
            # already cleaned itself.
            self._remove_task(task_id)

    def stop_all(self) -> None:
        """Stop the job manager by cancelling all tasks."""
        log.debug("cancelling %d tasks", len(self.tasks))

        for task_id in list(self.tasks.keys()):
            self.stop(task_id)

    async def wait_job(self, any_job_id: Union[str, Flake], *, timeout=None) -> None:
        """Wait for a job to complete."""

        job_id = str(any_job_id)

        async def empty_waiter():
            await self.events[job_id].empty_event.wait()
            self.events.pop(job_id)
            self.empty_waiters.pop(job_id)

        if job_id not in self.empty_waiters:
            self.empty_waiters[job_id] = self.spawn(
                empty_waiter, [], name=f"empty_waiter:{job_id}"
            )

        await asyncio.wait_for(self.events[job_id].wait(), timeout)

    async def wait_job_start(
        self, any_job_id: Union[str, Flake], *, timeout=None
    ) -> None:
        """Wait for a job to start.

        Start can be defined by:
         - When the queue worker acquires the jobs
         - When the queue handler signals it's start

        That is a queue setting.

        It is recommended for queues to signal themselves as custom,
        if they have any state to setup, or else, users of this function
        might get completely empty state.
        """

        job_id = str(any_job_id)

        async def waiter():
            await self.start_events[job_id].empty_event.wait()
            self.start_events.pop(job_id)
            self.empty_start_waiters.pop(job_id)

        if job_id not in self.empty_start_waiters:
            self.empty_start_waiters[job_id] = self.spawn(
                waiter, [], name=f"empty_start_waiter:{job_id}"
            )

        await asyncio.wait_for(self.start_events[job_id].wait(), timeout)
