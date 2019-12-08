# violet: An asyncio background job library
# Copyright 2019, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

import uuid
import asyncio
import logging
from typing import List, Any, Iterable, Dict, Optional

from .errors import TaskExistsError, QueueExistsError
from .models import Queue, QueueJobStatus
from .queue_worker import queue_worker, StopQueueWorker
from .utils import execute_with_json, fetchrow_with_json

log = logging.getLogger(__name__)


class JobManager:
    """Manage background jobs."""

    def __init__(self, *, loop=None, db=None):
        self.loop = loop or asyncio.get_event_loop()
        self.db = db
        self.tasks: Dict[str, asyncio.Task] = {}
        self.queues: Dict[str, Queue] = {}

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
            await function(*args)
        except asyncio.CancelledError:
            log.debug("task %r cancelled", task_id)
        except Exception:
            log.exception("error at task %r", task_id)
        finally:
            # TODO use remove_job()
            self.tasks.pop(task_id)

    def spawn(
        self, function, args: List[Any], *, job_id: str, **kwargs
    ) -> asyncio.Task:
        """Spawn the given function in the background.

        This is a wrapper around loop.create_task that gives you proper logging
        and optional recovery capabilities.

        If you wish the background task is fully recoverable even in the face
        of a crash, use a job queue.
        """

        # TODO quart context support
        return self._create_task(
            job_id, main_coroutine=self._wrapper(function, args, job_id, **kwargs)
        )

    def spawn_periodic(
        self, function, args: List[Any], *, period: int = 5, job_id: str, **kwargs
    ):
        """Spawn a function that ticks itself periodically every
        ``period`` seconds."""

        # TODO quart context support.
        async def ticker_func():
            while True:
                await function(*args)
                await asyncio.sleep(period)

        return self._create_task(
            job_id, main_coroutine=self._wrapper(ticker_func, [], job_id, **kwargs)
        )

    def create_job_queue(
        self,
        queue_name,
        *,
        args: Iterable[type],
        handler,
        takes: int = 5,
        period: int = 1,
    ):
        """Create a job queue.

        The job queue MUST be declared at the start of the application so
        recovery can happen by then.
        """
        if queue_name in self.queues:
            raise QueueExistsError()

        self.queues[queue_name] = Queue(queue_name, args, handler, takes, period)

    def _create_queue_worker(self, queue: Queue):
        async def _wrapper():
            try:
                await queue_worker(self, queue)
            except StopQueueWorker:
                pass
            except Exception:
                log.exception("Queue worker for queue %r failed", queue.name)
            finally:
                queue.task = None

        queue.task = self.loop.create_task(_wrapper())

    async def push_queue(
        self,
        queue_name: str,
        args: List[Any],
        *,
        job_id: Optional[str] = None,
        **kwargs,
    ):
        """Push data to a job queue."""

        # ensure queue was declared
        if queue_name not in self.queues:
            raise ValueError(f"Queue {queue_name} does not exist")

        log.debug("try push %r %r", queue_name, args)
        job_id = job_id or uuid.uuid4().hex

        await execute_with_json(
            self.db,
            """
            INSERT INTO violet_jobs (job_id, queue, args) VALUES ($1, $2, $3)
            """,
            job_id,
            queue_name,
            args,
        )
        log.debug("pushed %r %r", queue_name, args)

        queue = self.queues[queue_name]
        if queue.task is None:
            self._create_queue_worker(queue)

        return job_id

    async def fetch_queue_job_status(self, job_id: str) -> QueueJobStatus:
        row = await fetchrow_with_json(
            self.db,
            """
            SELECT
                queue, state, fail_mode, errors, args, inserted_at
            FROM violet_jobs
            WHERE
                job_id = $1
            """,
            job_id,
        )

        return QueueJobStatus(*row)

    async def set_queue_job_internal_state(
        self, job_id: str, internal_state: Dict[Any, Any]
    ) -> None:
        await execute_with_json(
            self.db,
            """
            UPDATE violet_jobs
            SET internal_state = $1
            WHERE
                job_id = $2
            """,
            internal_state,
            job_id,
        )
