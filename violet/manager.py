# violet: An asyncio background job library
# Copyright 2019, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

import asyncio
import logging
from typing import Callable, List, Any, Iterable, Dict

from .errors import TaskExistsError

log = logging.getLogger(__name__)


class JobManager:
    """Manage background jobs."""

    def __init__(self, *, loop=None, db=None):
        self.loop = loop or asyncio.get_event_loop()
        self.db = db
        self.tasks: Dict[str, asyncio.Task] = {}

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
        self, queue_name, *, args: Iterable[type], handler, concurrent_takes: int = 5
    ):
        """Create a job queue.

        The job queue MUST be declared at the start of the application so
        recovery can happen by then.
        """
        raise NotImplementedError()

    async def push_queue(self, queue: str, args: List[Any], **kwargs):
        """Push data to a job queue."""
        raise NotImplementedError()
