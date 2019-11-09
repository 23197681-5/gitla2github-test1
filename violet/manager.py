# violet: An asyncio background job library
# Copyright 2019, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

import asyncio
import logging
from typing import Function, List, Any

from .errors import JobExistsError

log = logging.getLogger(__name__)


class JobManager:
    """Manage background jobs."""

    def __init__(self, loop=None):
        log.debug("job manager start")
        self.loop = loop or asyncio.get_event_loop()
        self.jobs = {}

    def _create_task(self, task_id: str, *, main_coroutine):
        if task_id in self.jobs:
            raise JobExistsError(f"Job '{task_id}' already exists")

        task = self.loop.create_task(main_coroutine)
        self.jobs[task_id] = task
        return task

    def spawn(self, func, args, *, task_id: str, **kwargs):
        raise NotImplementedError()

    def spawn_periodic(self, func, args, *, period: int, task_id: str, **kwargs):
        raise NotImplementedError()

    def remove_job(self, job_id: str) -> None:
        """Remove a job from the internal jobs dictionary. You most likely want
        to use stop_job()."""
        try:
            self.jobs.pop(job_id)
        except KeyError:
            pass

    def stop_job(self, job_name: str) -> None:
        """Stop a single job."""
        log.debug("stopping job %r", job_name)

        try:
            task = self.jobs[job_name]
        except KeyError:
            log.warning("unknown job to cancel: %r", job_name)
            return

        try:
            task.cancel()
        finally:
            self.remove_job(job_name)

    def stop_all(self) -> None:
        """Stop the job manager by cancelling all jobs."""
        log.debug("cancelling %d jobs", len(self.jobs))
        for job_name in list(self.jobs.keys()):
            self.stop_job(job_name)
