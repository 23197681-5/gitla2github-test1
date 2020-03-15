# violet: An asyncio background job library
# Copyright 2019-2020, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

import enum
import asyncio
import datetime

from typing import (
    Iterable,
    Callable,
    Any,
    Awaitable,
    Optional,
)
from abc import ABC, abstractmethod
from dataclasses import dataclass

from hail import Flake


@dataclass
class JobDetails:
    """Represents the details of any given job."""

    id: Flake
    # function: Callable[..., Any]
    # args: Iterable[Any]


class FailMode(ABC):
    """Base class for failure modes."""

    @abstractmethod
    async def handle(self, job: JobDetails, exception: Exception, state: dict) -> bool:
        ...


@dataclass
class Queue:
    name: str
    args: Iterable[type]
    function: Callable[..., Awaitable[Any]]
    takes: int
    period: float
    start_existing_jobs: bool
    custom_start_event: bool
    fail_mode: FailMode

    task: Optional[asyncio.Task] = None


class JobState(enum.IntEnum):
    NotTaken = 0
    Taken = 1
    Completed = 2
    Error = 3


@dataclass
class QueueJobStatus:
    queue_name: str
    state: JobState
    fail_mode: str
    errors: str
    args: Iterable[type]
    inserted_at: datetime.datetime


@dataclass
class QueueJobContext:
    # TODO fix typing and recursive import that would happen for this maybe
    manager: Any
    queue: Queue
    job_id: Flake
    name: str

    def set_start(self):
        """Set the start event on the job. Raises RuntimeError if the queue
        is not configured for custom start events."""
        job_id_str = str(self.job_id)

        if not self.queue.custom_start_event:
            raise RuntimeError("Queue isn't configured for custom start events")

        if job_id_str in self.manager.start_events:
            self.manager.start_events[job_id_str].set()
