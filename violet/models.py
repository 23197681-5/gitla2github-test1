# violet: An asyncio background job library
# Copyright 2019-2020, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

import enum
import asyncio
import datetime

from typing import Iterable, Callable, Any, Awaitable, Tuple
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
    cls: Any
    asyncio_queue: asyncio.Queue


class JobState(enum.IntEnum):
    NotTaken = 0
    Taken = 1
    Completed = 2
    Error = 3


@dataclass
class QueueJobStatus:
    """Represents the status of a job in a job queue."""

    state: JobState
    errors: str
    inserted_at: datetime.datetime
    scheduled_at: datetime.datetime
    taken_at: datetime.datetime


@dataclass
class QueueJobContext:
    queue: Queue
    job_id: Flake
    name: str
    args: Any
