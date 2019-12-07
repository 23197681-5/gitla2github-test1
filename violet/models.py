import enum
import asyncio
import datetime
from typing import Iterable, Callable, Any, List, Awaitable, Optional
from dataclasses import dataclass


@dataclass
class Queue:
    name: str
    args: Iterable[type]
    function: Callable[..., Awaitable[Any]]
    takes: int
    period: int

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
