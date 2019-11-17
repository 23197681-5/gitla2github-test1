import asyncio
from typing import Iterable, Callable, Any, List, Awaitable, Optional
from dataclasses import dataclass


@dataclass
class Queue:
    args: Iterable[type]
    function: Callable[..., Awaitable[Any]]
    takes: int
    period: int

    task: Optional[asyncio.Task] = None
