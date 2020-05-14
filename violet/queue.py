from typing import Optional, Tuple
from violet.fail_modes import FailMode


class JobQueue:
    """Represents a job queue."""

    workers = 1
    start_existing_jobs = True
    fail_mode: Optional[FailMode] = None
    poller_takes: int = 1
    poller_seconds: float = 1.0

    @property
    @classmethod
    def poller_rate(cls) -> Tuple[int, float]:
        return cls.poller_takes, cls.poller_seconds

    @classmethod
    async def setup(_, ctx) -> None:
        ...

    @classmethod
    async def handle(_, ctx) -> None:
        ...
