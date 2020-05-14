from typing import TypeVar, Generic

QueueArgType = TypeVar("QueueArgType")


class JobQueue(Generic[QueueArgType]):
    """Represents a job queue."""

    @classmethod
    async def handle(_, ctx) -> None:
        ...

    @classmethod
    async def setup(_, ctx) -> None:
        ...
