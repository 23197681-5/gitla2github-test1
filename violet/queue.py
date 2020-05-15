import asyncio
import logging
from typing import Optional, Tuple, Union, Generic, TypeVar

from hail import Flake

from violet.fail_modes import FailMode
from violet.utils import execute_with_json, fetchrow_with_json
from violet.models import QueueJobStatus, JobState

log = logging.getLogger(__name__)


QueueArgType = TypeVar("QueueArgType")


class MetaJobQueue(type):
    def __init__(cls, *args, **kwargs):
        cls._sched = None

    @property
    def sched(cls):
        if cls._sched is None:
            raise RuntimeError("Job queue was not initialized.")
        return getattr(cls, "_sched")

    @property
    def name(cls):
        try:
            return getattr(cls, "queue_name")
        except AttributeError:
            raise RuntimeError("Job queues must have the `queue_name` attribute.")


class JobQueue(Generic[QueueArgType], metaclass=MetaJobQueue):
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
    def create_args(_, row) -> QueueArgType:
        ...

    @classmethod
    async def setup(_, ctx) -> None:
        """Setup routines for the job.
        Coroutines waiting for the job's start will be woken up here.
        """
        pass

    @classmethod
    async def handle(_, ctx) -> None:
        """Actual job code.
        Coroutines waiting for the job itself will be woken up after this
        function finishes.
        """
        ...

    @classmethod
    async def fetch_job_status(
        cls, job_id: Union[str, Flake]
    ) -> Optional[QueueJobStatus]:
        """Fetch the status of a job.
        Contains its job state (different from the internal state,
        which can be anything for the job), errors, and
        inserted/scheduled timestamps."""
        row = await fetchrow_with_json(
            cls.sched.db,
            f"""
            SELECT
                state, errors, inserted_at, scheduled_at, taken_at
            FROM {cls.name}
            WHERE
                job_id = $1

            """,
            str(job_id),
        )

        if row is None:
            return None

        return QueueJobStatus(**row)

    @classmethod
    async def set_job_state(cls, job_id: Union[str, Flake], state: dict) -> None:
        """Set a job's state. The job queue's table MUST have an
        ``internal_state jsonb default null`` column."""
        await execute_with_json(
            cls.sched.db,
            f"""
            UPDATE {cls.name}
            SET
                internal_state = $1
            WHERE
                job_id = $2
            """,
            state,
            str(job_id),
        )

    @classmethod
    async def fetch_job_state(cls, job_id: Union[str, Flake]) -> Optional[dict]:
        """Fetch a job's internal state."""
        row = await fetchrow_with_json(
            cls.sched.db,
            f"""
            SELECT internal_state
            FROM {cls.name}
            WHERE job_id = $1
            """,
            str(job_id),
        )

        return row["internal_state"] if row is not None else None

    @classmethod
    async def wait_job(cls, any_job_id: Union[str, Flake], *, timeout=None) -> None:
        """Wait for a job to complete."""

        job_id = str(any_job_id)

        sched = cls.sched

        # short-circuit if the given job already completed.
        # we would hang around forever if we waited for a job that already
        # released itself (which can happen!)
        state_int = await sched.db.fetchval(
            f"""
            SELECT state
            FROM {cls.name}
            WHERE job_id = $1
            """,
            job_id,
        )

        if state_int is None:
            raise ValueError("Unknown job")

        state = JobState(state_int)
        log.debug("pre-wait fetch %r %r", state_int, state)

        if state in (JobState.Completed, JobState.Error):
            return

        async def empty_waiter():
            await sched.events[job_id].empty_event.wait()
            sched.events.pop(job_id)
            sched.empty_waiters.pop(job_id)

        if job_id not in sched.empty_waiters:
            sched.empty_waiters[job_id] = sched.spawn(
                empty_waiter, [], name=f"empty_waiter:{job_id}"
            )

        await asyncio.wait_for(sched.events[job_id].wait(), timeout)
