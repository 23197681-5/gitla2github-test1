from typing import Optional, Tuple, Union
from hail import Flake
from violet.fail_modes import FailMode
from violet.utils import execute_with_json, fetchrow_with_json
from violet.models import QueueJobStatus
from violet.manager import JobManager


class JobQueue:
    """Represents a job queue."""

    workers = 1
    start_existing_jobs = True
    fail_mode: Optional[FailMode] = None
    poller_takes: int = 1
    poller_seconds: float = 1.0

    @property
    @classmethod
    def sched(cls) -> JobManager:
        try:
            return getattr(cls, "_sched")
        except AttributeError:
            raise RuntimeError("Job queue was not initialized.")

    @property
    @classmethod
    def name(cls):
        try:
            return getattr(cls, "name")
        except AttributeError:
            raise RuntimeError("Job queue must have a name attribute.")

    @property
    @classmethod
    def poller_rate(cls) -> Tuple[int, float]:
        return cls.poller_takes, cls.poller_seconds

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
                state, errors, inserted_at, scheduled_at
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
            """
            SELECT internal_state
            FROM violet_jobs
            WHERE job_id = $1
            """,
            str(job_id),
        )

        return row["internal_state"] if row is not None else None
