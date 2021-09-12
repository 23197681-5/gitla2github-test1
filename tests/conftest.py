import os
import pytest

import violet
import asyncpg


@pytest.fixture(name="sched")
def job_scheduler_fixture(event_loop):
    sched = violet.JobManager(
        db=event_loop.run_until_complete(
            asyncpg.create_pool(
                host=os.getenv("PSQL_HOST") or "localhost",
                port=os.getenv("PSQL_PORT") or "5432",
                user=os.getenv("PSQL_USER"),
                password=os.getenv("PSQL_PASS"),
                database=os.getenv("PSQL_DB"),
            )
        ),
    )
    yield sched
    event_loop.run_until_complete(sched.stop_all())
