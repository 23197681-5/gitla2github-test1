import logging
import asyncio
from .models import Queue
from .utils import fetch_with_json


async def queue_worker(pool, queue: Queue):
    log = logging.getLogger(f"violet.{queue.name}")
    # TODO fetch jobs with state = 1 and work on them before main loop

    while True:
        # TODO select columns instead of *, etc
        # TODO wrap in try/finally and make actual fetch be in a worker_tick
        rows = await fetch_with_json(
            pool,
            f"""
            SELECT *
            FROM violet_jobs
            WHERE queue = $1
            ORDER BY inserted_at
            LIMIT {queue.takes}
            """,
            queue.name,
        )
        log.debug("got %d jobs", len(rows))

        # TODO update all their state to 1
        # TODO work on them
        # TODO update all their state to 2

        await asyncio.sleep(queue.period)
