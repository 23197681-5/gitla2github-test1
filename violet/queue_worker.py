import asyncio
from .models import Queue
from .utils import fetch_with_json


async def queue_worker(pool, queue: Queue):
    while True:
        # TODO select columns, etc
        # TODO wrap in try/finally and make actual fetch be in a worker_tick
        rows = await fetch_with_json(
            pool, "SELECT * FROM violet_jobs WHERE queue = $1", queue
        )
        print(rows)
        await asyncio.sleep(queue.period)
