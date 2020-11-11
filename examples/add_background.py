import asyncio
import logging
from violet import JobManager
from violet.fail_modes import Retry, RaiseErr

logging.basicConfig(level=logging.DEBUG)


class CustomError(Exception):
    pass


async def my_function(a, b):
    print(a + b)
    raise CustomError("this is a test error")


def main():
    loop = asyncio.get_event_loop()
    sched = JobManager()
    task = sched.spawn(
        my_function,
        [2, 2],
        name="my_function",
        fail_mode=Retry(exception_list=(CustomError,)),
        fallback_mode=RaiseErr(),
    )
    loop.run_until_complete(task)


if __name__ == "__main__":
    main()
