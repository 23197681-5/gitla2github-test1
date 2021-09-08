import asyncio
from violet import JobManager, __version__
from violet.fail_modes import RaiseErr


def test_version():
    assert __version__ == "0.3.0"


class CustomError(Exception):
    pass


async def my_function(a: int, b: int, should_fail: bool = False) -> int:
    if should_fail:
        raise CustomError("this is a test error")

    return a + b


def test_simple_spawn(event_loop):
    sched = JobManager()
    task = sched.spawn(
        my_function,
        [2, 2, False],
        name="my_function",
        fail_mode=RaiseErr(),
    )

    task_with_error = sched.spawn(
        my_function,
        [2, 2, True],
        name="my_function_with_error",
        fail_mode=RaiseErr(),
    )

    event_loop.call_soon(task)
    event_loop.call_soon(task_with_error)
    event_loop.run_until_complete(asyncio.sleep(1))

    assert task.result() == 4

    try:
        task_with_error.result()
        assert False  # task supposed to have an error did not have an error
    except CustomError:
        assert True  # Task had an error, which is good
