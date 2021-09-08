# violet: An asyncio background job library
# Copyright 2019-2020, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

import random
import logging
from decimal import Decimal
from asyncio import sleep
from typing import Optional, Type, Tuple, Union, List

from violet.models import FailMode

log = logging.getLogger(__name__)


class LogOnly(FailMode):
    """Log the error and its stacktrace."""

    async def handle(self, job, exc, _state) -> bool:
        log.exception("error at task %r", job.id)
        return False


class RaiseErr(FailMode):
    """Failure mode to raise the job error back to the caller.

    Important for users of jobs that have the asyncio.Task object and want
    to get the exception via task.result()
    """

    def __init__(
        self,
        *,
        log_error: bool = True,
        omit_exceptions: Optional[List[Type[Exception]]] = None,
    ):
        self.log_error = log_error
        self.omit_exceptions = omit_exceptions or tuple()

    async def handle(self, job, exc, _state) -> bool:
        if isinstance(exc, self.omit_exceptions):
            raise exc

        if self.log_error:
            await LogOnly().handle(job, exc, _state)

        raise exc


class Retry(FailMode):
    """Retry a given task when it errors.

    This is a raw implementation of Exponential Backoff for tasks.

    All retry default parameters were chosen to be very general. It is highly
    recommended for your task to use custom numbers.

    Decimals are used for wanted accuracy, but are then casted into floats
    because of limitations on sleep()

    NOTE: Keep in mind with every retry the task will be spawned again
    recusrively. Be mindful of stack overflows.

    Parameters
    ----------
    log_error: bool
        If the error caught by the mode should be shown to logs.
    exception_list: Tuple[Type[Exception]]
        The exception list this fail mode will act upon. By default, all errors
        will be treated as to-be-retried.
    fallback_mode: FailMode
        If an exception does not match exception_list, this fail mode will
        be called. LogOnly by default.

    retry_cap: Decimal
        The maximum amount of seconds for the algorithm. The retry will not
        be higher than this cap.
    retry_base: Decimal
        The base number for exponential backoff. It determines the shape of
        your backoffs. It is recommended to find a custom number for the
        task at hand.
    retry_max_attempts: int
        The maximum amount of attempts until Retry gives up and raises the
        error.
    """

    def __init__(
        self,
        *,
        log_error: bool = True,
        exception_list: Optional[Tuple[Type[Exception]]] = None,
        fallback_mode: Optional[FailMode] = None,
        retry_cap: Decimal = Decimal(20),
        retry_base: Decimal = Decimal("0.3"),
        retry_max_attempts: int = 5,
    ):
        self.log_error = log_error
        self.exception_list: Tuple[Type[Exception]] = exception_list or (Exception,)
        self.fallback_mode: FailMode = fallback_mode or LogOnly()

        self.cap = retry_cap
        self.base = retry_base
        self.max_attempts = retry_max_attempts

    async def handle(self, job, exc, state) -> bool:
        if not isinstance(exc, self.exception_list):
            return await self.fallback_mode.handle(job, exc, state)

        if self.log_error:
            await LogOnly().handle(job, exc, state)

        state["counter"] = state.get("counter", 0) + 1

        sleep_secs: Union[int, float] = random.uniform(
            0, float(min(self.cap, self.base * 2 ** state["counter"]))
        )

        if state["counter"] > self.max_attempts:
            return await RaiseErr().handle(job, exc, state)

        log.warning("task '%s' sleeping for %.2f seconds", job.id, sleep_secs)
        await sleep(sleep_secs)
        return True
