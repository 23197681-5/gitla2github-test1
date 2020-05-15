# violet: An asyncio background job library
# Copyright 2019-2020, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

import asyncio


class JobEvent(asyncio.Event):
    """An event subclass that has a counter to signal when there are
    no other waiting tasks.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.counter = 0
        self.empty_event = asyncio.Event()

    def __repr__(self) -> str:
        return f"<JobEvent counter={self.counter}>"

    async def wait(self):
        self.counter += 1
        await super().wait()
        self.counter -= 1

        if self.counter == 0:
            self.empty_event.set()
