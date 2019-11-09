violet
==========

An asyncio background job library, or, as I like to call it,
a more powerful wrapper `asyncio.create_task`.

Note
--------
This library is:

1. Very barebones. You should not expect it to be general-purpose.
2. Designed for internal use in elixire_. Use at your own risk.

.. _elixire: https://gitlab.com/elixire/elixire

Usage
--------

Violet may require a database connection for job queues::

    from violet import JobManager
    sched = JobManager()

    async def function(a, b):
        print(a + b)

    # will spawn function in the background
    sched.spawn(function, [2, 2], job_id="my_function")

    # will spawn function every 5 seconds indefnitely
    sched.spawn_periodic(function, [2, 2], period=5, job_id="my_function")

    # will spawn function as a job queue (TODO nail down api for this)

    # it is the responsibility of the user to configure tables. violet will
    # not do it for the user. at most, violet will generate table definitions
    # to guide the user.

    sched = JobManager(job_queue=True, db=asyncpg.create_pool(...))
    sched.create_job_queue("add", args=(int, int), handler=function)
    await sched.push_queue("add", [1, 2])

Install
--------

TODO
