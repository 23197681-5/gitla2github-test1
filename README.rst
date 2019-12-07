violet
==========

An asyncio background job library, or, as I like to call it,
a more powerful wrapper `asyncio.create_task`.

Note
--------
This library is:

1. Very barebones. You should not expect it to be general-purpose.
2. Designed for internal use in elixire_. Use at your own risk.
3. Arguments to functions MUST be JSON serializable.
   Passing instances of classes will NOT work.

.. _elixire: https://gitlab.com/elixire/elixire

Usage
--------

Violet requires ::

    from violet import JobManager

    # it is the responsibility of the user to configure tables. violet will
    # not do it for the user. at most, violet will generate table definitions
    # to guide the user.
    sched = JobManager(db=loop.run_until_complete(asyncpg.create_pool(...)))

    async def function(a, b):
        print(a + b)

    # will spawn function in the background
    # (for one-shot tasks that don't need to recover)
    sched.spawn(function, [2, 2], job_id="my_function")

    # will spawn function every 5 seconds indefnitely (things that should run
    # when the webapp is starting up)
    sched.spawn_periodic(function, [2, 2], period=5, job_id="my_function")

    # will spawn function as a background job queue, which makes it resumable in
    # the face of a crash.

    # the create_job_queue call should happen when starting up.
    sched.create_job_queue(
        "add",
        args=(int, int),
        handler=function,
        max_concurrent_workers=5
    )

    # and, to push data into the queue
    await sched.push_queue("add", [1, 2])

Install
--------

TODO: publish on pypi?

::

    pip install git+https://gitlab.com/elixire/violet@master
