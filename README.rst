violet
==========

An asyncio background job library, or, as I like to call it,
a more powerful wrapper `asyncio.create_task`.

(this project's name is inspired by Violet Evergarden, an anime about a
robot girl's experience with trauma, and neuroatypical socialization in general.)

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

The simple subset of violet features can be described by

.. code-block:: python

    from violet import JobManager

    # it is the responsibility of the user to configure tables.
    sched = JobManager(db=loop.run_until_complete(asyncpg.create_pool(...)))

    async def function(a, b):
        print(a + b)

    # will spawn function in the background
    # (for one-shot tasks that don't need to recover)
    sched.spawn(function, [2, 2], job_id="my_function")

    # will spawn function every 5 seconds indefnitely (things that should run
    # when the webapp is starting up)
    sched.spawn_periodic(function, [2, 2], period=5, job_id="my_function")

    # the create_job_queue call should happen when starting up.
    sched.create_job_queue(
        "add",
        args=(int, int),
        handler=function,
        takes=5,
        period=1,
    )

    # and, to push data into the queue
    job_id = await sched.push_queue("add", [1, 2])

    # wait for the job to start
    await sched.wait_job_start(job_id)

    # wait for the job to finish
    await sched.wait_job(job_id)

    # finished with the scheduler? make it stop all running jobs
    await sched.stop_all(wait=True)

Install
--------

TODO: publish on pypi?

.. code-block:: sh

    pip install git+https://gitlab.com/elixire/violet@master

    # violet does not explicitly depend on asyncpg, but if you want to use
    # the job queue features of violet, an asyncpg connection must be given.
    pip install asyncpg>0.20
