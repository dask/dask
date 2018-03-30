Prioritizing Work
=================

When there is more work than workers, Dask has to decide which tasks to
prioritize over others.  Dask can determine these priorities automatically to
optimize performance, or a user can specify priorities manually according to
their needs.

Dask uses the following priorities, in order:

1.  **User priorities**: A user defined priority, provided by the ``priority=`` keyword argument
    to functions like ``compute()``, ``persist()``, ``submit()``, or ``map()``.
    Tasks with higher priorities run before tasks with lower priorities with
    the default priority being zero.

    .. code-block:: python

       future = client.submit(func, *args, priority=10)  # high priority task
       future = client.submit(func, *args, priority=-10)  # low priority task

       df = df.persist(priority=10)  # high priority computation

2.  **First in first out chronologically**: Dask prefers computations that were
    submitted early.  Because users can submit computations asynchronously it
    may be that several different computations are running on the workers at
    the same time.  Generally Dask prefers those groups of tasks that were
    submitted first.

    As a nuance, tasks that are submitted within a close window are often
    considered to be submitted at the same time.

    .. code-block:: python

       x = x.persist()  # submitted first and so has higher priority
       # wait a while
       x = x.persist()  # submitted second and so has lower priority

    In this case "a while" depends on the kind of computation. Operations
    that are often used in bulk processing, like ``compute`` and ``persist``
    consider any two computations submitted in the same sixty seconds
    to have the same priority.  Operations that are often used in real-time
    processing, like ``submit`` or ``map`` are considered the same priority if
    they are submitted within the 100 milliseconds of each other.  This
    behavior can be controlled with the ``fifo_timeout=`` keyword:

    .. code-block:: python

       x = x.persist()
       # wait one minute
       x = x.persist(fifo_timeout='10 minutes')  # has the same priority

       a = client.submit(func, *args)
       # wait no time at all
       b = client.submit(func, *args, fifo_timeout='0ms')  # is lower priority

3.  **Graph Structure**: Within any given computation (a compute or persist
    call) Dask orders tasks in such a way as to minimize the memory-footprint
    of the computation.  This is discussed in more depth in the
    `task ordering documentation <https://github.com/dask/dask/blob/master/dask/order.py>`_.
