Python API (advanced)
=====================

.. currentmodule:: distributed  #doctest: +SKIP

In some rare cases, experts may want to create ``Scheduler``, ``Worker``, and
``Nanny``  objects explicitly in Python.  This is often necessary when making
tools to automatically deploy Dask in custom settings.

It is more common to create a :doc:`Local cluster with Client() on a single
machine <single-distributed>` or use the :doc:`Command Line Interface (CLI) <cli>`.
New readers are recommended to start there.

If you do want to start Scheduler and Worker objects yourself you should be a
little familiar with ``async``/``await`` style Python syntax.  These objects
are awaitable and are commonly used within ``async with`` context managers.
Here are a few examples to show a few ways to start and finish things.

Full Example
------------

.. autosummary::
   Scheduler
   Worker
   Client

We first start with a comprehensive example of setting up a Scheduler, two Workers,
and one Client in the same event loop, running a simple computation, and then
cleaning everything up.

.. code-block:: python

   import asyncio
   from dask.distributed import Scheduler, Worker, Client

   async def f():
       async with Scheduler() as s:
           async with Worker(s.address) as w1, Worker(s.address) as w2:
               async with Client(s.address, asynchronous=True) as client:
                   future = client.submit(lambda x: x + 1, 10)
                   result = await future
                   print(result)

   asyncio.get_event_loop().run_until_complete(f())

Now we look at simpler examples that build up to this case.

Scheduler
---------

.. autosummary::
   Scheduler

We create scheduler by creating a ``Scheduler()`` object, and then ``await``
that object to wait for it to start up.  We can then wait on the ``.finished``
method to wait until it closes.  In the meantime the scheduler will be active
managing the cluster..

.. code-block:: python

   import asyncio
   from dask.distributed import Scheduler, Worker

   async def f():
       s = Scheduler()        # scheduler created, but not yet running
       s = await s            # the scheduler is running
       await s.finished()     # wait until the scheduler closes

   asyncio.get_event_loop().run_until_complete(f())

This program will run forever, or until some external process connects to the
scheduler and tells it to stop.  If you want to close things yourself you can
close any ``Scheduler``, ``Worker``, ``Nanny``, or ``Client`` class by awaiting
the ``.close`` method:

.. code-block:: python

   await s.close()


Worker
------

.. autosummary::
   Worker

The worker follows the same API.
The only difference is that the worker needs to know the address of the
scheduler.

.. code-block:: python

   import asyncio
   from dask.distributed import Scheduler, Worker

   async def f(scheduler_address):
       w = await Worker(scheduler_address)
       await w.finished()

   asyncio.get_event_loop().run_until_complete(f("tcp://127.0.0.1:8786"))


Start many in one event loop
----------------------------

.. autosummary::
   Scheduler
   Worker

We can run as many of these objects as we like in the same event loop.

.. code-block:: python

   import asyncio
   from dask.distributed import Scheduler, Worker

   async def f():
       s = await Scheduler()
       w = await Worker(s.address)
       await w.finished()
       await s.finished()

   asyncio.get_event_loop().run_until_complete(f())


Use Context Managers
--------------------

We can also use ``async with`` context managers to make sure that we clean up
properly.  Here is the same example as from above:

.. code-block:: python

   import asyncio
   from dask.distributed import Scheduler, Worker

   async def f():
       async with Scheduler() as s:
           async with Worker(s.address) as w:
               await w.finished()
               await s.finished()

   asyncio.get_event_loop().run_until_complete(f())

Alternatively, in the example below we also include a ``Client``, run a small
computation, and then allow things to clean up after that computation..

.. code-block:: python

   import asyncio
   from dask.distributed import Scheduler, Worker, Client

   async def f():
       async with Scheduler() as s:
           async with Worker(s.address) as w1, Worker(s.address) as w2:
               async with Client(s.address, asynchronous=True) as client:
                   future = client.submit(lambda x: x + 1, 10)
                   result = await future
                   print(result)

   asyncio.get_event_loop().run_until_complete(f())

This is equivalent to creating and ``awaiting`` each server, and then calling
``.close`` on each as we leave the context.
In this example we don't wait on ``s.finished()``, so this will terminate
relatively quickly.  You could have called ``await s.finished()`` though if you
wanted this to run forever.

Nanny
-----

.. autosummary::
   Nanny

Alternatively, we can replace ``Worker`` with ``Nanny`` if we want your workers
to be managed in a separate process.  The ``Nanny`` constructor follows the
same API. This allows workers to restart themselves in case of failure. Also,
it provides some additional monitoring, and is useful when coordinating many
workers that should live in different processes in order to avoid the GIL_.

.. code-block:: python

   # w = await Worker(s.address)
   w = await Nanny(s.address)

.. _GIL: https://docs.python.org/3/glossary.html#term-gil


API
---

These classes have a variety of keyword arguments that you can use to control
their behavior.  See the API documentation below for more information.

Scheduler
~~~~~~~~~
.. autoclass:: Scheduler
   :members:

Worker
~~~~~~

.. autoclass:: Worker
   :members:

Nanny
~~~~~

.. autoclass:: Nanny
   :members:
