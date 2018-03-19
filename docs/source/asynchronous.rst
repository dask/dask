Asynchronous Operation
======================

Dask can run fully asynchronously and so interoperate with other highly
concurrent applications.  Internally Dask is built on top of Tornado coroutines
but also has a compatibility layer for asyncio (see below).

Basic Operation
---------------

When starting a client provide the ``asynchronous=True`` keyword to tell Dask
that you intend to use this client within an asynchronous context, such as a
function defined with ``async/await`` syntax.

.. code-block:: python

   async def f():
       client = await Client(asynchronous=True)

Operations that used to block now provide Tornado coroutines on which you can
``await``.

Fast functions that only submit work remain fast and don't need to be awaited.
This includes all functions that submit work to the cluster, like ``submit``,
``map``, ``compute``, and ``persist``.

.. code-block:: python

   future = client.submit(lambda x: x + 1, 10)

You can await futures directly

.. code-block:: python

   result = await future

   >>> print(result)
   11

Or you can use the normal client methods.  Any operation that waited until it
received information from the scheduler should now be ``await``'ed.

.. code-block:: python

   result = await client.gather(future)

If you want to reuse the same client in asynchronous and synchronous
environments you can apply the ``asynchronous=True`` keyword at each method
call.

.. code-block:: python

   client = Client()  # normal blocking client

   async def f():
       futures = client.map(func, L)
       results = await client.gather(futures, asynchronous=True)
       return results

AsyncIO
-------

If you prefer to use the Asyncio event loop over the Tornado event loop you
should use the ``AioClient``.

.. code-block:: python

   from distributed.asyncio import AioClient
   client = await AioClient()

All other operations remain the same:

.. code-block:: python

   future = client.submit(lambda x: x + 1, 10)
   result = await future
   # or
   result = await client.gather(future)

Python 2 Compatibility
----------------------

Everything here works with Python 2 if you replace ``await`` with ``yield``.
See more extensive comparison in the example below.

Example
-------

This self-contained example starts an asynchronous client, submits a trivial
job, waits on the result, and then shuts down the client.  You can see
implementations for Python 2 and 3 and for Asyncio and Tornado.

Python 3 with Tornado
+++++++++++++++++++++

.. code-block:: python

   from dask.distributed import Client

   async def f():
       client = await Client(asynchronous=True)
       future = client.submit(lambda x: x + 1, 10)
       result = await future
       await client.close()
       return result

   from tornado.ioloop import IOLoop
   IOLoop().run_sync(f)

Python 2/3 with Tornado
+++++++++++++++++++++++

.. code-block:: python

   from dask.distributed import Client
   from tornado import gen

   @gen.coroutine
   def f():
       client = yield Client(asynchronous=True)
       future = client.submit(lambda x: x + 1, 10)
       result = yield future
       yield client.close()
       raise gen.Result(result)

   from tornado.ioloop import IOLoop
   IOLoop().run_sync(f)

Python 3 with Asyncio
+++++++++++++++++++++

.. code-block:: python

   from distributed.asyncio import AioClient

   async def f():
       client = await AioClient()
       future = client.submit(lambda x: x + 1, 10)
       result = await future
       await client.close()
       return result

   from asyncio import get_event_loop
   get_event_loop().run_until_complete(f())

Use Cases
---------

Historically this has been used in a few kinds of applications:

1.  To integrate Dask into other asynchronous services (such as web backends),
    supplying a computational engine similar to Celery, but while still
    maintaining a high degree of concurrency and not blocking needlessly.

2.  For computations that change or update state very rapidly, such as is
    common in some advanced machine learning workloads.

3.  To develop the internals of Dask's distributed infrastucture, which is
    written entirely in this style.

4.  For complex control and data structures in advanced applications.
