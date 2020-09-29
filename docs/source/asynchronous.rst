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


If you want to use an asynchronous function with a synchronous ``Client``
(one made without the ``asynchronous=True`` keyword) then you can apply the
``asynchronous=True`` keyword at each method call and use the ``Client.sync``
function to run the asynchronous function:

.. code-block:: python

   from dask.distributed import Client

   client = Client()  # normal blocking client

   async def f():
       future = client.submit(lambda x: x + 1, 10)
       result = await client.gather(future, asynchronous=True)
       return result

   client.sync(f)


.. note: Blocking operations like the .compute() method aren’t ok to use in
         asynchronous mode. Instead you’ll have to use the Client.compute
         method


.. code-block:: python

    async with Client(asynchronous=True) as client:
        arr = da.random.random((1000, 1000), chunks=(1000, 100))
        await client.compute(arr.mean())


Example
-------

This self-contained example starts an asynchronous client, submits a trivial
job, waits on the result, and then shuts down the client. You can see
implementations for Asyncio and Tornado.

Python 3 with Tornado or Asyncio
++++++++++++++++++++++++++++++++

.. code-block:: python

   from dask.distributed import Client

   async def f():
       client = await Client(asynchronous=True)
       future = client.submit(lambda x: x + 1, 10)
       result = await future
       await client.close()
       return result

   # Either use Tornado
   from tornado.ioloop import IOLoop
   IOLoop().run_sync(f)

   # Or use asyncio
   import asyncio
   asyncio.get_event_loop().run_until_complete(f())


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
