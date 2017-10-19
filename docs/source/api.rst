.. _api:

API
===

.. currentmodule:: distributed.client

**Client**

.. autosummary::
   Client
   Client.call_stack
   Client.cancel
   Client.close
   Client.compute
   Client.gather
   Client.get
   Client.get_dataset
   Client.get_executor
   Client.get_metadata
   Client.get_scheduler_logs
   Client.get_worker_logs
   Client.has_what
   Client.list_datasets
   Client.map
   Client.ncores
   Client.persist
   Client.publish_dataset
   Client.profile
   Client.rebalance
   Client.replicate
   Client.restart
   Client.run
   Client.run_on_scheduler
   Client.scatter
   Client.scheduler_info
   Client.set_metadata
   Client.start_ipython_workers
   Client.start_ipython_scheduler
   Client.submit
   Client.unpublish_dataset
   Client.upload_file
   Client.who_has

.. currentmodule:: distributed

.. autosummary::
   worker_client
   get_worker
   get_client
   secede
   rejoin
   Reschedule

.. currentmodule:: distributed.recreate_exceptions

.. autosummary::
   ReplayExceptionClient.get_futures_error
   ReplayExceptionClient.recreate_error_locally

.. currentmodule:: distributed.client


**Future**

.. autosummary::
   Future
   Future.add_done_callback
   Future.cancel
   Future.cancelled
   Future.done
   Future.exception
   Future.result
   Future.traceback

**Client Coordination**

.. currentmodule:: distributed

.. autosummary:: Lock
.. autosummary:: Queue
.. autosummary:: Variable


**Other**

.. autosummary::
   as_completed
   distributed.diagnostics.progress
   wait
   fire_and_forget


Asynchronous methods
--------------------

Most methods and functions can be used equally well within a blocking or
asynchronous environment using Tornado coroutines.  If used within a Tornado
IOLoop then you should yield or await otherwise blocking operations
appropriately.

You must tell the client that you intend to use it within an asynchronous
environment by passing the ``asynchronous=True`` keyword

.. code-block:: python

   # blocking
   client = Client()
   future = client.submit(func, *args)  # immediate, no blocking/async difference
   result = client.gather(future)  # blocking

   # asynchronous Python 2/3
   client = yield Client(asynchronous=True)
   future = client.submit(func, *args)  # immediate, no blocking/async difference
   result = yield client.gather(future)  # non-blocking/asynchronous

   # asynchronous Python 3
   client = await Client(asynchronous=True)
   future = client.submit(func, *args)  # immediate, no blocking/async difference
   result = await client.gather(future)  # non-blocking/asynchronous

The asynchronous variants must be run within a Tornado coroutine.  See the
:doc:`Asynchronous <asynchronous>` documentation for more information.


Client
------

.. currentmodule:: distributed.client

.. autoclass:: Client
   :members:

.. autoclass:: distributed.recreate_exceptions.ReplayExceptionClient
   :members:


Future
------

.. autoclass:: Future
   :members:


Other
-----

.. autofunction:: as_completed
.. autofunction:: distributed.diagnostics.progress
.. autofunction:: wait

.. currentmodule:: distributed

.. autofunction:: distributed.worker_client
.. autofunction:: distributed.get_worker
.. autofunction:: distributed.get_client
.. autofunction:: distributed.secede
.. autofunction:: distributed.rejoin

.. autoclass:: Queue
   :members:
.. autoclass:: Variable
   :members:


Asyncio Client
--------------

.. currentmodule:: distributed.asyncio
.. autoclass:: AioClient
   :members:


Adaptive
--------

.. currentmodule:: distributed.deploy
.. autoclass:: Adaptive
   :members:
