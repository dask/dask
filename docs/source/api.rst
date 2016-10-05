API
===

.. currentmodule:: distributed.client

**Client**

.. autosummary::
   Client
   Client.cancel
   Client.compute
   Client.gather
   Client.get
   Client.get_dataset
   Client.has_what
   Client.list_datasets
   Client.map
   Client.ncores
   Client.persist
   Client.publish_dataset
   Client.rebalance
   Client.replicate
   Client.restart
   Client.run
   Client.scatter
   Client.shutdown
   Client.scheduler_info
   Client.shutdown
   Client.start_ipython_workers
   Client.start_ipython_scheduler
   Client.submit
   Client.unpublish_dataset
   Client.upload_file
   Client.who_has

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

**Other**

.. autosummary::
   as_completed
   distributed.diagnostics.progress
   wait

Asynchronous methods
--------------------

If you desire Tornado coroutines rather than typical functions these can
commonly be found as underscore-prefixed versions of the functions above.  For
example the ``client.restart()`` method can be replaced in an asynchronous workflow
with ``yield client._restart()``.  Many methods like ``client.compute`` are non-blocking
regardless; these do not have a coroutine-equivalent.

.. code-block:: python

   client.restart()  # synchronous
   yield client._restart()  # non-blocking


Client
--------

.. autoclass:: Client
   :members:

CompatibleExecutor
------------------

.. autoclass:: CompatibleExecutor
    :members: map

Future
------

.. autoclass:: Future
   :members:


Other
-----

.. autofunction:: as_completed
.. autofunction:: distributed.diagnostics.progress
.. autofunction:: wait
