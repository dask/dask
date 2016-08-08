API
===


.. currentmodule:: distributed.executor

**Executor**

.. autosummary::
   Executor
   Executor.cancel
   Executor.compute
   Executor.gather
   Executor.get
   Executor.has_what
   Executor.map
   Executor.ncores
   Executor.persist
   Executor.rebalance
   Executor.replicate
   Executor.restart
   Executor.run
   Executor.scatter
   Executor.shutdown
   Executor.submit
   Executor.upload_file
   Executor.who_has

**Future**

.. autosummary::
   Future
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
example the ``e.restart()`` method can be replaced in an asynchronous workflow
with ``yield e._restart()``.  Many methods like ``e.compute`` are non-blocking
regardless; these do not have a coroutine-equivalent.

.. code-block:: python

   e.restart()  # synchronous
   yield e._restart()  # non-blocking


Executor
--------

.. autoclass:: Executor
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
