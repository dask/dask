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
   Executor.map
   Executor.persist
   Executor.restart
   Executor.run
   Executor.scatter
   Executor.shutdown
   Executor.submit
   Executor.upload_file

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
