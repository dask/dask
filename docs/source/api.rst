API
===


.. currentmodule:: distributed.executor

**Executor**

.. autosummary::
   Executor
   Executor.submit
   Executor.map
   Executor.scatter
   Executor.gather
   Executor.get
   Executor.compute
   Executor.upload_file
   Executor.restart

**Future**

.. autosummary::
   Future
   Future.result
   Future.exception
   Future.traceback
   Future.done
   Future.cancelled

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
