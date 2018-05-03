API
===

Dask APIs generally follow from upstream APIs:

-  The :doc:`Dask Array API <array-api>` follows the Numpy API
-  The :doc:`Dask Dataframe API <dataframe-api>` follows the Pandas API
-  The `Dask-ML API <https://dask-ml.readthedocs.io/en/latest/modules/api>`_ follows the Scikit-Learn API and other related machine learning libraries
-  The :doc:`Dask Bag API <bag-api>` follows the map/filter/groupby/reduce API common in PySpark, PyToolz, and the Python standard library
-  The :doc:`Dask Delayed API <delayed-api>` wraps general Python code
-  The :doc:`Real-time Futures API <futures>` follows the `concurrent.futures <https://docs.python.org/3/library/concurrent.futures.html>`_ API from the standard library.

Additionally, Dask has its own functions to start computations, persist data in
memory, check progress, and so forth that complement the APIs above.
These more general Dask functions are described below:

.. currentmodule:: dask

.. autosummary::
   compute
   is_dask_collection
   optimize
   persist
   visualize

These functions work with any scheduler.  More advanced operations are
available when using the newer scheduler and starting a
:obj:`dask.distributed.Client` (which, despite its name, runs nicely on a
single machine).  This API provides the ability to submit, cancel, and track
work asynchronously, and includes many functions for complex inter-task
workflows.  These are not necessary for normal operation, but can be useful for
real-time or advanced operation.

This more advanced API is available in the `Dask distributed documentation
<https://distributed.readthedocs.org/en/latest/api.html>`_

.. autofunction:: compute
.. autofunction:: is_dask_collection
.. autofunction:: optimize
.. autofunction:: persist
.. autofunction:: visualize
