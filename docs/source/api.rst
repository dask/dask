API Reference
=============

Dask APIs generally follow from upstream APIs:

-  :doc:`Arrays<array-api>` follows NumPy
-  :doc:`DataFrames <dataframe-api>` follows Pandas
-  :doc:`Bag <bag-api>` follows map/filter/groupby/reduce common in Spark and Python iterators
-  :doc:`Delayed <delayed-api>` wraps general Python code
-  :doc:`Futures <futures>` follows `concurrent.futures <https://docs.python.org/3/library/concurrent.futures.html>`_ from the standard library for real-time computation.

.. toctree::
   :maxdepth: 1
   :hidden:

   Array <array-api.rst>
   DataFrame <dataframe-api.rst>
   Bag <bag-api.rst>
   Delayed <delayed-api.rst>
   Futures <futures>


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
<https://distributed.dask.org/en/latest/api.html>`_

.. autofunction:: annotate
.. autofunction:: compute
.. autofunction:: is_dask_collection
.. autofunction:: optimize
.. autofunction:: persist
.. autofunction:: visualize

Datasets
--------

Dask has a few helpers for generating demo datasets

.. currentmodule:: dask.datasets

.. autofunction:: make_people
.. autofunction:: timeseries

.. _api.utilities:

Utilities
---------

Dask has some public utility methods. These are primarily used for parsing
configuration values.

.. currentmodule:: dask.utils

.. autofunction:: format_bytes
.. autofunction:: format_time
.. autofunction:: parse_bytes
.. autofunction:: parse_timedelta
