Execution
=========

Execution on bags provide two benefits:

1.  Streaming: data processes lazily, allowing smooth execution of
    larger-than-memory data
2.  Parallel: data is split up, allowing multiple cores to execute in parallel.


Trigger Evaluation
------------------

Bags have a ``.compute()`` method to trigger computation:

.. code-block:: python

   >>> c = b.map(func)
   >>> c.compute()
   [1, 2, 3, 4, ...]

You must ensure that your result will fit in memory:

Bags also support the ``__iter__``
protocol and so work well with pythonic collections like ``list, tuple, set,
dict``.  Converting your object into a list or dict can look more Pythonic
than calling ``.compute()``:

.. code-block:: python

   >>> list(b.map(lambda x: x + 1))
   [1, 2, 3, 4, ...]

   >>> dict(b.frequencies())
   {'Alice': 100, 'Bob': 200, ...}


Default scheduler
-----------------

By default ``dask.bag`` uses ``dask.multiprocessing`` for computation.  As a
benefit dask bypasses the GIL and uses multiple cores on Pure Python objects.
As a drawback dask.bag doesn't perform well on computations that include a
great deal of inter-worker communication.  For common operations this is
rarely an issue as most ``dask.bag`` workflows are embarrassingly parallel or
result in reductions with little data moving between workers.

Additionally, using multiprocessing opens up potential problems with function
serialization (see below).

Shuffle
-------

Some operations, like full ``groupby`` and bag-to-bag ``join`` do require
substantial inter-worker communication.  These are handled specially by shuffle
operations that use disk and a central memory server as a central point of
communication.

Shuffle operations are expensive and better handled by projects like
``dask.dataframe``.  It is best to use ``dask.bag`` to clean and process data,
then transform it into an array or dataframe before embarking on the more
complex operations that require shuffle steps.

Dask.bag uses partd_ to perform efficient, parallel, spill-to-disk shuffles.

.. _partd: https://github.com/mrocklin/partd


Function Serialization and Error Handling
-----------------------------------------

Dask.bag uses cloudpickle_ to serialize functions to send to worker processes.
cloudpickle supports almost any kind of function, including lambdas, closures,
partials and functions defined interactively.

When an error occurs in a remote process the dask schedulers record the
Exception and the traceback and delivers these to the main process.  These
tracebacks can not be navigated (i.e. you can't use ``pdb``) but still contain
valuable contextual information.

These two features are arguably the most important when comparing ``dask.bag``
to direct use of ``multiprocessing``.

If you would like to turn off multiprocessing you can do so by setting the
default get function to the synchronous single-core scheduler:

.. code-block:: python

   >>> from dask.async import get_sync
   >>> b.compute(get=get_sync)

   or

   >>> import dask
   >>> dask.set_options(get=get_sync)  # set global
   >>> list(b)  # uses synchronous scheduler

.. _cloudpickle: https://github.com/cloudpipe/cloudpickle
