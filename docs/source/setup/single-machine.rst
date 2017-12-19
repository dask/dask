Single-Machine Scheduler
========================

The default Dask scheduler provides parallelism on a single machine by using
either threads or processes.  It is the default choice used by Dask because it
requires no setup.  You don't need to make any choices or set anything up to
use this scheduler, however you do have a choice between threads and processes:

1.  **Threads**: Use multiple threads in the same process.  This option is good
    for numeric code that releases the GIL_ (like NumPy, Pandas, Scikit-Learn,
    Numba, ...) because data is free to share.  This is the default scheduler for
    ``dask.array``, ``dask.dataframe``, and ``dask.delayed``

2.  **Processes**: Send data to separate processes for processing.  This option
    is good when operating on pure Python objects like strings or JSON-like
    dictionary data that holds onto the GIL_ but not very good when operating
    on numeric data like Pandas dataframes or NumPy arrays.  Using processes
    avoids GIL issues but can also result in a lot of inter-process
    communication, which can be slow.  This is the default scheduler for
    ``dask.bag`` and is sometimes useful with ``dask.dataframe``.

    Note that the dask.distributed scheduler is often a better choice when
    working with GIL-bound code.  See :doc:`Dask.distributed on a single
    machine <single-distributed>`.

3.  **Single-threaded**: Execute computations in a single thread.  This option
    provides no parallelism, but is useful when debugging or profiling.
    Turning your parallel execution into a sequential one can be a convenient
    option in many situations where you want to better understand what is going
    on.

.. _GIL: https://docs.python.org/3/glossary.html#term-gil


Selecting Threads, Processes, or Single Threaded
------------------------------------------------

Currently these options are available by selecting different ``get`` functions:

-  ``dask.threaded.get``: The threaded scheduler
-  ``dask.multiprocessing.get``: The multiprocessing scheduler
-  ``dask.local.get_sync``: The single-threaded scheduler

You can specify these functions in any of the following ways:

-   When calling ``.compute()``

    .. code-block:: python

       x.compute(get=dask.threaded.get)

-   With a context manager

    .. code-block:: python

       with dask.set_options(get=dask.threaded.get):
           x.compute()
           y.compute()

-   As a global setting

    .. code-block:: python

       dask.set_options(get=dask.threaded.get)


Use the Distributed Scheduler
-----------------------------

The newer dask.distributed scheduler also works well on a single machine and
offers more features and diagnostics.  See :doc:`this page
<single-distributed>` for more information.
