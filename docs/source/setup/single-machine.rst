Single-Machine Scheduler
========================

The default Dask scheduler provides parallelism on a single machine by using
either threads or processes.  It is the default choice used by Dask because it
requires no setup. You don't need to make any choices or set anything up to
use this scheduler. However, you do have a choice between threads and processes:

1.  **Threads**: Use multiple threads in the same process.  This option is good
    for numeric code that releases the GIL_ (like NumPy, Pandas, Scikit-Learn,
    Numba, ...) because data is free to share.  This is the default scheduler for
    ``dask.array``, ``dask.dataframe``, and ``dask.delayed``

2.  **Processes**: Send data to separate processes for processing.  This option
    is good when operating on pure Python objects like strings or JSON-like
    dictionary data that holds onto the GIL_, but not very good when operating
    on numeric data like Pandas DataFrames or NumPy arrays.  Using processes
    avoids GIL issues, but can also result in a lot of inter-process
    communication, which can be slow.  This is the default scheduler for
    ``dask.bag``, and it is sometimes useful with ``dask.dataframe``

    Note that the ``dask.distributed`` scheduler is often a better choice when
    working with GIL-bound code.  See :doc:`dask.distributed on a single
    machine <single-distributed>`

3.  **Single-threaded**: Execute computations in a single thread.  This option
    provides no parallelism, but is useful when debugging or profiling.
    Turning your parallel execution into a sequential one can be a convenient
    option in many situations where you want to better understand what is going
    on

.. _GIL: https://docs.python.org/3/glossary.html#term-gil


Selecting Threads, Processes, or Single Threaded
------------------------------------------------

You can select between these options by specifying one of the following three
values to the ``scheduler=`` keyword:

-  ``"threads"``: Uses a ThreadPool in the local process
-  ``"processes"``: Uses a ProcessPool to spread work between processes
-  ``"single-threaded"``: Uses a for-loop in the current thread

You can specify these options in any of the following ways:

-   When calling ``.compute()``

    .. code-block:: python

       x.compute(scheduler='threads')

-   With a context manager

    .. code-block:: python

       with dask.config.set(scheduler='threads'):
           x.compute()
           y.compute()

-   As a global setting

    .. code-block:: python

       dask.config.set(scheduler='threads')


Use the Distributed Scheduler
-----------------------------

Dask's newer distributed scheduler also works well on a single machine and
offers more features and diagnostics.  See :doc:`this page
<single-distributed>` for more information.
