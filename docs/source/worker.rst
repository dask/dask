Worker
======

Overview
--------

Workers provide two functions:

1.  Compute tasks as directed by the scheduler
2.  Store and serve computed results to other workers or clients

Each worker contains a ThreadPool that it uses to evaluate tasks as requested
by the scheduler.  It stores the results of these tasks locally and serves them
to other workers or clients on demand.  If the worker is asked to evaluate a
task for which it does not have all of the necessary data then it will reach
out to its peer workers to gather the necessary dependencies.

A typical conversation between a scheduler and two workers Alice and Bob may
look like the following::

   Scheduler -> Alice:  Compute ``x <- add(1, 2)``!
   Alice -> Scheduler:  I've computed x and am holding on to it!

   Scheduler -> Bob:    Compute ``y <- add(x, 10)``!
                        You will need x.  Alice has x.
   Bob -> Alice:        Please send me x.
   Alice -> Bob:        Sure.  x is 3!
   Bob -> Scheduler:    I've computed y and am holding on to it!

Storing Data
------------

Data is stored locally in a dictionary in the ``.data`` attribute that
maps keys to the results of function calls.

.. code-block:: python

   >>> worker.data
   {'x': 3,
    'y': 13,
    ...
    '(df, 0)': pd.DataFrame(...),
    ...
    }

This ``.data`` attribute is a ``MutableMapping`` that is typically a
combination of in-memory and on-disk storage with an LRU policy to move data
between them.

Spill Excess Data to Disk
-------------------------

Short version: To enable workers to spill excess data to disk start
``dask-worker`` with the ``--memory-limit`` option.  Either giving ``auto`` to
have it guess how many bytes to keep in memory or an integer, if you know the
number of bytes it should use::

    $ dask-worker scheduler:port --memory-limit=auto  # 75% of available RAM
    $ dask-worker scheduler:port --memory-limit=2e9  # two gigabytes

Some workloads may produce more data at one time than there is available RAM on
the cluster.  In these cases Workers may choose to write excess values to disk.
This causes some performance degradation because writing to and reading from
disk is generally slower than accessing memory, but is better than running out
of memory entirely, which can cause the system to halt.

If the ``dask-worker --memory-limit=NBYTES`` keyword is set during
initialization then the worker will store at most NBYTES of data (as measured
with ``sizeof``) in memory.  After that it will start storing least recently
used (LRU) data in a temporary directory.   Workers serialize data for writing
to disk with the same system used to write data on the wire, a combination of
``pickle`` and the default compressor.

Now whenever new data comes in it will push out old data until at most NBYTES
of data is in RAM.  If an old value is requested it will be read from disk,
possibly pushing other values down.

It is still possible to run out of RAM on a worker.  Here are a few possible
issues:

1.  The objects being stored take up more RAM than is stated with the
    `__sizeof__ protocol <https://docs.python.org/3/library/sys.html#sys.getsizeof>`_.
    If you use custom classes then we encourage adding a faithful
    ``__sizeof__`` method to your class that returns an accurate accounting of
    the bytes used.
2.  Computations and communications may take up additional RAM not accounted
    for.  It is wise to have a suitable buffer of memory that can handle your
    most expensive function RAM-wise running as many times as there are active
    threads on the machine.
3.  It is possible to misjudge the amount of RAM on the machine.  Using the
    ``--memory-limit=auto`` heuristic sets the value to 75% of the return value
    of ``psutil.virtual_memory().total``.


Thread Pool
-----------

Each worker sends computations to a thread in a
`concurrent.futures.ThreadPoolExecutor <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_
for computation.  These computations occur in the same process as the Worker
communication server so that they can access and share data efficiently between
each other.  For the purposes of data locality all threads within a worker are
considered the same worker.

If your computations are mostly numeric in nature (for example NumPy and Pandas
computations) and release the GIL entirely then it is advisable to run
``dask-worker`` processes with many threads and one process.  This reduces
communication costs and generally simplifies deployment.

If your computations are mostly Python code and don't release the GIL then it
is advisable to run ``dask-worker`` processes with many processes and one
thread per core::

   $ dask-worker scheduler:8786 --nprocs 8

If your computations are external to Python and long-running and don't release
the GIL then beware that while the computation is running the worker process
will not be able to communicate to other workers or to the scheduler.  This
situation should be avoided.  If you don't link in your own custom C/Fortran
code then this topic probably doesn't apply to you.

Command Line tool
-----------------

Use the ``dask-worker`` command line tool to start an individual worker.  Here
are the available options::

   $ dask-worker --help
   Usage: dask-worker [OPTIONS] SCHEDULER

   Options:
     --worker-port INTEGER  Serving worker port, defaults to randomly assigned
     --http-port INTEGER    Serving http port, defaults to randomly assigned
     --nanny-port INTEGER   Serving nanny port, defaults to randomly assigned
     --port INTEGER         Deprecated, see --nanny-port
     --host TEXT            Serving host. Defaults to an ip address that can
                            hopefully be visible from the scheduler network.
     --nthreads INTEGER     Number of threads per process. Defaults to number of
                            cores
     --nprocs INTEGER       Number of worker processes.  Defaults to one.
     --name TEXT            Alias
     --memory-limit TEXT     Number of bytes before spilling data to disk
     --no-nanny
     --help                 Show this message and exit.


Internal Scheduling
-------------------

Internally tasks that come to the scheduler proceed through the following
pipeline:

.. image:: images/worker-task-state.svg
    :alt: Dask worker task states

As tasks arrive they are prioritized and put into a heap.  They are then taken
from this heap in turn to have any remote dependencies collected.  For each
dependency we select a worker at random that has that data and collect the
dependency from that worker.  To improve bandwidth we opportunistically gather
other dependencies of other tasks are known to be on that worker, up to a
maximum of 100MB of data (too little data and bandwidth suffers, too much data
and responsiveness suffers).  We use a fixed number of connections (around
10-20) so as to avoid overly-fragmenting our network bandwidth.  After all
dependencies for a task are in memory we transition the task to the ready state
and put the task again into a heap of tasks that are ready to run.

We collect from this heap and put the task into a thread from a local thread
pool to execute.

Optionally, this task may identify itself as a long-running task (see
:doc:`Tasks launching tasks <task-launch>`), at which point it secedes from the
thread pool.

A task either errs or its result is put into memory.  In either case a response
is sent back to the scheduler.



API Documentation
-----------------

.. autoclass:: distributed.worker.Worker
