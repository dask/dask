Choosing between Schedulers
===========================

Dask enables you to run your computations on a variety of schedulers that use
different technologies, ranging from a single thread, to multiple processes, to
a distributed cluster.  This document helps explain the differences between the
different choices so that users can make decisions that improve performance.

Briefly, the current options are as follows:

*   ``dask.threaded.get``: Uses multiple threads in the same process.  Good for
    numeric code that releases the GIL_ (NumPy, Pandas, SKLearn, Numba) because
    data is free to share.  The default scheduler for ``dask.array``,
    ``dask.dataframe`` and ``dask.delayed``
*   ``dask.multiprocessing.get``: Uses multiple processes.  Good for Python
    bound code that needs multiple interpreters to accelerate.  There are some
    costs to sharing data back and forth between processes.  The default
    scheduler for ``dask.bag`` and sometimes useful with ``dask.dataframe``.
*   ``dask.async.get_sync``: Uses the single main thread.  Good for profiling
    and debugging because all code is run sequentially
*   ``distributed.Client.get``:  Uses multiple machines connected over
    sockets.  Good for larger work but also a viable alternative to
    ``dask.multiprocessing`` on a single machine.  Also sometimes used for its
    improved diagnostic tools.

Threads vs Processes
--------------------

Threads are good because they can share data back and forth in the same memory
space without transfer costs.  Threads can pass large arrays between each other
instantaneously.  Unfortunately due to the GIL_ pure Python code (like JSON
parsing) does not parallelize well under threads, and so when computing on pure
Python objects, like strings or lists or our custom objects, we may prefer to
use processes.  Threads are good when using numeric data and when the
computation is complex with many cross-dependencies.

Processes don't have issues with the GIL, but data transfer between cores when
using processes can be expensive.  Data transfer isn't an issue for
embarrassingly parallel computations, which is fortunately the norm when
dealing with pure Python computations.

Single Threaded Scheduler
-------------------------

Debugging, profiling, and general comprehension of code is hard when computing
in parallel.  Standard tools like ``pdb`` or ``cProfile`` fail to operate well
when running under multiple threads or processes.  To resolve this problem
there is a dask scheduler, ``dask.async.get_sync`` that doesn't actually run in
parallel, but instead steps through your graph in the main thread.  It
otherwise operates exactly like the threaded and multiprocessing schedulers,
and so is a faithful proxy when tracking down difficult issues.


Distributed Scheduler on a Cluster
----------------------------------

The distributed scheduler is more sophisticated than the single machine
schedulers (threaded, multiprocessing, and synchronous all share a common
codebase).  This is good because it can be significantly smarter about how it
runs computations.  However this also introduces extra conceptual overhead and
potential setup costs.

The primary reason to use the distributed scheduler is to use multiple machines
on a distributed cluster.  This allows computations to scale to significantly
larger problems.  This doesn't come for free though, as you will need to `setup
the distributed scheduler`_ on those machines.

.. _`setup the distributed scheduler`: https://distributed.readthedocs.io/en/latest/setup.html

Distributed Scheduler on a Single Machine
-----------------------------------------

It is also reasonable to use the `distributed scheduler`_ on a single machine.
The algorithms, reporting, and diagnostics in this scheduler are more effective
in some cases.  You can create a local "cluster" and use this scheduler by
default by creating a ``dask.distributed.Client`` with no arguments.

.. code-block:: python

   from dask.distributed import Client
   client = Client(set_as_default=True)

.. _`distributed scheduler`: https://distributed.readthedocs.io/en/latest/

Diagnostics
~~~~~~~~~~~

One reason to do this is to get access to the pleasant `web interface`_, which
gives a real-time visualization of what's computing on your cores.

.. _`web interface`: https://distributed.readthedocs.io/en/latest/web.html

Asynchronous Interface
~~~~~~~~~~~~~~~~~~~~~~

The distributed scheduler also provides asynchronous computation, where you can
submit a computation to run in the background, and only collect its results
later

Data Locality
~~~~~~~~~~~~~

The distributed scheduler is sometimes more efficient than the multiprocessing
scheduler, particularly when tasks have complex dependency structures and require
non-trivial communication.

Because of how the standard library's ``multiprocessing.Pool`` works, the
multiprocessing scheduler always brings intermediate results back to the master
process and then sends them out to a worker process afterwards if further work
needs to be done.  This back-and-forth communication can dominate costs and
slow down overall performance.  The distributed scheduler does not have this
flaw, can reason well about data-in-place, and can move small pieces of data to
larger ones.

.. _GIL: https://docs.python.org/3/glossary.html#term-gil
