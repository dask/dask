Single Machine
--------------

.. note::

   We recommend most users use the :doc:`Distributed Scheduler <scheduling-distributed>` which often provides a better user
   experience today, even on a single machine.

If you import Dask, set up a computation, and then call ``compute``, then you
will use the single-machine scheduler by default.

For different computations you may find better performance with particular single-machine scheduler settings.
This document helps you understand how to choose between and configure different schedulers,
and provides guidelines on when one might be more appropriate.


Local Threads
-------------

.. code-block:: python

   import dask
   dask.config.set(scheduler='threads')  # overwrite default with threaded scheduler

The threaded scheduler executes computations with a local
``concurrent.futures.ThreadPoolExecutor``.
It is lightweight and requires no setup.
It introduces very little task overhead (around 50us per task)
and, because everything occurs in the same process,
it incurs no costs to transfer data between tasks.
However, due to Python's Global Interpreter Lock (GIL),
this scheduler only provides parallelism when your computation is dominated by non-Python code,
as is primarily the case when operating on numeric data in NumPy arrays, Pandas DataFrames,
or using any of the other C/C++/Cython based projects in the ecosystem.

The threaded scheduler is the default choice for
:doc:`Dask Array <array>`, :doc:`Dask DataFrame <dataframe>`, and :doc:`Dask Delayed <delayed>`.

.. code-block:: python

   import dask.dataframe as dd
   df = dd.read_csv(...)
   df.x.sum().compute()  # This uses the single-machine scheduler by default

However, if your computation is dominated by processing pure Python objects
like strings, dicts, or lists,
then you may want to try one of the process-based schedulers below
(we currently recommend the distributed scheduler on a local machine).

Local Processes
---------------

.. code-block:: python

   import dask
   dask.config.set(scheduler='processes')  # overwrite default with multiprocessing scheduler


The multiprocessing scheduler executes computations with a local
``concurrent.futures.ProcessPoolExecutor``.
It is lightweight to use and requires no setup.
Every task and all of its dependencies are shipped to a local process,
executed, and then their result is shipped back to the main process.
This means that it is able to bypass issues with the GIL and provide parallelism
even on computations that are dominated by pure Python code,
such as those that process strings, dicts, and lists.

However, moving data to remote processes and back can introduce performance penalties,
particularly when the data being transferred between processes is large.
The multiprocessing scheduler is an excellent choice when workflows are relatively linear,
and so does not involve significant inter-task data transfer
as well as when inputs and outputs are both small, like filenames and counts.

This is common in basic data ingestion workloads,
such as those are common in :doc:`Dask Bag <bag>`,
where the multiprocessing scheduler is the default:

.. code-block:: python

   >>> import dask.bag as db
   >>> db.read_text('*.json').map(json.loads).pluck('name').frequencies().compute()
   {'alice': 100, 'bob': 200, 'charlie': 300}

For more complex workloads,
where large intermediate results may be depended upon by multiple downstream tasks,
we generally recommend the use of the distributed scheduler on a local machine.
The distributed scheduler is more intelligent about moving around large intermediate results.

.. _single-threaded-scheduler:

Single Thread
-------------

.. code-block:: python

   import dask
   dask.config.set(scheduler='synchronous')  # overwrite default with single-threaded scheduler

The single-threaded synchronous scheduler executes all computations in the local thread
with no parallelism at all.
This is particularly valuable for debugging and profiling,
which are more difficult when using threads or processes.

For example, when using IPython or Jupyter notebooks, the ``%debug``, ``%pdb``, or ``%prun`` magics
will not work well when using the parallel Dask schedulers
(they were not designed to be used in a parallel computing context).
However, if you run into an exception and want to step into the debugger,
you may wish to rerun your computation under the single-threaded scheduler
where these tools will function properly.

Configuration
-------------

You can configure the global default scheduler by using the ``dask.config.set(scheduler...)`` command.
This can be done globally:

.. code-block:: python

   dask.config.set(scheduler='threads')

   x.compute()

or as a context manager:

.. code-block:: python

   with dask.config.set(scheduler='threads'):
       x.compute()

or within a single compute call:

.. code-block:: python

   x.compute(scheduler='threads')

Each scheduler may support extra keywords specific to that scheduler. For example,
the pool-based single-machine scheduler allows you to provide custom pools or
specify the desired number of workers:

.. code-block:: python

   from concurrent.futures import ThreadPoolExecutor
   with dask.config.set(pool=ThreadPoolExecutor(4)):
       x.compute()

   with dask.config.set(num_workers=4):
       x.compute()

Note that Dask also supports custom ``concurrent.futures.Executor`` subclasses,
such as the ``ReusablePoolExecutor`` from loky_:

.. _loky: https://github.com/joblib/loky

.. code-block:: python

   from loky import get_reusable_executor
   with dask.config.set(scheduler=get_reusable_executor()):
       x.compute()

Other libraries like ipyparallel_ and mpi4py_ also supply
``concurrent.futures.Executor`` subclasses that could be used as well.

.. _ipyparallel: https://ipyparallel.readthedocs.io/en/latest/examples/Futures.html#Executors
.. _mpi4py: https://mpi4py.readthedocs.io/en/latest/mpi4py.futures.html
