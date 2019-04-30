Debugging
=========

Debugging parallel programs is hard.  Normal debugging tools like logging and
using ``pdb`` to interact with tracebacks stop working normally when exceptions
occur in far-away machines, different processes, or threads.

Dask has a variety of mechanisms to make this process easier.  Depending on
your situation, some of these approaches may be more appropriate than others.

These approaches are ordered from lightweight or easy solutions to more
involved solutions.

Exceptions
----------

When a task in your computation fails, the standard way of understanding what
went wrong is to look at the exception and traceback.  Often people do this
with the ``pdb`` module, IPython ``%debug`` or ``%pdb`` magics, or by just
looking at the traceback and investigating where in their code the exception
occurred.

Normally when a computation executes in a separate thread or a different
machine, these approaches break down.  To address this, Dask provides a few 
mechanisms to recreate the normal Python debugging experience.

Inspect Exceptions and Tracebacks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, Dask already copies the exception and traceback wherever they
occur and reraises that exception locally.  If your task failed with a
``ZeroDivisionError`` remotely, then you'll get a ``ZeroDivisionError`` in your
interactive session.  Similarly you'll see a full traceback of where this error
occurred, which, just like in normal Python, can help you to identify the
troublesome spot in your code.

However, you cannot use the ``pdb`` module or ``%debug`` IPython magics with
these tracebacks to look at the value of variables during failure.  You can
only inspect things visually.  Additionally, the top of the traceback may be
filled with functions that are Dask-specific and not relevant to your
problem, so you can safely ignore these.

Both the single-machine and distributed schedulers do this.


Use the Single-Threaded Scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dask ships with a simple single-threaded scheduler.  This doesn't offer any
parallel performance improvements but does run your Dask computation
faithfully in your local thread, allowing you to use normal tools like ``pdb``,
``%debug`` IPython magics, the profiling tools like the ``cProfile`` module, and
`snakeviz <https://jiffyclub.github.io/snakeviz/>`_.  This allows you to use
all of your normal Python debugging tricks in Dask computations, as long as you
don't need parallelism.

The single-threaded scheduler can be used, for example, by setting
``scheduler='single-threaded'`` in a compute call:

.. code-block:: python

    >>> x.compute(scheduler='single-threaded')

For more ways to configure schedulers, see the :ref:`scheduler configuration
documentation <scheduling-configuration>`.

This only works for single-machine schedulers.  It does not work with
``dask.distributed`` unless you are comfortable using the Tornado API (look at the
`testing infrastructure
<https://distributed.dask.org/en/latest/develop.html#writing-tests>`_
docs, which accomplish this).  Also, because this operates on a single machine,
it assumes that your computation can run on a single machine without exceeding
memory limits.  It may be wise to use this approach on smaller versions of your
problem if possible.


Rerun Failed Task Locally
~~~~~~~~~~~~~~~~~~~~~~~~~

If a remote task fails, we can collect the function and all inputs, bring them
to the local thread, and then rerun the function in hopes of triggering the
same exception locally where normal debugging tools can be used.

With the single machine schedulers, use the ``rerun_exceptions_locally=True``
keyword:

.. code-block:: python

   >>> x.compute(rerun_exceptions_locally=True)

On the distributed scheduler use the ``recreate_error_locally`` method on
anything that contains ``Futures``:

.. code-block:: python

   >>> x.compute()
   ZeroDivisionError(...)

   >>> %pdb
   >>> future = client.compute(x)
   >>> client.recreate_error_locally(future)


Remove Failed Futures Manually
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes only parts of your computations fail, for example, if some rows of a
CSV dataset are faulty in some way.  When running with the distributed
scheduler, you can remove chunks of your data that have produced bad results if
you switch to dealing with Futures:

.. code-block:: python

   >>> import dask.dataframe as dd
   >>> df = ...           # create dataframe
   >>> df = df.persist()  # start computing on the cluster

   >>> from distributed.client import futures_of
   >>> futures = futures_of(df)  # get futures behind dataframe
   >>> futures
   [<Future: status: finished, type: pd.DataFrame, key: load-1>
    <Future: status: finished, type: pd.DataFrame, key: load-2>
    <Future: status: error, key: load-3>
    <Future: status: pending, key: load-4>
    <Future: status: error, key: load-5>]

   >>> # wait until computation is done
   >>> while any(f.status == 'pending' for f in futures):
   ...     sleep(0.1)

   >>> # pick out only the successful futures and reconstruct the dataframe
   >>> good_futures = [f for f in futures if f.status == 'finished']
   >>> df = dd.from_delayed(good_futures, meta=df._meta)

This is a bit of a hack, but often practical when first exploring messy data.
If you are using the concurrent.futures API (map, submit, gather), then this
approach is more natural.


Inspect Scheduling State
------------------------

Not all errors present themselves as exceptions.  For example, in a distributed
system workers may die unexpectedly, your computation may be unreasonably
slow due to inter-worker communication or scheduler overhead, or one of several
other issues.  Getting feedback about what's going on can help to identify
both failures and general performance bottlenecks.

For the single-machine scheduler, see :doc:`diagnostics
<understanding-performance>` documentation.  The rest of the section will
assume that you are using the `distributed scheduler
<https://distributed.dask.org/en/latest/>`_ where these issues arise more
commonly.

Web Diagnostics
~~~~~~~~~~~~~~~

First, the distributed scheduler has a number of `diagnostic web pages
<https://distributed.dask.org/en/latest/web.html>`_ showing dozens of
recorded metrics like CPU, memory, network, and disk use, a history of previous
tasks, allocation of tasks to workers, worker memory pressure, work stealing,
open file handle limits, etc.  *Many* problems can be correctly diagnosed by
inspecting these pages.  By default, these are available at
``http://scheduler:8787/``, ``http://scheduler:8788/``, and ``http://worker:8789/``,
where ``scheduler`` and ``worker`` should be replaced by the addresses of the
scheduler and each of the workers. See `web diagnostic docs
<https://distributed.dask.org/en/latest/web.html>`_ for more information.

Logs
~~~~

The scheduler, workers, and client all emits logs using `Python's standard
logging module <https://docs.python.org/3/library/logging.html>`_.  By default,
these emit to standard error.  When Dask is launched by a cluster job scheduler
(SGE/SLURM/YARN/Mesos/Marathon/Kubernetes/whatever), that system will track
these logs and will have an interface to help you access them.  If you are
launching Dask on your own, they will probably dump to the screen unless you
`redirect stderr to a file
<https://en.wikipedia.org/wiki/Redirection_(computing)#Redirecting_to_and_from_the_standard_file_handles>`_
.

You can control the logging verbosity in the ``~/.dask/config.yaml`` file.
Defaults currently look like the following:

.. code-block:: yaml

   logging:
     distributed: info
     distributed.client: warning
     bokeh: error

So, for example, you could add a line like ``distributed.worker: debug`` to get
*very* verbose output from the workers.


LocalCluster
------------

If you are using the distributed scheduler from a single machine, you may be
setting up workers manually using the command line interface or you may be
using `LocalCluster <https://distributed.dask.org/en/latest/local-cluster.html>`_
which is what runs when you just call ``Client()``:

.. code-block:: python

   >>> from dask.distributed import Client, LocalCluster
   >>> client = Client()  # This is actually the following two commands

   >>> cluster = LocalCluster()
   >>> client = Client(cluster.scheduler.address)

LocalCluster is useful because the scheduler and workers are in the same
process with you, so you can easily inspect their `state
<https://distributed.dask.org/en/latest/scheduling-state.html>`_ while
they run (they are running in a separate thread):

.. code-block:: python

   >>> cluster.scheduler.processing
   {'worker-one:59858': {'inc-123', 'add-443'},
    'worker-two:48248': {'inc-456'}}

You can also do this for the workers *if* you run them without nanny processes:

.. code-block:: python

   >>> cluster = LocalCluster(nanny=False)
   >>> client = Client(cluster)

This can be very helpful if you want to use the Dask distributed API and still
want to investigate what is going on directly within the workers.  Information
is not distilled for you like it is in the web diagnostics, but you have full
low-level access.


Inspect state with IPython
--------------------------

Sometimes you want to inspect the state of your cluster but you don't have the
luxury of operating on a single machine.  In these cases you can launch an
IPython kernel on the scheduler and on every worker, which lets you inspect
state on the scheduler and workers as computations are completing.

This does not give you the ability to run ``%pdb`` or ``%debug`` on remote
machines. The tasks are still running in separate threads, and so are not
easily accessible from an interactive IPython session.

For more details, see the `Dask distributed IPython docs
<https://distributed.dask.org/en/latest/ipython.html>`_.
