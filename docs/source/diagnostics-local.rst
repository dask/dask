Diagnostics (local)
====================

Profiling parallel code can be challenging, but ``dask.diagnostics`` provides
functionality to aid in profiling and inspecting execution with the
:doc:`local task scheduler <scheduling>`.

This page describes the following few built-in options:

1.  ProgressBar
2.  Profiler
3.  ResourceProfiler
4.  CacheProfiler

Furthermore, this page then provides instructions on how to build your own custom diagnostic.

.. currentmodule:: dask.diagnostics


Progress Bar
------------

.. autosummary::
   ProgressBar

The ``ProgressBar`` class builds on the scheduler callbacks described above to
display a progress bar in the terminal or notebook during computation. This can
give a nice feedback during long running graph execution. It can be used as a
context manager around calls to ``get`` or ``compute`` to profile the
computation:

.. code-block:: python

    >>> from dask.diagnostics import ProgressBar
    >>> a = da.random.normal(size=(10000, 10000), chunks=(1000, 1000))
    >>> res = a.dot(a.T).mean(axis=0)

    >>> with ProgressBar():
    ...     out = res.compute()
    [########################################] | 100% Completed | 17.1 s

or registered globally using the ``register`` method:

.. code-block:: python

    >>> pbar = ProgressBar()
    >>> pbar.register()
    >>> out = res.compute()
    [########################################] | 100% Completed | 17.1 s

To unregister from the global callbacks, call the ``unregister`` method:

.. code-block:: python

    >>> pbar.unregister()



Profiler
--------

.. autosummary::
   Profiler

Dask provides a few tools for profiling execution. As with the ``ProgressBar``,
they each can be used as context managers or registered globally.

The ``Profiler`` class is used to profile Dask's execution at the task level.
During execution, it records the following information for each task:

1. Key
2. Task
3. Start time in seconds since the epoch
4. Finish time in seconds since the epoch
5. Worker id


ResourceProfiler
----------------

.. autosummary::
   ResourceProfiler

The ``ResourceProfiler`` class is used to profile Dask's execution at the
resource level. During execution, it records the following information
for each timestep:

1. Time in seconds since the epoch
2. Memory usage in MB
3. % CPU usage

The default timestep is 1 second, but can be set manually using the ``dt``
keyword:

.. code-block:: python

    >>> from dask.diagnostics import ResourceProfiler
    >>> rprof = ResourceProfiler(dt=0.5)


CacheProfiler
-------------

.. autosummary::
   CacheProfiler

The ``CacheProfiler`` class is used to profile Dask's execution at the scheduler
cache level. During execution, it records the following information for each
task:

1. Key
2. Task
3. Size metric
4. Cache entry time in seconds since the epoch
5. Cache exit time in seconds since the epoch

Here the size metric is the output of a function called on the result of each
task. The default metric is to count each task (``metric`` is 1 for all tasks).
Other functions may be used as a metric instead through the ``metric`` keyword.
For example, the ``nbytes`` function found in ``cachey`` can be used to measure
the number of bytes in the scheduler cache:

.. code-block:: python

    >>> from dask.diagnostics import CacheProfiler
    >>> from cachey import nbytes
    >>> cprof = CacheProfiler(metric=nbytes)


Example
-------

As an example to demonstrate using the diagnostics, we'll profile some linear
algebra done with Dask Array. We'll create a random array, take its QR
decomposition, and then reconstruct the initial array by multiplying the Q and
R components together. Note that since the profilers (and all diagnostics) are
just context managers, multiple profilers can be used in a with block:

.. code-block:: python

    >>> import dask.array as da
    >>> from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler
    >>> a = da.random.random(size=(10000, 1000), chunks=(1000, 1000))
    >>> q, r = da.linalg.qr(a)
    >>> a2 = q.dot(r)

    >>> with Profiler() as prof, ResourceProfiler(dt=0.25) as rprof,
    ...         CacheProfiler() as cprof:
    ...     out = a2.compute()

The results of each profiler are stored in their ``results`` attribute as a
list of ``namedtuple`` objects:

.. code-block:: python

    >>> prof.results[0]
    TaskData(key=('tsqr-8d16e396b237bf7a731333130d310cb9_QR_st1', 5, 0),
             task=(qr, (_apply_random, 'random_sample', 1060164455, (1000, 1000), (), {})),
             start_time=1454368444.493292,
             end_time=1454368444.902987,
             worker_id=4466937856)

    >>> rprof.results[0]
    ResourceData(time=1454368444.078748, mem=74.100736, cpu=0.0)

    >>> cprof.results[0]
    CacheData(key=('tsqr-8d16e396b237bf7a731333130d310cb9_QR_st1', 7, 0),
              task=(qr, (_apply_random, 'random_sample', 1310656009, (1000, 1000), (), {})),
              metric=1,
              cache_time=1454368444.49662,
              free_time=1454368446.769452)

These can be analyzed separately or viewed in a bokeh plot using the provided
``visualize`` method on each profiler:

.. code-block:: python

    >>> prof.visualize()


.. raw:: html

    <iframe src="_static/profile.html"
            marginwidth="0" marginheight="0" scrolling="no"
            width="650" height="300" style="border:none"></iframe>

To view multiple profilers at the same time, the ``dask.diagnostics.visualize``
function can be used. This takes a list of profilers and creates a vertical
stack of plots aligned along the x-axis:

.. code-block:: python

    >>> from dask.diagnostics import visualize
    >>> visualize([prof, rprof, cprof])


.. raw:: html

    <iframe src="_static/stacked_profile.html"
            marginwidth="0" marginheight="0" scrolling="no"
            width="650" height="700" style="border:none"></iframe>


Looking at the above figure, from top to bottom:

1. The results from the ``Profiler`` object: This shows the execution time for
   each task as a rectangle, organized along the y-axis by worker (in this case
   threads). Similar tasks are grouped by color and, by hovering over each task,
   one can see the key and task that each block represents.

2. The results from the ``ResourceProfiler`` object: This shows two lines, one
   for total CPU percentage used by all the workers, and one for total memory
   usage.

3. The results from the ``CacheProfiler`` object: This shows a line for each
   task group, plotting the sum of the current ``metric`` in the cache against
   time. In this case it's the default metric (count) and the lines represent
   the number of each object in the cache at time. Note that the grouping and
   coloring is the same as for the ``Profiler`` plot, and that the task
   represented by each line can be found by hovering over the line.

From these plots we can see that the initial tasks (calls to
``numpy.random.random`` and ``numpy.linalg.qr`` for each chunk) are run
concurrently, but only use slightly more than 100\% CPU. This is because the
call to ``numpy.linalg.qr`` currently doesn't release the Global Interpreter
Lock (GIL), so those calls can't truly be done in parallel. Next, there's a reduction
step where all the blocks are combined. This requires all the results from the
first step to be held in memory, as shown by the increased number of results in
the cache, and increase in memory usage. Immediately after this task ends, the
number of elements in the cache decreases, showing that they were only needed
for this step. Finally, there's an interleaved set of calls to ``dot`` and
``sum``. Looking at the CPU plot, it shows that these run both concurrently and in
parallel, as the CPU percentage spikes up to around 350\%.


Custom Callbacks
----------------

.. autosummary:: Callback

Schedulers based on ``dask.local.get_async`` (currently
``dask.get``, ``dask.threaded.get``, and ``dask.multiprocessing.get``)
accept five callbacks, allowing for inspection of scheduler execution.

The callbacks are:

1. ``start(dsk)``: Run at the beginning of execution, right before the 
state is initialized.  Receives the Dask graph

2. ``start_state(dsk, state)``: Run at the beginning of execution, right 
after the state is initialized.  Receives the Dask graph and scheduler state

3. ``pretask(key, dsk, state)``: Run every time a new task is started. 
Receives the key of the task to be run, the Dask graph, and the scheduler state

4. ``posttask(key, result, dsk, state, id)``: Run every time a task is finished. 
Receives the key of the task that just completed, the result, the Dask graph, 
the scheduler state, and the id of the worker that ran the task

5. ``finish(dsk, state, errored)``: Run at the end of execution, right before the 
result is returned. Receives the Dask graph, the scheduler state, and a boolean 
indicating whether or not the exit was due to an error

Custom diagnostics can be created either by instantiating the ``Callback``
class with the some of the above methods as keywords or by subclassing the
``Callback`` class.
Here we create a class that prints the name of every key as it's computed:

.. code-block:: python

    from dask.callbacks import Callback
    class PrintKeys(Callback):
        def _pretask(self, key, dask, state):
            """Print the key of every task as it's started"""
            print("Computing: {0}!".format(repr(key)))

This can now be used as a context manager during computation:

.. code-block:: python

    >>> from operator import add, mul
    >>> dsk = {'a': (add, 1, 2), 'b': (add, 3, 'a'), 'c': (mul, 'a', 'b')}

    >>> with PrintKeys():
    ...     get(dsk, 'c')
    Computing 'a'!
    Computing 'b'!
    Computing 'c'!

Alternatively, functions may be passed in as keyword arguments to ``Callback``:

.. code-block:: python

    >>> def printkeys(key, dask, state):
    ...    print("Computing: {0}!".format(repr(key)))

    >>> with Callback(pretask=printkeys):
    ...     get(dsk, 'c')
    Computing 'a'!
    Computing 'b'!
    Computing 'c'!


API
---

.. autosummary::
   CacheProfiler
   Callback
   Profiler
   ProgressBar
   ResourceProfiler
   visualize

.. autofunction:: ProgressBar
.. autofunction:: Profiler
.. autofunction:: ResourceProfiler
.. autofunction:: CacheProfiler
.. autofunction:: Callback
.. autofunction:: visualize
