Diagnostics
===========

Profiling parallel code can be tricky. ``dask.diagnostics`` provides
functionality to aid in profiling and inspecting dask graph execution.


Scheduler Callbacks
-------------------

Schedulers based on ``dask.async.get_async`` (currently
``dask.async.get_sync``, ``dask.threaded.get``, and
``dask.multiprocessing.get``) accept four callbacks, allowing for inspection of
dask execution. The callbacks are:

1. ``start(dask, state)``

   Run at the beginning of execution, right after the state is initialized.
   Receives the dask and the scheduler state.

2. ``pretask(key, dask, state)``

   Run every time a new task is started. Receives the key of the task to be
   run, the dask, and the scheduler state.

3. ``posttask(key, dask, state, id)``

   Run every time a task is finished. Receives the key of the task to be run,
   the dask, the scheduler state, and the id of the worker that ran the task.

4. ``finish(dask, state, errored)``

   Run at the end of execution, right before the result is returned. Receives
   the dask, the scheduler state, and a boolean indicating whether the exit was
   due to an error or not.

These are internally represented as tuples of length 4, stored in the order
presented above.  Callbacks for common use cases are provided in
``dask.diagnostics``.

Profiler
--------

The ``Profiler`` class builds on the scheduler callbacks described above to
profile dask execution at the task level. This can be used as a contextmanager
around calls to ``get`` or ``compute`` to profile the computation.


.. code-block:: python

    >>> from dask.diagnostics import Profiler
    >>> import dask.array as da
    >>> a = da.random.random(size=(10000,1000), chunks=(1000,1000))
    >>> q, r = da.linalg.qr(a)
    >>> a2 = q.dot(r)

    >>> with Profiler() as prof:    # doctest: +SKIP
    ...     out = a2.compute()

During execution the profiler records the following information for each task:

1. Key
2. Task
3. Start time in seconds since the epoch
4. Finish time in seconds since the epoch
5. Worker id

These results can then be accessed by the ``results`` method. This returns a
list of ``namedtuple`` objects containing the data for each task.

.. code-block:: python

    >>> data = prof.results()
    >>> data[0]  # doctest: +SKIP
    TaskData(key=('tsqr_1_QR_st1', 9, 0),
             task=(qr, (_apply_random, 'random_sample', 1730327976, (1000, 1000), (), {})),
             start_time=1435613641.833878,
             end_time=1435613642.336109,
             worker_id=4367847424)

These can be analyzed separately, or viewed in a bokeh plot using the provided
``visualize`` method.

.. code-block:: python

    >>> prof.visualize()    # doctest: +SKIP


.. raw:: html

    <iframe src="_static/profile.html"
            marginwidth="0" marginheight="0" scrolling="no"
            width="650" height="350" style="border:none"></iframe>


Progress Bar
------------

The ``ProgressBar`` class displays a progress bar in the terminal or notebook
during computation. This can be nice feedback during long running graph
execution.

As with ``Profiler``, this can be used as a contextmanager around calls to
``compute``.

.. code-block:: python

    >>> from dask.diagnostics import ProgressBar
    >>> a = da.random.normal(size=(10000, 10000), chunks=(1000, 1000))
    >>> res = a.dot(a.T).mean(axis=0)

    >>> with ProgressBar()      # doctest: +SKIP
    ...     out = res.compute()
    [########################################] | 100% Completed | 17.1 s

Note that multiple diagnostic tools can be used concurrently by using multiple
context managers:

.. code-block:: python

    >>> with ProgressBar(), Profiler() as prof:     # doctest: +SKIP
    ...     out = res.compute()
    [########################################] | 100% Completed | 17.1 s
    >>> prof.visualize()                            # doctest: +SKIP


Custom Callbacks
----------------

Custom diagnostics can be created using the callback mechanism described above.
To add your own, it's recommended to subclass the ``Callback`` class, and
define your own methods. Below we create a class that prints the name of every
key as it's computed.

.. code-block:: python

    from dask.callbacks import Callback
    class PrintKeys(Callback):
        def _pretask(self, key, dask, state):
            """Print the key of every task as it's started"""
            print("Computing: {0}!".format(repr(key)))

This can now be used as a contextmanager during computation:

.. code-block:: python

    >>> from operator import add, mul
    >>> dsk = {'a': (add, 1, 2), 'b': (add, 3, 'a'), 'c': (mul, 'a', 'b')}
    >>> with PrintKeys():
    ...     get(dsk, 'c')
    Computing 'a'!
    Computing 'b'!
    Computing 'c'!

Alternatively, functions can be passed in as keyword arguments to ``Callback``:

.. code-block:: python

    >>> def printkeys(key, dask, state):
    ...    print("Computing: {0}!".format(repr(key)))
    >>> with Callback(pretask=printkeys):
    ...     get(dsk, 'c')
    Computing 'a'!
    Computing 'b'!
    Computing 'c'!
