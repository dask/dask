Diagnostics
===========

Profiling parallel code can be tricky. ``dask.diagnostics`` provides
functionality to aid in profiling and inspecting dask graph execution.


Scheduler Callbacks
-------------------

Schedulers based on ``dask.async.get_async`` (currently
``dask.async.get_sync``, ``dask.threaded.get``, and
``dask.multiprocessing.get``) accept two callbacks, allowing for inspection of
dask execution. The callbacks are:

1. ``start_callback(self, key, dask, state)``

    Callback run every time a new task is started. Receives the key of the task
    to be run, the dask, and the scheduler state. At the end of computation
    this will called a final time with ``None`` as the key, as no new tasks
    were added at that tick.

2. ``end_callback(self, key, dask, state, id)``

    Callback run every time a task is finished. Receives the key of the task to
    be run, the dask, the scheduler state, and the id of the worker that ran
    the task.  At the end of computation this will called a final time with
    ``None`` for both key and worker id, as no new tasks were finished at that
    tick.


Callbacks for common use cases are provided in ``dask.diagnostics``.


Profiler
--------

The ``Profiler`` class builds on the scheduler callbacks described above to
profile dask execution at the task level. To use, create a profiler from a
scheduler ``get`` function:

.. code-block:: python

    >>> from dask.threaded import get
    >>> from dask.diagnostics import Profiler
    >>> thread_prof = Profiler(get)

For convenience, profilers for the threaded and multiprocessing scheduler
have already been created:

.. code-block:: python

    >>> from dask.diagnostics import thread_prof, process_prof

The ``get`` method of the profiler then works like a normal scheduler, but
records the following information for each task during execution:

1. Key
2. Task
3. Start time in seconds since the epoch
4. Finish time in seconds since the epoch
5. Worker id

.. code-block:: python

    >>> import dask.array as da
    >>> a = da.random.random(size=(10000,1000), chunks=(1000,1000))
    >>> q, r = da.linalg.qr(a)
    >>> a2 = q.dot(r)

    >>> out = a2.compute(get=thread_prof.get)

The results of the profiling can be accessed by the ``results`` method. This
returns a list of ``namedtuple`` objects containing the data for each task.

.. code-block:: python

    >>> prof_data = thread_prof.results()
    >>> prof_data[0]  # doctest: +SKIP
    TaskData(key=('tsqr_1_QR_st1', 9, 0),
             task=(qr, (_apply_random, 'random_sample', 1730327976, (1000, 1000), (), {})),
             start_time=1435613641.833878,
             end_time=1435613642.336109,
             worker_id=4367847424)

These can be analyzed separately, or viewed in a bokeh plot using the provided
``visualize`` method.

.. code-block:: python

    >>> thread_prof.visualize()


.. raw:: html

    <iframe src="_static/profile.html"
            marginwidth="0" marginheight="0" scrolling="no"
            width="850" height="450" style="border:none"></iframe>
