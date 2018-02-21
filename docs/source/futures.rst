Futures
=======

Dask supports a real-time task framework that extends Python's
`concurrent.futures <https://docs.python.org/3/library/concurrent.futures.html>`_
interface.  This interface is good for arbitrary task scheduling, like
:doc:`dask.delayed <delayed>`, but is immediate rather than lazy, which
provides some more flexibility in situations where the computations may evolve
over time.

These features depend on the second generation task scheduler found in
`dask.distributed <https://distributed.readthedocs.org/en/latest>`_ (which,
despite its name, runs very well on a single machine).

.. currentmodule:: distributed

Start Dask Client
-----------------

You must start a ``Client`` to use the futures interface.  This tracks state
among the various worker processes or threads.

.. code-block:: python

   from dask.distributed import Client

   client = Client()  # start local workers as processes
   # or
   client = Client(processes=False)  # start local workers as threads

If you have `Bokeh <https://bokeh.pydata.org>`_ installed then this starts up a
diagnostic dashboard at http://localhost:8787 .

Submit Tasks
------------

.. autosummary::
   Client.submit
   Client.map
   Future.result

Then you can submit individual tasks using the ``submit`` method.

.. code-block:: python

   def inc(x):
       return x + 1

   def add(x, y):
       return x + y

   a = client.submit(inc, 10)  # calls inc(10) in background thread or process
   b = client.submit(inc, 20)  # calls inc(20) in background thread or process

Submit returns a ``Future``, which refers to a remote result.  This result may
not yet be completed:

.. code-block:: python

   >>> a
   <Future: status: pending, key: inc-b8aaf26b99466a7a1980efa1ade6701d>

Eventually it will complete.  The result stays in the remote
thread/process/worker until you ask for it back explicitly.

.. code-block:: python

   >>> a
   <Future: status: finished, type: int, key: inc-b8aaf26b99466a7a1980efa1ade6701d>

   >>> a.result()  # blocks until task completes and data arrives
   11

You can pass futures as inputs to submit.  Dask automatically handles dependency
tracking; once all input futures have completed they will be moved onto a
single worker (if necessary), and then the computation that depends on them
will be started.  You do not need to wait for inputs to finish before
submitting a new task; Dask will handle this automatically.

.. code-block:: python

   c = client.submit(add, a, b)  # calls add on the results of a and b

Similar to Python's ``map`` you can use ``Client.map`` to call the same
function and many inputs:

.. code-block:: python

   futures = client.map(inc, range(1000))

However note that each task comes with about 1ms of overhead.  If you want to
map a function over a large number of inputs then you might consider
:doc:`dask.bag <bag>` or :doc:`dask.dataframe <dataframe>` instead.

Move Data
---------

.. autosummary::
   Future.result
   Client.gather
   Client.scatter

Given any future you can call the ``.result`` method to gather the result.
This will block until the future is done computing and then transfer the result
back to your local process if necessary.

.. code-block:: python

   >>> c.result()
   32

You can gather many results concurrently using the ``Client.gather`` method.
This can be more efficient than calling ``.result()`` on each future
sequentially.

.. code-block:: python

   >>> # results = [future.result() for future in futures]
   >>> results = client.gather(futures)  # this can be faster

If you have important local data that you want to include in your computation
you can either include it as a normal input to a submit or map call:

.. code-block:: python

   >>> df = pd.read_csv('training-data.csv')
   >>> future = client.submit(my_function, df)

Or you can ``scatter`` it explicitly.  Scattering moves your data to a worker
and returns a future pointing to that data:

.. code-block:: python

   >>> remote_df = client.scatter(df)
   >>> remote_df
   <Future: status: finished, type: DataFrame, key: bbd0ca93589c56ea14af49cba470006e>

   >>> future = client.submit(my_function, remote_df)

Both of these accomplish the same result, but using scatter can sometimes be
faster.  This is especially true if you use processes or distributed workers
(where data transfer is necessary) and you want to use ``df`` in many
computations.  Scattering the data beforehand avoids excessive data movement.

Calling scatter on a list scatters all elements individually.  Dask will spread
these elements evenly throughout workers in a round-robin fashion:

.. code-block:: python

   >>> client.scatter([1, 2, 3])
   [<Future: status: finished, type: int, key: c0a8a20f903a4915b94db8de3ea63195>,
    <Future: status: finished, type: int, key: 58e78e1b34eb49a68c65b54815d1b158>,
    <Future: status: finished, type: int, key: d3395e15f605bc35ab1bac6341a285e2>]

References, Cancellation, and Exceptions
----------------------------------------

.. autosummary::
   Future.cancel
   Future.exception
   Future.traceback
   Client.cancel

Dask will only compute and hold onto results for which there are active
futures.  In this way your local variables define what is active in Dask.  When
a future is garbage collected by your local Python session, Dask will feel free
to delete that data or stop ongoing computations that were trying to produce
it.

.. code-block:: python

   >>> del future  # deletes remote data once future is garbage collected

You can also explicitly cancel a task using the ``Future.cancel`` or
``Client.cancel`` methods.

.. code-block:: python

   >>> future.cancel()  # deletes data even if other futures point to it

If a future fails, then Dask will raise the remote exceptions and tracebacks if
you try to get the result.

.. code-block:: python

   def div(x, y):
       return x / y

   >>> a = client.submit(div, 1, 0)  # 1 / 0 raises a ZeroDivisionError
   >>> a
   <Future: status: error, key: div-3601743182196fb56339e584a2bf1039>

   >>> a.result()
         1 def div(x, y):
   ----> 2     return x / y

   ZeroDivisionError: division by zero

All futures that depend on an erred future also err with the same exception:

.. code-block:: python

   >>> b = client.submit(inc, a)
   >>> b
   <Future: status: error, key: inc-15e2e4450a0227fa38ede4d6b1a952db>

You can collect the exception or traceback explicitly with the
``Future.exception`` or ``Future.traceback`` methods.


Waiting on Futures
------------------

.. autosummary::
   as_completed
   wait

You can wait on a future or collection of futures using the ``wait`` function:

.. code-block:: python

   from dask.distributed import wait

   >>> wait(futures)

This blocks until all futures are finished or have erred.

You can also iterate over the futures as they complete using the
``as_completed`` function:

.. code-block:: python

   from dask.distributed import as_completed

   futures = client.map(score, x_values)

   best = -1
   for future in as_completed(futures):
      y = future.result()
      if y > best:
          best = y

For greater efficiency you can also ask ``as_completed`` to gather the results
in the background.

.. code-block:: python

   for future in as_completed(futures, results=True):
      ...

Or collect futures all futures in batches that had arrived since the last iteration

.. code-block:: python

   for batch in as_completed(futures, results=True).batches():
      for future in batch:
          ...

Additionally, for iterative algorithms you can add more futures into the ``as_completed`` iterator

.. code-block:: python

   seq = as_completed(futures)

   for future in seq:
       y = future.result()
       if condition(y):
           new_future = client.submit(...)
           seq.add(new_future)  # add back into the loop


Fire and Forget
---------------

.. autosummary::
   fire_and_forget

Sometimes we don't care about gathering the result of a task, and only care
about side effects that it might have, like writing a result to a file.

.. code-block:: python

   >>> a = client.submit(load, filename)
   >>> b = client.submit(process, a)
   >>> c = client.submit(write, c, out_filename)

As noted above, Dask will stop work that doesn't have any active futures.  It
thinks that because no one has a pointer to this data that no one cares.  You
can tell Dask to compute a task anyway, even if there are no active futures,
using the ``fire_and_forget`` function:

.. code-block:: python

   from dask.distributed import fire_and_forget

   >>> fire_and_forget(c)

This is particularly useful when a future may go out of scope, for example as
part of a function:

.. code-block:: python

    def process(filename):
        out_filename = 'out-' + filename
        a = client.submit(load, filename)
        b = client.submit(process, a)
        c = client.submit(write, c, out_filename)
        fire_and_forget(c)
        return  # here we lose the reference to c, but that's now ok

    for filename in filenames:
        process(filename)


Submit Tasks from Tasks
-----------------------

.. autosummary::
   get_client
   secede

Tasks can launch other tasks by getting their own client.  This enables complex
and highly dynamic workloads.

.. code-block:: python

   from dask.distributed import get_client

   def my_function(x):
       ...

       # Get locally created client
       client = get_client()

       # Do normal client operations, asking cluster for computation
       a = client.submit(...)
       b = client.submit(...)
       a, b = client.gather([a, b])

       return a + b

It also allows you to set up long running tasks that watch other resources like
sockets or physical sensors:

.. code-block:: python

   def monitor(device):
      client = get_client()
      while True:
          data = device.read_data()
          future = client.submit(process, data)
          fire_and_forget(future)

   for device in devices:
       fire_and_forget(client.submit(monitor))

However, each running task takes up a single thread, and so if you launch many
tasks that launch other tasks then it is possible to deadlock the system if you
are not careful.  You can call the ``secede`` function from within a task to
have it remove itself from the dedicated thread pool into an administrative
thread that does not take up a slot within the Dask worker:

.. code-block:: python

   from dask.distributed import get_client, secede

   def monitor(device):
      client = get_client()
      secede()
      while True:
          data = device.read_data()
          future = client.submit(process, data)
          fire_and_forget(future)


Coordinate Data Between Clients
-------------------------------

.. autosummary::
   Queue
   Variable

In the section above we saw that you could have multiple clients running at the
same time, each of which generated and manipulated futures.  These clients can
coordinate with each other using Dask ``Queue`` and ``Variable`` objects, which
can communicate futures or small bits of data between clients sensibly.

Dask queues follow the API for the standard Python Queue, but now move futures
or small messages between clients.  Queues serialize sensibly and reconnect
themselves on remote clients if necessary.

.. code-block:: python

   from dask.distributed import Queue

   def load_and_submit(filename):
       data = load(filename)
       client = get_client()
       future = client.submit(process, data)
       queue.put(future)

   client = Client()

   queue = Queue()

   for filename in filenames:
       future = client.submit(load_and_submit, filename)
       fire_and_forget(filename)

   while True:
       future = queue.get()
       print(future.result())


Queues can also send small pieces of information, anything that is msgpack
encodable (ints, strings, bools, lists, dicts, etc..).  This can be useful to
send back small scores or administrative messages:

.. code-block:: python

   def func(x):
       try:
          ...
       except Exception as e:
           error_queue.put(str(e))

   error_queue = Queue()

Variables are like Queues in that they communicate futures and small data
between clients.  However variables hold only a single value.  You can get or
set that value at any time.

.. code-block:: python

   >>> var = Variable('stopping-criterion')
   >>> var.set(False)

   >>> var.get()
   False

This is often used to signal stopping criteria or current parameters, etc.
between clients.

If you want to share large pieces of information then scatter the data first

.. code-block:: python

   >>> parameters = np.array(...)
   >>> future = client.scatter(parameters)
   >>> var.set(future)

API
---

**Client**

.. autosummary::
   Client
   Client.cancel
   Client.compute
   Client.gather
   Client.get
   Client.get_dataset
   Client.get_executor
   Client.has_what
   Client.list_datasets
   Client.map
   Client.ncores
   Client.persist
   Client.publish_dataset
   Client.rebalance
   Client.replicate
   Client.restart
   Client.run
   Client.run_on_scheduler
   Client.scatter
   Client.shutdown
   Client.scheduler_info
   Client.shutdown
   Client.start_ipython_workers
   Client.start_ipython_scheduler
   Client.submit
   Client.unpublish_dataset
   Client.upload_file
   Client.who_has

**Future**

.. autosummary::
   Future
   Future.add_done_callback
   Future.cancel
   Future.cancelled
   Future.done
   Future.exception
   Future.result
   Future.traceback

**Functions**

.. autosummary::
   as_completed
   fire_and_forget
   get_client
   secede
   wait

.. autofunction:: as_completed
.. autofunction:: fire_and_forget
.. autofunction:: get_client
.. autofunction:: secede
.. autofunction:: wait

.. autoclass:: Client
   :members:

.. autoclass:: Future
   :members:


.. autoclass:: Queue
   :members:

.. autoclass:: Variable
   :members:
