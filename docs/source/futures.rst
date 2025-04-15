Futures
=======

.. meta::
    :description: Dask futures reimplements the Python futures API so you can scale your Python futures workflow across a Dask cluster.

Dask supports a real-time task framework that extends Python's
`concurrent.futures <https://docs.python.org/3/library/concurrent.futures.html>`_
interface. Dask futures allow you to scale generic Python workflows across
a Dask cluster with minimal code changes.

.. raw:: html

   <iframe width="560"
           height="315"
           src="https://www.youtube.com/embed/07EiCpdhtDE"
           style="margin: 0 auto 20px auto; display: block;"
           frameborder="0"
           allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"
           allowfullscreen></iframe>

.. currentmodule:: distributed

This interface is good for arbitrary task scheduling like
:doc:`dask.delayed <delayed>`, but is immediate rather than lazy, which
provides some more flexibility in situations where the computations may evolve
over time. These features depend on the second generation task scheduler found in
`dask.distributed <https://distributed.dask.org/en/latest>`_ (which,
despite its name, runs very well on a single machine).


Examples
--------

Visit https://examples.dask.org/futures.html to see and run examples
using futures with Dask.

Start Dask Client
-----------------

You must start a ``Client`` to use the futures interface.  This tracks state
among the various worker processes or threads:

.. code-block:: python

   from dask.distributed import Client

   client = Client()  # start local workers as processes
   # or
   client = Client(processes=False)  # start local workers as threads

If you have `Bokeh <https://docs.bokeh.org>`_ installed, then this starts up a
diagnostic dashboard at ``http://localhost:8787`` .

Submit Tasks
------------

.. autosummary::
   Client.submit
   Client.map
   Future.result

You can submit individual tasks using the ``submit`` method:

.. code-block:: python

   def inc(x):
       return x + 1

   def add(x, y):
       return x + y

   a = client.submit(inc, 10)  # calls inc(10) in background thread or process
   b = client.submit(inc, 20)  # calls inc(20) in background thread or process

The ``submit`` function returns a ``Future``, which refers to a remote result.  This result may
not yet be completed:

.. code-block:: python

   >>> a
   <Future: status: pending, key: inc-b8aaf26b99466a7a1980efa1ade6701d>

Eventually it will complete.  The result stays in the remote
thread/process/worker until you ask for it back explicitly:

.. code-block:: python

   >>> a
   <Future: status: finished, type: int, key: inc-b8aaf26b99466a7a1980efa1ade6701d>

   >>> a.result()  # blocks until task completes and data arrives
   11

You can pass futures as inputs to submit.  Dask automatically handles dependency
tracking; once all input futures have completed, they will be moved onto a
single worker (if necessary), and then the computation that depends on them
will be started.  You do not need to wait for inputs to finish before
submitting a new task; Dask will handle this automatically:

.. code-block:: python

   c = client.submit(add, a, b)  # calls add on the results of a and b

Similar to Python's ``map``, you can use ``Client.map`` to call the same
function and many inputs:

.. code-block:: python

   futures = client.map(inc, range(1000))

However, note that each task comes with about 1ms of overhead.  If you want to
map a function over a large number of inputs, then you might consider
:doc:`dask.bag <bag>` or :doc:`dask.dataframe <dataframe>` instead.

.. note: See `this page <https://docs.dask.org/en/latest/graphs.html>`_ for
   restrictions on what functions you use with Dask.

Move Data
---------

.. autosummary::
   Future.result
   Client.gather
   Client.scatter

Given any future, you can call the ``.result`` method to gather the result.
This will block until the future is done computing and then transfer the result
back to your local process if necessary:

.. code-block:: python

   >>> c.result()
   32

You can gather many results concurrently using the ``Client.gather`` method.
This can be more efficient than calling ``.result()`` on each future
sequentially:

.. code-block:: python

   >>> # results = [future.result() for future in futures]
   >>> results = client.gather(futures)  # this can be faster

If you have important local data that you want to include in your computation,
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
futures.  In this way, your local variables define what is active in Dask.  When
a future is garbage collected by your local Python session, Dask will feel free
to delete that data or stop ongoing computations that were trying to produce
it:

.. code-block:: python

   >>> del future  # deletes remote data once future is garbage collected

You can also explicitly cancel a task using the ``Future.cancel`` or
``Client.cancel`` methods:

.. code-block:: python

   >>> future.cancel()  # deletes data even if other futures point to it

If a future fails, then Dask will raise the remote exceptions and tracebacks if
you try to get the result:

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

For greater efficiency, you can also ask ``as_completed`` to gather the results
in the background:

.. code-block:: python

   for future, result in as_completed(futures, with_results=True):
       # y = future.result()  # don't need this
      ...

Or collect all futures in batches that had arrived since the last iteration:

.. code-block:: python

   for batch in as_completed(futures, with_results=True).batches():
      for future, result in batch:
          ...

Additionally, for iterative algorithms, you can add more futures into the
``as_completed`` iterator *during* iteration:

.. code-block:: python

   seq = as_completed(futures)

   for future in seq:
       y = future.result()
       if condition(y):
           new_future = client.submit(...)
           seq.add(new_future)  # add back into the loop

or use ``seq.update(futures)`` to add multiple futures at once.


Fire and Forget
---------------

.. autosummary::
   fire_and_forget

Sometimes we don't care about gathering the result of a task, and only care
about side effects that it might have like writing a result to a file:

.. code-block:: python

   >>> a = client.submit(load, filename)
   >>> b = client.submit(process, a)
   >>> c = client.submit(write, b, out_filename)

As noted above, Dask will stop work that doesn't have any active futures.  It
thinks that because no one has a pointer to this data that no one cares.  You
can tell Dask to compute a task anyway, even if there are no active futures,
using the ``fire_and_forget`` function:

.. code-block:: python

   from dask.distributed import fire_and_forget

   >>> fire_and_forget(c)

This is particularly useful when a future may go out of scope, for example, as
part of a function:

.. code-block:: python

    def process(filename):
        out_filename = 'out-' + filename
        a = client.submit(load, filename)
        b = client.submit(process, a)
        c = client.submit(write, b, out_filename)
        fire_and_forget(c)
        return  # here we lose the reference to c, but that's now ok

    for filename in filenames:
        process(filename)


Submit task and retrieve results from a different process
---------------------------------------------------------

Sometimes we care about retrieving a result but not necessarily from the same process.

.. code-block:: python

   from distributed import Variable

   var = Variable("my-result")
   fut = client.submit(...)
   var.set(fut)

Using a ``Variable`` instructs dask to remember the result of this task under
the given name so that it can be retrieved later without having to keep the
Client alive in the meantime.

.. code-block:: python

   var = Variable("my-result")
   fut = var.get()
   result = fut.result()


Submit Tasks from Tasks
-----------------------

.. autosummary::
   get_client
   rejoin
   secede

*This is an advanced feature and is rarely necessary in the common case.*

Tasks can launch other tasks by getting their own client.  This enables complex
and highly dynamic workloads:

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
tasks that launch other tasks, then it is possible to deadlock the system if you
are not careful.  You can call the ``secede`` function from within a task to
have it remove itself from the dedicated thread pool into an administrative
thread that does not take up a slot within the Dask worker:

.. code-block:: python

   from dask.distributed import get_client, secede

   def monitor(device):
      client = get_client()
      secede()  # remove this task from the thread pool
      while True:
          data = device.read_data()
          future = client.submit(process, data)
          fire_and_forget(future)

If you intend to do more work in the same thread after waiting on client work,
you may want to explicitly block until the thread is able to *rejoin* the
thread pool.  This allows some control over the number of threads that are
created and stops too many threads from being active at once, over-saturating
your hardware:

.. code-block:: python

   def f(n):  # assume that this runs as a task
      client = get_client()

      secede()  # secede while we wait for results to come back
      futures = client.map(func, range(n))
      results = client.gather(futures)

      rejoin()  # block until a slot is open in the thread pool
      result = analyze(results)
      return result


Alternatively, you can just use the normal ``compute`` function *within* a
task.  This will automatically call ``secede`` and ``rejoin`` appropriately:

.. code-block:: python

   def f(name, fn):
       df = dd.read_csv(fn)  # note that this is a dask collection
       result = df[df.name == name].count()

       # This calls secede
       # Then runs the computation on the cluster (including this worker)
       # Then blocks on rejoin, and finally delivers the answer
       result = result.compute()

       return result


Coordination Primitives
-----------------------

.. autosummary::
   Queue
   Variable
   Lock
   Event
   Semaphore

.. note: These are advanced features and are rarely necessary in the common case.

Sometimes situations arise where tasks, workers, or clients need to coordinate
with each other in ways beyond normal task scheduling with futures.  In these
cases Dask provides additional primitives to help in complex situations.

Dask provides distributed versions of coordination primitives like locks, events,
queues, and global variables that, where appropriate, match
their in-memory counterparts.  These can be used to control access to external
resources, track progress of ongoing computations, or share data in
side-channels between many workers, clients, and tasks sensibly.

.. raw:: html

   <iframe width="560"
           height="315"
           src="https://www.youtube.com/embed/Q-Y3BR1u7c0"
           style="margin: 0 auto 20px auto; display: block;"
           frameborder="0"
           allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"
           allowfullscreen></iframe>

These features are rarely necessary for common use of Dask.  We recommend that
beginning users stick with using the simpler futures found above (like
``Client.submit`` and ``Client.gather``) rather than embracing needlessly
complex techniques.


Queues
~~~~~~

.. autosummary::
   Queue

Dask queues follow the API for the standard Python Queue, but now move futures
or small messages between clients.  Queues serialize sensibly and reconnect
themselves on remote clients if necessary:

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
       fire_and_forget(future)

   while True:
       future = queue.get()
       print(future.result())


Queues can also send small pieces of information, anything that is msgpack
encodable (ints, strings, bools, lists, dicts, etc.).  This can be useful to
send back small scores or administrative messages:

.. code-block:: python

   def func(x):
       try:
          ...
       except Exception as e:
           error_queue.put(str(e))

   error_queue = Queue()

Queues are mediated by the central scheduler, and so they are not ideal for
sending large amounts of data (everything you send will be routed through a
central point).  They are well suited to move around small bits of metadata, or
futures.  These futures may point to much larger pieces of data safely:

.. code-block:: python

   >>> x = ... # my large numpy array

   # Don't do this!
   >>> q.put(x)

   # Do this instead
   >>> future = client.scatter(x)
   >>> q.put(future)

   # Or use futures for metadata
   >>> q.put({'status': 'OK', 'stage=': 1234})


Global Variables
~~~~~~~~~~~~~~~~

.. autosummary::
   Variable

Variables are like Queues in that they communicate futures and small data
between clients.  However, variables hold only a single value.  You can get or
set that value at any time:

.. code-block:: python

   >>> var = Variable('stopping-criterion')
   >>> var.set(False)

   >>> var.get()
   False

This is often used to signal stopping criteria or current parameters
between clients.

If you want to share large pieces of information, then scatter the data first:

.. code-block:: python

   >>> parameters = np.array(...)
   >>> future = client.scatter(parameters)
   >>> var.set(future)


Locks
~~~~~

.. autosummary::
   Lock

You can also hold onto cluster-wide locks using the ``Lock`` object.
Dask Locks have the same API as normal ``threading.Lock`` objects, except that
they work across the cluster:

.. code-block:: python

       from dask.distributed import Lock
       lock = Lock()

       with lock:
           # access protected resource

You can manage several locks at the same time.  Lock can either be given a
consistent name or you can pass the lock object around itself.

Using a consistent name is convenient when you want to lock some known named resource:

.. code-block:: python

   from dask.distributed import Lock

   def load(fn):
       with Lock('the-production-database'):
           # read data from filename using some sensitive source
           return ...

   futures = client.map(load, filenames)

Passing around a lock works as well and is easier when you want to create short-term
locks for a particular situation:

.. code-block:: python

   from dask.distributed import Lock
   lock = Lock()

   def load(fn, lock=None):
       with lock:
           # read data from filename using some sensitive source
           return ...

   futures = client.map(load, filenames, lock=lock)

This can be useful if you want to control concurrent access to some external
resource like a database or un-thread-safe library.


Events
~~~~~~

.. autosummary::
   Event

Dask Events mimic ``asyncio.Event`` objects, but on a cluster scope.
They hold a single flag which can be set or cleared.
Clients can wait until the event flag is set.
Different from a ``Lock``, every client can set or clear the flag and there
is no "ownership" of an event.

You can use events to e.g. synchronize multiple clients:

.. code-block:: python

   # One one client
   from dask.distributed import Event

   event = Event("my-event-1")
   event.wait()

The call to wait will block until the event is set, e.g. in another client

.. code-block:: python

   # In another client
   from dask.distributed import Event

   event = Event("my-event-1")

   # do some work

   event.set()

Events can be set, cleared and waited on multiple times.
Every waiter referencing the same event name will be notified on event set
(and not only the first one as in the case of a lock):

.. code-block:: python

   from dask.distributed import Event

   def wait_for_event(x):
      event = Event("my-event")

      event.wait()
      # at this point, all function calls
      # are in sync once the event is set

   futures = client.map(wait_for_event, range(10))

   Event("my-event").set()
   client.gather(futures)


Semaphore
~~~~~~~~~

.. autosummary::
   Semaphore

Similar to the single-valued ``Lock`` it is also possible to use a cluster-wide
semaphore to coordinate and limit access to a sensitive resource like a
database.

.. code-block:: python

   from dask.distributed import Semaphore

   sem = Semaphore(max_leases=2, name="database")

   def access_limited(val, sem):
      with sem:
         # Interact with the DB
         return

   futures = client.map(access_limited, range(10), sem=sem)
   client.gather(futures)
   sem.close()

Actors
------

Actors allow workers to manage rapidly changing state without coordinating with
the central scheduler.  This has the advantage of reducing latency
(worker-to-worker roundtrip latency is around 1ms), reducing pressure on the
centralized scheduler (workers can coordinate actors entirely among each other),
and also enabling workflows that require stateful or in-place memory
manipulation.

However, these benefits come at a cost.  The scheduler is unaware of actors and
so they don't benefit from diagnostics, load balancing, or resilience.  Once an
actor is running on a worker it is forever tied to that worker.  If that worker
becomes overburdened or dies, then there is no opportunity to recover the
workload.

*Because Actors avoid the central scheduler they can be high-performing, but not resilient.*

Example: Counter
~~~~~~~~~~~~~~~~

An actor is a class containing both state and methods that is submitted to a
worker:

.. code-block:: python

   class Counter:
       n = 0

       def __init__(self):
           self.n = 0

       def increment(self):
           self.n += 1
           return self.n

   from dask.distributed import Client

   if __name__ == '__main__':
       client = Client()

       future = client.submit(Counter, actor=True)
       counter = future.result()

   >>> counter
   <Actor: Counter, key=Counter-afa1cdfb6b4761e616fa2cfab42398c8>

Method calls on this object produce ``ActorFutures``, which are similar to
normal Futures, but interact only with the worker holding the Actor:

.. code-block:: python

   >>> future = counter.increment()
   >>> future
   <ActorFuture>

   >>> future.result()
   1

Attribute access is synchronous and blocking:

.. code-block:: python

   >>> counter.n
   1


Example: Parameter Server
~~~~~~~~~~~~~~~~~~~~~~~~~

This example will perform the following minimization with a parameter server:

.. math::

   \min_{p\in\mathbb{R}^{1000}} \sum_{i=1}^{1000} (p_i - 1)^2

This is a simple minimization that will serve as an illustrative example.

The Dask Actor will serve as the parameter server that will hold the model.
The client will calculate the gradient of the loss function above.

.. code-block:: python

   import numpy as np

   from dask.distributed import Client
   client = Client(processes=False)

   class ParameterServer:
       def __init__(self):
           self.data = dict()

       def put(self, key, value):
           self.data[key] = value

       def get(self, key):
           return self.data[key]

   def train(params, lr=0.1):
       grad = 2 * (params - 1)  # gradient of (params - 1)**2
       new_params = params - lr * grad
       return new_params

   ps_future = client.submit(ParameterServer, actor=True)
   ps = ps_future.result()

   ps.put('parameters', np.random.default_rng().random(1000))
   for k in range(20):
       params = ps.get('parameters').result()
       new_params = train(params)
       ps.put('parameters', new_params)
       print(new_params.mean())
       # k=0: "0.5988202981316124"
       # k=10: "0.9569236575164062"

This example works, and the loss function is minimized. The (simple) equation
above is minimize, so each :math:`p_i` converges to 1. If desired, this example
could be adapted to machine learning with a more complex function to minimize.

Asynchronous Operation
~~~~~~~~~~~~~~~~~~~~~~

All operations that require talking to the remote worker are awaitable:

.. code-block:: python

   async def f():
       future = client.submit(Counter, actor=True)
       counter = await future  # gather actor object locally

       counter.increment()  # send off a request asynchronously
       await counter.increment()  # or wait until it was received

       n = await counter.n  # attribute access also must be awaited

Generally, all I/O operations that trigger computations (e.g. ``to_parquet``) should be done using the ``compute=False``
parameter to avoid asynchronous blocking:

.. code-block:: python

   await client.compute(ddf.to_parquet('/tmp/some.parquet', compute=False))

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
   Client.profile
   Client.publish_dataset
   Client.rebalance
   Client.replicate
   Client.restart
   Client.run
   Client.run_on_scheduler
   Client.scatter
   Client.shutdown
   Client.scheduler_info
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
   rejoin
   wait
   print
   warn

.. autofunction:: as_completed
.. autofunction:: fire_and_forget
.. autofunction:: get_client
.. autofunction:: secede
.. autofunction:: rejoin
.. autofunction:: wait
.. autofunction:: print
.. autofunction:: warn

.. autoclass:: Client
   :members:

.. autoclass:: Future
   :members:

.. autoclass:: Queue
   :members:

.. autoclass:: Variable
   :members:

.. autoclass:: Lock
   :members:

.. autoclass:: Event
   :members:

.. autoclass:: Semaphore
   :members:
