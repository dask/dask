Actors
======

.. note:: This is an experimental feature and is subject to change without notice
.. note:: This is an advanced feature and may not be suitable for beginning users.
   It is rarely necessary for common workloads.

Actors enable stateful computations within a Dask workflow.  They are useful
for some rare algorithms that require additional performance and are willing to
sacrifice resilience.

An actor is a pointer to a user-defined-object living on a remote worker.
Anyone with that actor can call methods on that remote object.

Example
-------

Here we create a simple ``Counter`` class, instantiate that class on one worker,
and then call methods on that class remotely.

.. code-block:: python

   class Counter:
       """ A simple class to manage an incrementing counter """
       n = 0

       def __init__(self):
           self.n = 0

       def increment(self):
           self.n += 1
           return self.n

       def add(self, x):
           self.n += x
           return self.n

   from dask.distributed import Client          # Start a Dask Client
   client = Client()

   future = client.submit(Counter, actor=True)  # Create a Counter on a worker
   counter = future.result()                    # Get back a pointer to that object

   counter
   # <Actor: Counter, key=Counter-1234abcd>

   future = counter.increment()                 # Call remote method
   future.result()                              # Get back result
   # 1

   future = counter.add(10)                     # Call remote method
   future.result()                              # Get back result
   # 11

Motivation
----------

Actors are motivated by some of the challenges of using pure task graphs.

Normal Dask computations are composed of a graph of functions.
This approach has a few limitations that are good for resilience, but can
negatively affect performance:

1.  **State**: The functions should not mutate their inputs in-place or rely on
    global state.  They  should instead operate in a pure-functional manner,
    consuming inputs and producing separate outputs.
2.  **Central Overhead**: The execution location and order is determined by the
    centralized scheduler.  Because the scheduler is involved in every decision
    it can sometimes create a central bottleneck.

Some workloads may need to update state directly, or may involve more tiny
tasks than the scheduler can handle (the scheduler can coordinate about 4000
tasks per second).

Actors side-step both of these limitations:

1.  **State**: Actors can hold on to and mutate state.  They are allowed to
    update their state in-place.
2.  **Overhead**: Operations on actors do not inform the central scheduler, and
    so do not contribute to the 4000 task/second overhead.  They also avoid an
    extra network hop and so have lower latencies.

Create an Actor
---------------

You create an actor by submitting a Class to run on a worker using normal Dask
computation functions like ``submit``, ``map``, ``compute``, or ``persist``,
and using the ``actors=`` keyword (or ``actor=`` on ``submit``).

.. code-block:: python

   future = client.submit(Counter, actors=True)

You can use all other keywords to these functions like ``workers=``,
``resources=``, and so on to control where this actor ends up.

This creates a normal Dask future on which you can call ``.result()`` to get
the Actor once it has successfully run on a worker.

.. code-block:: python

   >>> counter = future.result()
   >>> counter
   <Actor: Counter, key=...>

A ``Counter`` object has been instantiated on one of the workers, and this
``Actor`` object serves as our proxy to that remote object.  It has the same
methods and attributes.

.. code-block:: python

   >>> dir(counter)
   ['add', 'increment', 'n']

Call Remote Methods
-------------------

However accessing an attribute or calling a method will trigger a communication
to the remote worker, run the method on the remote worker in a separate thread
pool, and then communicate the result back to the calling side.  For attribute
access these operations block and return when finished, for method calls they
return an ``ActorFuture`` immediately.

.. code-block:: python

   >>> future = counter.increment()  # Immediately returns an ActorFuture
   >>> future.result()               # Block until finished and result arrives
   1

``ActorFuture`` are similar to normal Dask ``Future`` objects, but not as fully
featured.  They curently *only* support the ``result`` method and nothing else.
They don't currently work with any other Dask functions that expect futures,
like ``as_completed``, ``wait``, or ``client.gather``.  They can't be placed
into additional submit or map calls to form dependencies.  They communicate
their results immediately (rather than waiting for result to be called) and
cache the result on the future itself.

Access Attributes
-----------------

If you define an attribute at the class level then that attribute will be
accessible to the actor.

.. code-block:: python

   class Counter:
       n = 0   # Recall that we defined our class with `n` as a class variable

       ...

   >>> counter.n                     # Blocks until finished
   1

Attribute access blocks automatically.  It's as though you called ``.result()``.


Execution on the Worker
-----------------------

When you call a method on an actor, your arguments get serialized and sent
to the worker that owns the actor's object.  If you do this from a worker this
communication is direct.  If you do this from a Client then this will be direct
if the Client has direct access to the workers (create a client with
``Client(..., direct_to_workers=True)`` if direct connections are possible) or
by proxying through the scheduler if direct connections from the client to the
workers are not possible.

The appropriate method of the Actor's object is then called in a separate
thread, the result captured, and then sent back to the calling side.  Currently
workers have only a single thread for actors, but this may change in the
future.

The result is sent back immediately to the calling side, and is not stored on
the worker with the actor.  It is cached on the ``ActorFuture`` object.


Calling from coroutines and async/await
---------------------------------------

If you use actors within a coroutine or async/await function then actor methods
and attrbute access will return Tornado futures

.. code-block:: python

   async def f():
       counter = await client.submit(Counter, actor=True)

       await counter.increment()
       n = await counter.n


Coroutines and async/await on the Actor
---------------------------------------

If you define an ``async def`` function on the actor class then that method
will run on the Worker's event loop thread rather than a separate thread.

.. code-block:: python

   def Waiter(object):
       def __init__(self):
           self.event = tornado.locks.Event()

       async def set(self):
           self.event.set()

       async def wait(self):
           await self.event.wait()

   waiter = client.submit(Waiter, actor=True).result()
   waiter.wait().result()  # waits until set, without consuming a worker thread


Performance
-----------

Worker operations currently have about 1ms of latency, on top of any network
latency that may exist.  However other activity in a worker may easily increase
these latencies if enough other activities are present.


Limitations
-----------

Actors offer advanced capabilities, but with some cost:

1.  **No Resilience:** No effort is made to make actor workloads resilient to
    worker failure.  If the worker dies while holding an actor that actor is
    lost forever.
2.  **No Diagnostics:** Because the scheduler is not informed about actor
    computations no diagnostics are available about these computations.
3.  **No Load balancing:** Actors are allocated onto workers evenly, without
    serious consideration given to avoiding communication.
4.  **Experimental:** Actors are a new feature and subject to change without
    warning
