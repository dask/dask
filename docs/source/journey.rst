Journey of a Task
=================

We follow a single task through the user interface, scheduler, worker nodes,
and back.  Hopefully this helps to illustrate the inner workings of the system.

User code
---------

A user computes the addition of two variables already on the cluster, then pulls the result back to the local process.

.. code-block:: python

   e = Executor('host:port')
   x = e.submit(...)
   y = e.submit(...)

   z = e.submit(add, x, y)  # we follow z

   print(z.result())


Step 1: Executor
----------------

``z`` begins its life when the ``Executor.submit`` function enqueues the following
message on the ``scheduler_queue``::

    {'op': 'update-graph',
     'dsk': {'z': (add, x, y)},
     'keys': ['z']}

Which tells the scheduler (running in a separate thread) to merge this little
dask graph onto the full graph and to keep the key ``'z'`` around until told
otherwise and not to garbage collect it.  (Actually, the key will be something
like ``'add-8fea4942f8524dd28b45'``, but ``'z'`` is easier to type.

The executor then creates a ``Future`` object with the key ``'z'`` and returns
that object back to the user.  This happens even before the message has been
received by the scheduler.  The status of the future says ``'pending'``.


Step 2: Arrive in the Scheduler
-------------------------------

The scheduler eventually arrives at the message provided by the executor,
telling it to update its state with this little graph that shows how to compute
``z``.  It goes ahead and does so::

    dsk.update[msg['dsk']]

But it also has *a lot* of other work to do.  Notably, it has to identify that
``x`` and ``y`` are themselves futures, and connect all of those dependencies.
This is a long and detail oriented process that involves updating roughly 10
sets and dictionaries.  Interested readers should investigate
``distributed/scheduler.py::update_state()``.  While this is fairly complex and
tedious to describe rest assured that it all happens in constant time and in
about a millisecond.


Step 3: Select a Worker
-----------------------

Once the latter of ``x`` and ``y`` finishes, the scheduler notices that all of
``z``'s dependencies are in memory and that ``z`` itself may now run.  Which worker
should ``z`` select?  We consider a sequence of criteria:

1.  First, we quickly downselect to only those workers that have either ``x``
    or ``y`` in local memory.
2.  Then, we select the worker that would have to gather the least number of
    bytes in order to get both ``x`` and ``y`` locally.  E.g. if two different
    workers have ``x`` and ``y`` and if ``y`` takes up more bytes than ``x``
    then we select the machine that holds ``y`` so that we don't have to
    communicate as much.
3.  If there are multiple workers that require the minimum number of
    communication bytes then we select the worker that is the least busy

``z`` considers the workers and chooses one based on the above criteria.  In the
common case the choice is pretty obvious after step 1.  ``z`` waits on a stack
associated with the chosen worker.  The worker may still be busy though, so ``z``
may wait a while.

*Note: This policy is under flux and this part of this document is quite
possibly out of date.*

Step 4: Transmit to the Worker
------------------------------

Eventually the worker finishes a task, has a spare core, and ``z`` finds itself at
the top of the stack (note, that this may be some time after the last section
if other tasks placed themselves on top of the worker's stack in the meantime.)

We place ``z`` into a ``worker_queue`` associated with that worker and a
``worker_core`` coroutine pulls it out.  ``z``'s function and the keys associated
to its arguments are packed up into a message that looks like this::

    {'op': 'compute',
     'function': execute_task,
     'args': ((add, 'x', 'y'),),
     'needed': ['x', 'y'],
     'key': 'z'}

This message is serialized and sent across a TCP socket to the worker.


Step 5: Execute on the Worker
-----------------------------

The worker unpacks the message, and notices that it needs to have both ``x``
and ``y``.  If the worker does not already have both of these then it asks the
center where it can get them::

    Worker: Hey Center, where can I find 'y'?
    Center: Hey Worker, Try any of these locations {Alice, Bob, ...}
    Worker: Hey Alice, can you send me 'y'?
    Alice: Hey Worker, here's the value for 'y', ...

After this exchange, the worker has both the value for ``x`` and the value for
``z``.  So it launches the computation ``add(x, y)`` in a local
``ThreadPoolExecutor`` and waits on the result.

*In the mean time the worker repeats this process concurrently for other tasks.
Nothing blocks.*

Eventually the computation completes.  The Worker stores this result in its
local memory::

    data['x'] = ...

And transmits back a success, and the number of bytes of the result::

    Worker: Hey Scheduler, 'z' worked great.
            I'm holding onto it.
            It takes up 64 bytes.

The worker does not transmit back the actual value for ``z``.

Step 6:  Scheduler Aftermath
----------------------------

The scheduler receives this message and does a few things:

1.  It notes that the worker has a free core, and sends up another task if
    available
2.  If ``x`` or ``y`` are no longer needed then it sends a message out to
    delete them from local memory.
3.  It triggers an ``Event`` that the ``z`` ``Future`` knows about, waking up
    any client code waiting on ``z``'s future.  In particular, this wakes up
    the ``z.result()`` command executed by the user originally.


Step 7:  Gather
---------------

When the user calls ``z.result()`` they wait both on the completion of the
computation and for the computation to be sent back over the wire to the local
process.  Usually this isn't necessary, usually you don't want to move data
back to the local process but instead want to keep in on the cluster.

But perhaps the user really wanted to actually know this value, so they called
``z.result()``.

The scheduler checks who has ``z`` and sends them a message asking for the result.
This message doesn't wait in a queue or for other jobs to complete, it starts
instantly.  The value gets serialized, sent over TCP, and then deserialized and
returned to the user (passing through a queue or two on the way.)


Step 8:  Garbage Collection
---------------------------

The user leaves this part of their code and the local variable ``z`` goes out
of scope.  The Python garbage collector cleans it up.  This triggers a
decremented reference on the executor (we didn't mention this, but when we
created the ``Future`` we also started a reference count.)  If this is the only
instance of a Future pointing to ``z`` then we send a message down to the
scheduler (in the other thread) that it is OK to release ``z``.  The user no
longer requires it to persist.

The scheduler receives this message and, if there are no computations that
might depend on ``z`` in the immediate future, it sends a message to the ``delete``
coroutine (along the ``delete_queue``) that it should delete ``z`` from distributed
memory.  Every second or so the delete coroutine sends a batch of variables to
be deleted out to the ``Center``, a remote process, which then goes and informs
various workers that they can clear out ``z`` from their local dictionaries.

Overhead
--------

The user experiences this in about 10 milliseconds, depending on network
latency.
