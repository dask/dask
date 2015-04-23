Distributed Scheduling Details
==============================

Worker-Scheduler and Worker-Worker interactions can be complex and deserve
prose documentation.  Because this code lives in two separate files we
consolidate documentation here.  We organize this by communication behavior.

In general communications between two nodes (worker or scheduler) have two
frames, a header and a payload

A Header is a pickled Python dict with the following keys

    address:  Return address of the sender
    function: The name of the operation to execute on the recipient node
    jobid: Some identifier, optional
    -timestamp-: not yet implemented, but we should send datetimes

The Payload can be anything, but is generally also a pickled dict.  This
depends on the operation.

Both workers and schedulers maintain dictionaries of functions that they expose
to other workers or schedulers, e.g.

    Worker().worker_functions = {'getitem': self.getitem_worker, ...}
    Worker().scheduler_funcitons = {'compute': self.compute, ...

The threads that listen for events fire these functions asynchronously using a
local threadpool.

Example
-------

Alice a scheduler sends a message to Bob, a worker

    header = {'function': 'getitem', 'address': 'ipc://alice'}
    payload = {'key': 'x', 'queue': 'name-of-queue-on-alice'}

Bob gets this information, unpickles the header and matches it against his
dictionary of functions

    >>> Bob.scheduler_functions['getitem']
    Bob.getitem_scheduler

So Bob calls this function in a separate thread

    Bob.pool.apply_async(Bob.getitem_scheduler, args=(header, payload))

That function might go ahead and trigger computation or future communication,
in which case Alice goes through a similar sequence to what Bob just did.


Getitem Scheduler -> [Workers]
------------------------------

    worker: 'getitem'
    scheduler: 'getitem-ack'

While that example is fresh in our minds, lets look at that pattern.

Schedulers often need to grab lots of data from many workers, give up control,
and then gain control again when all of the data has arrived.  We do this by
creating a queue on the scheduler for each batch of received messages.

    Alice.queues['unique-identifier'] = Queue()

Alice now sends off a bunch of messages to many workers

    header: {'function': 'getitem', 'address': 'ipc://alice'}
    payload1: {'key': 'x', 'queue': 'unique-identifier'}
    payload2: {'key': 'y', 'queue': 'unique-identifier'}

Bob and Charlie and company handle many of these, returning payloads like the
following:

    payload1: {'key': 'x', 'value': 10, 'queue': 'unique-identifier'}
    payload2: {'key': 'y', 'value': 20, 'queue': 'unique-identifier'}

Alice watches the queue, and when the right number of results have come in does
whatever she wanted to do in the first place.


Collect Worker -> [Worker]
--------------------------

Workers go through a very similar process with the ``Worker.collect`` function.
They maintain similar queues and fire of similar numbers of messages.  This
occurs whenever they are asked to ``compute`` anything.


Compute Scheduler -> Worker
---------------------------

    worker: 'compute'
    scheduler: 'finished-task'

During active scheduling the Scheduler maintains what tasks should be run next
and maintains a queue of available workers.  General flow is as follows:

Scheduler selects task `{'x': (inc, 'y')}`, selects worker `'tcp://bob'`, and
sends to the worker the following information under the function name,
`'compute'`

    key: identifier of the task in the dask graph
         e.g. 'x' in {'x': (inc, 'y')}
    task: task in the dask graph
         e.g. (inc, 'y') in {'x': (inc, 'y')}
    locations: locations of where data might live in the network of dependencies
         e.g. {'y': ['tcp://charlie', 'tcp://dennis']}

    {'key': 'x',
     'task': (inc, 'y'),
     'locations': {'y': ['tcp://charlie', 'tcp://dennis']}

The worker receives this and goes through a collect phase as described above,
gathering the necessary dependencies to its local data store.

The worker then does actual computation.

The worker reports back to the scheduler under the function `'finished-task'`

    key: identifier of the dask in the dask graph
    duration: the time in seconds that it took to complete the task
    status: Hopefully the text 'OK'
    dependencies: The keys of the data that it had to collect `list(locations)`

    {'key': 'x',
     'duration': 0.0001,
     'status': 'OK',
     'dependencies': ['y']}

Notably, the worker *does not* send back the result.  The scheduler merely
notes that the worker has the result and will send other workers there if
necessary.

When the scheduler receives the `'finished-task'` response it updates its
bookkeeping data structures showing what data lives where, and puts the worker
back on the `available_workers` queue.



