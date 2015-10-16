Scheduler
=========

.. image:: images/scheduler-detailed.png
   :alt: Scheduler layout
   :width: 100 %

The scheduler orchestrates which workers compute which tasks in what order.
It consists of several Tornado coroutines running in a single event loop,
communicating with each other over queues.

Coroutines
----------

The scheduler consists of the following coroutines:

1.  **Scheduler:** The main coroutine which tracks the state of our computations
    and the internal state of all workers
2.  **Workers:** One coroutine per worker node.  These do nothing except launch
    worker cores and handle broken connections.
3.  **Worker Cores:**  One coroutine per worker core.  These accept tasks to run,
    communicate with the worker to run the task, and then return the result (or
    the exception) back to the scheduler when done.  Each worker core coroutine
    maintains a stream/socket with one worker.
4.  **Delete:** Send a batch of data-to-be-released up to the center.  This
    coroutine batches by one second intervals.  The delete coroutine maintains
    a stream/socket with the center.

These coroutines all run on a single event loop in a single thread.


Queues
------

There are several relevant queues.

1.  **Scheduler Queue:** This is the main source of control to the system, both
    to internal and external signals.  All of the coroutines push messages into
    the scheduler queue, for example a worker-core sends a message that a task
    has finished to the scheduler queue.  The scheduler queue is also the
    primary input to the system and how the scheduler expects to be driven,
    typically through ``update-graph`` messages that include new graphs and
    keys.
2.  **Worker Queues:** There is one worker queue per worker, not per worker
    core.  All worker core coroutines corresponding to the same worker node
    pull from the same worker queue.
3.  **Delete Queue:** Scheduler passes ``delete-key`` messages to delete with
    this queue.
4.  **Report Queue:** Scheduler reports actions on this queue like
    ``task-finished`` or ``lost-key``.  Client systems should watch this queue
    and use its messages to drive action on the client side.


Interaction
-----------

This scheduling system is intended to be run constantly, forever listening to
and responding to events.  It accepts input through the scheduler queue and
pushes output through the report queue.  Client systems generally spin up a
scheduler system and then put and get from those queues respectively.


Internal API
------------

.. autofunction:: distributed.scheduler.scheduler
.. autofunction:: distributed.scheduler.worker
.. autofunction:: distributed.scheduler.worker_core
.. autofunction:: distributed.scheduler.delete

.. autofunction:: distributed.scheduler.heal
.. autofunction:: distributed.scheduler.update_state
.. autofunction:: distributed.scheduler.validate_state
.. autofunction:: distributed.scheduler.decide_worker
