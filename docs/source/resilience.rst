Resilience
==========

Software fails, Hardware fails, network connections fail, user code fails.
This document describes how ``distributed`` responds in the face of these
failures and other known bugs.

User code failures
------------------

When a function raises an error that error is kept and transmitted to the
executor on request.  Any attempt to gather that result *or any dependent
result* will raise that exception.

.. code-block:: python

   >>> def div(a, b):
   ...     return a / b

   >>> x = executor.submit(div, 1, 0)
   >>> x.result()
   ZeroDivisionError: division by zero

   >>> y = executor.submit(add, x, 10)
   >>> y.result()  # same error as above
   ZeroDivisionError: division by zero

This does not affect the smooth operation of the scheduler or worker in any
way.

Closed Network Connections
--------------------------

If the connection to a remote worker unexpectedly closes and the local process
appropriately raises an ``IOError`` then the scheduler will reroute all pending
computations to other workers.

If the lost worker was the only worker to hold vital results necessary for
future computations then those results will be recomputed by surviving workers.
The scheduler maintains a full history of how each result was produced and so is
able to reproduce those same computations on other workers.

This has some fail cases.

1.  If results depend on impure functions then you may get a different
    (although still entirely accurate) result
2.  If the worker failed due to a bad function, for example a function that
    causes a segmentation fault, then that bad function will repeatedly be
    called on other workers, and proceed to kill the distributed system, one
    worker at a time.
3.  Data ``scatter``ed out to the workers is not kept in the scheduler (it is
    often quite large) and so the loss of this data is irreparable.


Hardware Failures
-----------------

It is not clear under which circumstances the local process will know that the
remote worker has closed the connection.  If the socket does not close cleanly
then the system will wait for a timeout, roughly three seconds, before marking
the worker as failed and resuming smooth operation.


Scheduler Failure
-----------------

The process containing the scheduler might die.  There is currently no
persistence mechanism to record and recover the scheduler state.  The data will
remain on the cluster until cleared.


Restart and Nanny Processes
---------------------------

The executor provides a mechanism to restart all of the workers in the cluster.
This is convenient if, during the course of experimentation, you find your
workers in an inconvenient state that makes them unresponsive.  The
``Executor.restart`` method does the following process:

1.  Sends a soft shutdown signal to all of the coroutines watching workers
2.  Sends a hard kill signal to each worker's Nanny process, which oversees
    that worker.  This Nanny process terminates the worker process
    ungracefully and unregisters that worker from the Scheduler.
3.  Clears out all scheduler state and sets all Future's status to
    ``'cancelled'``
4.  Sends a restart signal to all Nanny processes, which in turn restart clean
    Worker processes and register these workers with the Scheduler.  New workers
    may not have the same port as their previous iterations.  The
    ``.nannies`` dictionary on the Executor serves as an accurate set of
    aliases if necessary.
5.  Restarts the scheduler, with clean and empty state

This effectively removes all data and clears out all computations from the
scheduler.  Any data or computations not saved to persistent storage are
lost.  This process is very robust to a number of failure modes, including
non-responsive or swamped workers but not including full hardware failures.

Currently the user may experience a few error logging messages from
Tornado upon closing their session.  These can safely be ignored.
