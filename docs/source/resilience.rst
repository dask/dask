Resilience
==========

Software fails, Hardware fails, network connections fail, user code fails.
This document describes how ``dask.distributed`` responds in the face of these
failures and other known bugs.

User code failures
------------------

When a function raises an error that error is kept and transmitted to the
client on request.  Any attempt to gather that result *or any dependent
result* will raise that exception.

.. code-block:: python

   >>> def div(a, b):
   ...     return a / b

   >>> x = client.submit(div, 1, 0)
   >>> x.result()
   ZeroDivisionError: division by zero

   >>> y = client.submit(add, x, 10)
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
    called on other workers.  This function will be marked as "bad" after it
    kills a fixed number of workers (defaults to three).
3.  Data scattered out to the workers is not kept in the scheduler (it is
    often quite large) and so the loss of this data is irreparable.  You may
    wish to call ``Client.replicate`` on the data with a suitable replication
    factor to ensure that it remains long-lived or else back the data off of
    some resilient store, like a file system.


Hardware Failures
-----------------

It is not clear under which circumstances the local process will know that the
remote worker has closed the connection.  If the socket does not close cleanly
then the system will wait for a timeout, roughly three seconds, before marking
the worker as failed and resuming smooth operation.


Scheduler Failure
-----------------

The process containing the scheduler might die.  There is currently no
persistence mechanism to record and recover the scheduler state.

The workers and clients will all reconnect to the scheduler after it comes back
online but records of ongoing computations will be lost.


Restart and Nanny Processes
---------------------------

The client provides a mechanism to restart all of the workers in the cluster.
This is convenient if, during the course of experimentation, you find your
workers in an inconvenient state that makes them unresponsive.  The
``Client.restart`` method kills all workers, flushes all scheduler state, and
then brings all workers back online, resulting in a clean cluster.
