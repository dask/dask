Resilience
==========

Software fails, Hardware fails, network connections fail, user code fails.
This document describes how ``distributed`` responds in the face of these
failures and other known bugs.

User code failures
------------------

When a function raises an error that error is kept and transmitted to the
executor on request.  Any attempt to gather that result *or any dependent
result* will raise that exception

.. code-block:: python

   >>> def div(a, b):
   ...     return a / b

   >>> x = executor.submit(div, 1, 0)
   >>> x.result()
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
remote worker has closed the connection.  The fool-proof solution to this
problem is to depend on regular checks from each of the workers (so called
heart-beating).  The ``distributed`` library does not do this at this time.


Local Failures
--------------

Your own process containing the executor might die.  There is currently no
persistence mechanism to record and recover the scheduler state.  The data will
remain on the cluster until cleared.

.. code-block:: python

    >>> from distributed.client import clear
    >>> clear('center-ip:8787')
