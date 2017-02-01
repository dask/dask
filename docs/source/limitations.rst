Limitations
===========

Dask.distributed has limitations.  Understanding these can help you to reliably
create efficient distributed computations.

Performance
-----------

-  The central scheduler spends a few hundred microseconds on every task.  For
   optimal performance, task durations should be greater than 10-100ms.
-  Dask can not parallelize within individual tasks.  Individual tasks should
   be a comfortable size so as not to overwhelm any particular worker.
-  Dask assigns tasks to workers heuristically.  It *usually* makes the right
   decision, but non-optimal situations do occur.
-  The workers are just Python processes, and inherit all capabilities and
   limitations of Python.  They do not bound or limit themselves in any way.
   In production you may wish to run dask-workers within containers.

Assumptions on Functions and Data
---------------------------------

Dask assumes the following about your functions and your data:

-  All functions must be serializable either with pickle or
   `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_.  This is
   *usually* the case except in fairly exotic situations.  The
   following should work::

        from cloudpickle import dumps, loads
        loads(dumps(my_object))

-  All data must be serializable either with pickle, cloudpickle, or using
   Dask's custom serialization system.
-  Dask may run your functions multiple times,
   such as if a worker holding an intermediate result dies.  Any side effects
   should be `idempotent <https://en.wikipedia.org/wiki/Idempotence>`_.
-

Security
--------

As a distributed computing framework, Dask enables the remote execution of
arbitrary code.  You should only host dask-workers within networks that you
trust.  This is standard among distributed computing frameworks, but is worth
repeating.
