Configuration Reference
=======================

When defining nested configurations, the top level default value will be **blank**, with subsequent keys and values listed below


Scheduler
---------
The scheduler can be tuned for performance and debugging.

This is a generated table below

.. yamltotable:: scheduler
    :header: "Key", "Default", "Description"
    :widths: 20, 10, 20

This is a manually created table

.. csv-table::
   :header: "Key", "Default", "Description"
   :widths: 20, 10, 20
   :escape: \

   "allowed-failures", "3", Number of retries before a task is considered bad
   "bandwidth", "100000000", Estimated worker-worker bandwidth (100 MB/s) -- used is work-stealing calculations
   "blocked-handlers", "[]", A list of handlers to restricted from running on the scheduler -- Issue #2550
   "default-task-durations", "", How long we expect function names to run '1h'\, '1s' -- helps for long tasks
   "default-task-durations.rechunk-split", "1us", Expect ``rechunk-split`` to run in 1 us
   "default-task-durations.shuffle-split", "1us", Expect ``rechunk-split``' to run in 1 us



Worker
------
The worker can be tuned for performance and debugging.

.. csv-table::
   :header: "Key", "Default", "Description"
   :widths: 20, 10, 20
   :escape: \

   "blocked-handlers", "[]", A list of handlers to restricted from running on the scheduler -- Issue #2550
   "multiprocessing-method", "spawn", Method to create worker processes (spawn\, forkserver\, fork)
   "use-file-locking", True, Use file locks with temporary worker directories PR #1543

