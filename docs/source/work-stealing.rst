Work Stealing
=============

Some tasks prefer to run on certain workers.  This may be because that worker
holds data dependencies of the task or because the user has expressed a loose
desire that the task run in a particular place.  Occasionally this results in a
few very busy workers and several idle workers.  In this situation the idle
workers may choose to steal work from busy workers, even if stealing work
requires the costly movement of data.

This is a performance optimization and not required for correctness.  Work
stealing provides robustness in many ad-hoc cases, but can also backfire when
we steal the wrong tasks and reduce performance.


Task criteria for stealing
--------------------------

If a task has been specifically restricted to run on particular workers (such
as is the case when special hardware is required) then we do not steal.
Barring this case, stealing usually occurs for tasks that have been assigned to
a particular worker because that worker holds the data necessary to compute the
task.

Stealing is profitable when the computation time for a task is much longer than
the communication time of the task's dependencies.  It is also good long term if
stealing causes highly-sought-after data to be replicated on more workers.

**Bad example**

We do not want to steal tasks that require moving a large dependent piece of
data across a wire from the victim to the thief if the computation is fast.  We
end up spending far more time in communication than just waiting a bit longer
and giving up on parallelism.

.. code-block:: python

   [data] = e.scatter([np.arange(1000000000)])
   x = e.submit(np.sum, data)


**Good example**

We do want to steal task tasks that only need to move dependent pieces of data,
especially when the computation time is expensive (here 100 seconds.)

.. code-block:: python

   [data] = e.scatter([100])
   x = e.submit(sleep, data)

Fortunately we often know both the number of bytes of dependencies (as
reported by calling ``sys.getsizeof`` on the workers) and the runtime cost of
previously seen functions.


When do we worksteal
--------------------

The scheduler maintains a set of idle workers and a set of saturated workers.
At various events, such as when new tasks arrive from the client, when new
workers arrive, or when we learn that workers have completed a set of tasks, we
play these two sets of idle and saturated workers against each other.


Choosing tasks to steal
-----------------------

Occupied workers maintain a stack of excess work.  The tasks at the top of this
stack are prioritized to be run by that worker before the tasks at the bottom.

Ideally we choose the worker with the *largest* stack of excess work and then
select the task at the *bottom* of this stack, hopefully starting a new
sequence of computations that are somewhat unrelated to what the busy worker is
currently working on.

All operations in the scheduler endeavor to be computed in constant time (or
linear time relative to the number of processed tasks.)  We can pull from the
bottom of the stack in constant time by implementing each worker's stack as a
``collections.deque``.  However, we currently do not maintain the data
structures necessary to efficiently find the most occupied workers.  Common
solutions, like maintaining priority queue of workers by stack length add a
``log(n)`` cost to the common case.

Instead we just call ``next(iter(saturated_workers))`` and allow Python to iterate through the set of saturated workers however it prefers.
