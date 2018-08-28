Work Stealing
=============

Some tasks prefer to run on certain workers.  This may be because that worker
holds data dependencies of the task or because the user has expressed a loose
desire that the task run in a particular place.  Occasionally this results in a
few very busy workers and several idle workers.  In this situation the idle
workers may choose to steal work from the busy workers, even if stealing work
requires the costly movement of data.

This is a performance optimization and not required for correctness.  Work
stealing provides robustness in many ad-hoc cases, but can also backfire when
we steal the wrong tasks and reduce performance.


Criteria for stealing
---------------------

Computation to Communication Ratio
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stealing is profitable when the computation time for a task is much longer than
the communication time of the task's dependencies.

**Bad example**

We do not want to steal tasks that require moving a large dependent piece of
data across a wire from the victim to the thief if the computation is fast.  We
end up spending far more time in communication than just waiting a bit longer
and giving up on parallelism.

.. code-block:: python

   [data] = client.scatter([np.arange(1000000000)])
   x = client.submit(np.sum, data)


**Good example**

We do want to steal task tasks that only need to move dependent pieces of data,
especially when the computation time is expensive (here 100 seconds.)

.. code-block:: python

   [data] = client.scatter([100])
   x = client.submit(sleep, data)

Fortunately we often know both the number of bytes of dependencies (as
reported by calling ``sys.getsizeof`` on the workers) and the runtime cost of
previously seen functions, which is maintained as an exponentially weighted
moving average.

Saturated Worker Burden
~~~~~~~~~~~~~~~~~~~~~~~

Stealing may be profitable even when the computation-time to communication-time
ratio is poor.  This occurs when the saturated workers have a very long backlog
of tasks and there are a large number of idle workers.  We determine if it
acceptable to steal a task if the last task to be run by the saturated workers
would finish more quickly if stolen or if it remains on the original/victim
worker.

The longer the backlog of stealable tasks, and the smaller the number of active
workers we have both increase our willingness to steal.  This is balanced
against the compute-to-communicate cost ratio.

Replicate Popular Data
~~~~~~~~~~~~~~~~~~~~~~

It is also good long term if stealing causes highly-sought-after data to be
replicated on more workers.

Steal from the Rich
~~~~~~~~~~~~~~~~~~~

We would like to steal tasks from particularly over-burdened workers rather
than workers with just a few excess tasks.

Restrictions
~~~~~~~~~~~~

If a task has been specifically restricted to run on particular workers (such
as is the case when special hardware is required) then we do not steal.

Choosing tasks to steal
-----------------------

We maintain a list of sets of stealable tasks, ordered into bins by
computation-to-communication time ratio.  The first bin contains all tasks with
a compute-to-communicate ratio greater than or equal to 8 (considered high
enough to always steal), the next bin with a ratio of 4, the next bin with a
ratio of 2, etc.., all the way down to a ratio of 1/256, which we will never
steal.

This data structure provides a somewhat-ordered view of all stealable tasks
that we can add to and remove from in constant time, rather than ``log(n)`` as
with more traditional data structures, like a heap.

During any stage when we submit tasks to workers we check if there are both
idle and saturated workers and if so we quickly run through this list of sets,
selecting tasks from the best buckets first, working our way down to the
buckets of less desirable stealable tasks.  We stop either when there are no
more stealable tasks, no more idle workers, or when the quality of the
task-to-be-stolen is not high enough given the current backlog.

This approach is fast, optimizes to steal the tasks with the best
computation-to-communication cost ratio (up to a factor of two) and tends to
steal from the workers that have the largest backlogs, just by nature that
random selection tends to draw from the largest population.


Transactional Work Stealing
---------------------------

To avoid running the same task twice, Dask implements transactional work
stealing.  When the scheduler identifies a task that should be moved it first
sends a request to the busy worker.  The worker inspects its current state of
the task and sends a response to the scheduler:

1.  If the task is not yet running, then the worker cancels the task and
    informs the scheduler that it can reroute the task elsewhere.
2.  If the task is already running or complete then the worker tells the
    scheduler that it should not replicate the task elsewhere.

This avoids redundant work, and also the duplication of side effects for more
exotic tasks.  However, concurrent or repeated execution of the same task *is
still possible* in the event of worker death or a disrupted network connection.
