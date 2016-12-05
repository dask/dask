Scheduling Policies
===================

This document describes the policies used to select the preference of tasks and
to select the preference of workers used by Dask's distributed scheduler.  For
more information on how this these policies are enacted efficiently see
:doc:`Scheduling State<scheduling-state>`.


Choosing Workers
----------------

When a task transitions from waiting to a processing state we decide a suitable
worker for that task.  If the task has significant data dependencies or if the
workers are under heavy load then this choice of worker can strongly impact
global performance.  Currently workers for tasks are determined as follows:

1.  If the task has no major dependencies and no restrictions then we find the
    least occupied worker.
2.  Otherwise, if a task has user-provided restrictions (for example it must
    run on a machine with a GPU) then we restrict the available pool of workers
    to just that set, otherwise we consider all workers
3.  From among this pool of workers we determine the workers to whom the least
    amount of data would need to be transferred.
4.  We break ties by choosing the worker that currently has the fewest tasks,
    counting both those tasks in memory and those tasks processing currently.

This process is easy to change (and indeed this document may be outdated).  We
encourage readers to inspect the ``decide_worker`` function in scheduler.py

.. currentmodule:: distributed.scheduler

.. autosummary:: decide_worker


Choosing Tasks
--------------

We often have a choice between running many valid tasks.  There are a few
competing interests that might motivate our choice:

1.  Run tasks on a first-come-first-served basis for fairness between
    multiple clients
2.  Run tasks that are part of the critical path in an effort to
    reduce total running time and minimize straggler workloads
3.  Run tasks that allow us to release many dependencies in an effort to keep
    the memory footprint small
4.  Run tasks that are related so that large chunks of work can be completely
    eliminated before running new chunks of work

Accomplishing all of these objectives simultaneously is impossible.  Optimizing
for any of these objectives perfectly can result in costly overhead.  The
heuristics with the scheduler do a decent but imperfect job of optimizing for
all of these (they all come up in important workloads) quickly.

Last in, first out
~~~~~~~~~~~~~~~~~~

When a worker finishes a task the immediate dependencies of that task get top
priority.  This encourages a behavior of finishing ongoing work immediately
before starting new work.  This often conflicts with the
first-come-first-served objective but often results in shorter total runtimes
and significantly reduced memory footprints.

Break ties with children and depth
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Often a task has multiple dependencies and we need to break ties between them
with some other objective.  Breaking these ties has a surprisingly strong
impact on performance and memory footprint.

When a client submits a graph we perform a few linear scans over the graph to
determine something like the number of descendants of each node (not quite,
because it's a DAG rather than a tree, but this is a close proxy).  This number
can be used to break ties and helps us to prioritize nodes with longer critical
paths and nodes with many children.  The actual algorithms used are somewhat
more complex and are described in detail in `dask/order.py`_

.. _`dask/order.py`: https://github.com/dask/dask/blob/master/dask/order.py

Initial Task Placement
~~~~~~~~~~~~~~~~~~~~~~

When a new large batch of tasks come in and there are many idle workers then we
want to give each worker a set of tasks that are close together/related and
unrelated from the tasks given to other workers.  This usually avoids
inter-worker communication down the line.  The same
depth-first-with-child-weights priority given to workers described above can
usually be used to properly segment the leaves of a graph into decently well
separated sub-graphs with relatively low inter-sub-graph connectedness.


First-Come-First-Served, Coarsely
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The last-in-first-out behavior used by the workers to minimize memory footprint
can distort the task order provided by the clients.  Tasks submitted recently
may run sooner than tasks submitted long ago because they happen to be more
convenient given the current data in memory.  This behavior can be *unfair* but
improves global runtimes and system efficiency, sometimes quite significantly.

However, workers inevitably run out of tasks that were related to tasks they
were just working on and the last-in-first-out policy eventually exhausts
itself.  In these cases workers often pull tasks from the common task pool.
The tasks in this pool *are* ordered in a first-come-first-served basis and so
workers do behave in a fair scheduling manner at a *coarse* level if not a fine
grained one.

Dask's scheduling policies are short-term-efficient and long-term-fair.
