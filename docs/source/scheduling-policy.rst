Scheduling in Depth
===================

*Note: this technical document is not optimized for user readability.*

The default shared memory scheduler used by most dask collections lives in
``dask/async.py``. This scheduler dynamically schedules tasks to new workers as
they become available.  It operates in a shared memory environment without
consideration to data locality, all workers have access to all data equally.

We find that our workloads are best served by trying to minimize the memory
footprint.  This document talks about our policies to accomplish this in our
scheduling budget of one millisecond per task, irrespective of the number of
tasks.

Generally we are faced with the following situation:  A worker arrives with a
newly completed task.  We update our data structures of execution state and
have to provide a new task for that worker.  In general there are very many
available tasks, which should we give to the worker?

*Q: Which of our available tasks should we give to the newly ready worker?*

This question is simple and local and yet strongly impacts the performance of
our algorithm.  We want to choose a task that lets us free memory now and in
the future.  We need a clever and cheap way to break a tie between the set of
available tasks.

At this stage we choose the policy of "last in, first out."  That is we choose
the task that was most recently made available, quite possibly by the worker
that just returned to us.  This encourages the general theme of finishing
things before starting new things.

We implement this with a stack.  When a worker arrives with its finished task
we figure out what new tasks we can now compute with the new data and put those
on top of the stack if any exist.  We pop an item off of the top of the stack
and deliver that to the waiting worker.

And yet if the newly completed task makes ready multiple newly ready tasks in
which order should we place them on the stack?  This is yet another opportunity
for a tie breaker.  This is particularly important at *the beginning* of
execution where we typically add a large number of leaf tasks onto the stack.
Our choice in this tie breaker also strongly affects performance in many cases.

We want to encourage depth first behavior where, if our computation is composed
of something like many trees we want to fully explore one subtree before moving
on to the next.  This encourages our workers to complete blocks/subtrees of our
graph before moving on to new blocks/subtrees.

And so to encourage this "depth first behavior" we do a depth first search and
number all nodes according to their number in the depth first search (DFS) 
traversal.  We use this number to break ties when adding tasks on to the stack.  
Please note that while we spoke of optimizing the many-distinct-subtree case 
above this choice is entirely local and applies quite generally beyond this 
case.  Anything that behaves even remotely like the many-distinct-subtree case 
will benefit accordingly, and this case is quite common in normal workloads.

And yet we have glossed over another tie breaker. Performing the depth
first search, when we arrive at a node with many children we can choose the
order in which to traverse the children.  We resolve this tie breaker by
selecting those children whose result is depended upon by the most nodes.  This
dependence can be either direct for those nodes that take that data as input or
indirect for any ancestor node in the graph.  This emphasizing traversing first
those nodes that are parts of critical paths having long vertical chains that
rest on top of this node's result, and nodes whose data is depended upon by
many nodes in the future.  We choose to dive down into these subtrees first in
our depth first search so that future computations don't get stuck waiting for
them to complete.

And so we have three tie breakers

1.  Q:  Which of these available tasks should I run?

    A:  Last in, first out
2.  Q:  Which of these tasks should I put on the stack first?

    A:  Do a depth first search before the computation, use that ordering.
3.  Q:  When performing the depth first search how should I choose between
    children?

    A:  Choose those children on whom the most data depends

We have found common workflow types that require each of these decisions.  We
have not yet run into a commonly occurring graph type in data analysis that is
not well handled by these heuristics for the purposes of minimizing memory use.
