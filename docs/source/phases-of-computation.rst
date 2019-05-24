Stages of Computation
=====================

This page describes all of the parts of computation, some common causes of
slowness, and how to effectively profile.  This is intended for more advanced
users who are encountering slowdowns on larger computations.  In particular we
cover the following:

1.  Graph construction
2.  Graph optimization
3.  Graph serialization (distributed only)
4.  Graph transmission (distributed only)
5.  Scheduling
6.  Execution

Graph Construction
------------------

Operations on Dask collections (array, dataframe, bag, delayed) build task
graphs.  These are dictionaries of Python functions that include an entry
every time some function needs to run on some chunk of data.  When these
dictionaries become large (millions of tasks) the overhead of constructing them
can become considerable.  Additionally the code that builds the graphs may
itself be inefficient.

Fortunately, this computation is all happening in normal Python right on your
own computer, so you can profile it just as you would any other Python code on
your computer using tools like the cProfile module, or the ``%prun`` or
``%snakeviz`` IPython magics.

Assuming that no obvious cause comes up when profiling, a common solution to
this problem is to reduce your graph size by increasing your chunk size if
possible, or manually batching many operations into fewer functions.

Graph Optimization
------------------

Just before you submit the graph to be executed, Dask sees if it can clean up
the graph a bit.  This helps to remove unnecessary work, and sometimes swaps
out more efficient operations.  As before though, if your graph is very large
(millions of tasks) then this can take some time.

Also as before, this is all happening in Python on your local machine.  You can
profile optimization separately from computation with the ``dask.optimize``
function.

.. code-block:: python

   # x, y = dask.compute(x, y)
   x, y = dask.optimize(x, y)

It's rare for people to change optimization.  It is rarely the main cause of
slowdown.


Graph serialization
-------------------

When you are using the distributed scheduler the graph must be sent to the
scheduler process, and from there to the workers.  To send this data it must
first be converted into bytes.  This serialization process can sometimes be
expensive either if the objects that you're passing around are very complex, or
if they are very large.

The easiest way to profile this is to profile the ``persist`` call with the
distributed scheduler.  This will include both the optimization phase above, as
well as the serialization and some of the communication phase below
(serialization is often the largest component).  Fortunately ``persist``
returns immediately after, not waiting for the computation to actually finish.

Most often the cause of long serialization times is placing large objects
like NumPy arrays or Pandas dataframes into your graph repeatedly.  Dask will
usually raise a warning when it notices this.  Often the best solution is to
read your data in as a task instead of include it directly, pre-scatter large
data, or wrap them in ``dask.delayed``.  Sometimes serialization is caused by
other issues with complex objects.  These tend to be very library specific, and
so it is hard to provide general guidelines for them.

Graph Communication
-------------------

The graph must then be communicated to the scheduler.  You can watch the
``/system`` tab of the dashboard to watch network communication to and from the
scheduler.  There is no good way to profile this.


Scheduling
----------

The scheduler now receives the graph, and must populate its internal data
structures to be able to efficiently schedule these tasks to the various
workers.

It is only after these data structures are populated that the dashboard will
show any activity.  All time between pressing ``compute/persist`` and seeing
activity is taken up in the stages above.

You can profile scheduling costs with the ``/profile-server`` page of the
dashboard.  However, this is rarely useful for users because, unless you're
willing to dive into the scheduling code, it is hard to act here.  Still, the
interested user may find the profile of the inter workings of the scheduler of
interest.

If scheduling is expensive then the best you can do is to reduce your graph
size, often by increasing chunk size.


Execution
---------

Finally your workers get sent some tasks and get to run them.  Your code runs
on a thread of a worker, and does whatever it was told to do.

Dask's Dashboard is a good tool to profile and investigate performance here,
particularly the ``/status`` and ``/profile`` pages.

Accelerating this phase is often up to the author of the tasks that you are
submitting.  This might be you if you are using custom code, or the NumPy or
Pandas developers.  We encourage you to consider efficient libraries like
Cython, Numba, or any other solution that is commonly used to accelerate Python
code.
