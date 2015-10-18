Opportunistic Caching
=====================

Storing intermediate results in memory can be both good and bad.

1.  Intermediate results might be useful in future unknown computations, so we
    should hold onto them.
2.  Intermediate results fill up memory, reducing space for the rest of our
    current computation.

Negotiating between these two concerns helps us to leverage the memory that we
have to speedup future, unanticipated computations.  Which intermediate results
should we keep?

This document explains an opportunistic caching mechanism that automatically
picks out useful tasks run in the dask schedulers.


Motivating Example
------------------

Consider computing the maximum value of a column in a CSV file

.. code-block:: python

   >>> import dask.dataframe as dd
   >>> df = dd.read_csv('myfile.csv')
   >>> df.columns
   ['first-name', 'last-name', 'amount', 'id', 'timestamp']

   >>> df.amount.max().compute()
   1000

Even though our full dataset may be too large to fit in memory, the single
``amount`` column may be small enough to hold in memory just in case it might
be useful in the future.  This is typically the case.  We often iterate on the
same subset of our data repeatedly before moving on.  For example we may now
want to find the minimum of the amount column.

.. code-block:: python

   >>> df.amount.min().compute()
   -1000

Under normal operation this would need to read through the whole CSV file over
again.


Two Simple Solutions
--------------------

If we know ahead of time that we want both the maximum and minimum we can
compute them both at once.  Dask will share intermediates intelligently,
reading through the dataset only once.

.. code-block:: python

   >>> dd.compute(df.amount.max(), df.amount.min())
   (1000, -1000)

If we know that this column fits in memory then we can also explicitly compute the column and then continue forward with straight Pandas.

.. code-block:: python

   >>> amount = df.amount.compute()
   >>> amount.max()
   1000
   >>> amount.min()
   -1000

If either of these solutions work for you, great.  No need to read on.


Automatic Opportunistic Caching
-------------------------------

A third approach is to watch *all* intermediate computations and *guess* which
ones might be valuable to keep for the future.  Dask has an *opportunistic
caching* mechanism that stores intermediate tasks with the following
characteristics

1.  Expensive to compute
2.  Cheap to store
3.  Frequently used

We can activate a fixed sized cache as a callback_.

.. _callback: diagnostics.rst

.. code-block:: python

   >>> from dask.cache import Cache
   >>> cache = Cache(2e9)  # use two gigabytes of space
   >>> cache.register()    # Turn cache on globally

Now the cache will watch every small part of the computation and judge the
value of that part based on the three characteristics listed above, expensive
to compute, cheap to store, and frequently used.  It will hold on to 2GB of the
best intermediate results it can find.  If the ``df.amount`` column fits in 2GB
then probably all of it will be stored while we keep working on it.  If we move
and work on something else, then that column will likely be evicted for other
results.

.. code-block:: python

   >>> df.amount.max().compute()  # slow the first time
   1000
   >>> df.amount.min().compute()  # fast because df.amount is in the cache
   -1000


Cache tasks, not expressions
----------------------------

This caching happens at the low-level scheduling layer, not the high-level
dask.dataframe or dask.array layer.  We don't actually cache the column
``df.amount``, we cache the hundreds of small pieces of that column that form
the dask graph.  It could be that we end up caching only a fraction of the
column.

You can see which tasks are held by the cache by inspecting the following
dictionary and list

.. code-block:: python

   >>> cache.cache.data
   <stored values>
   >>> cache.cache.heap.heap
   <scores of items in cache>
   >>> cache.cache.nbytes
   <number of bytes per item in cache>
