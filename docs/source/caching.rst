Opportunistic Caching
=====================

EXPERIMENTAL FEATURE added to Version 0.6.2 and above - see :ref:`disclaimer<disclaimer>`.

Dask usually removes intermediate values as quickly as possible in order to
make space for more data to flow through your computation.  However, in some
cases, we may want to hold onto intermediate values, because they might be
useful for future computations in an interactive session.

We need to balance the following concerns:

1.  Intermediate results might be useful in future unknown computations
2.  Intermediate results also fill up memory, reducing space for the rest of our
    current computation.

Negotiating between these two concerns helps us to leverage the memory that we
have available to speed up future, unanticipated computations.  Which intermediate results
should we keep?

This document explains an experimental, opportunistic caching mechanism that automatically
picks out and stores useful tasks.


Motivating Example
------------------

Consider computing the maximum value of a column in a CSV file:

.. code-block:: python

   >>> import dask.dataframe as dd
   >>> df = dd.read_csv('myfile.csv')
   >>> df.columns
   ['first-name', 'last-name', 'amount', 'id', 'timestamp']

   >>> df.amount.max().compute()
   1000

Even though our full dataset may be too large to fit in memory, the single
``df.amount`` column may be small enough to hold in memory just in case it
might be useful in the future.  This is often the case during data exploration,
because we investigate the same subset of our data repeatedly before moving on.

For example, we may now want to find the minimum of the amount column:

.. code-block:: python

   >>> df.amount.min().compute()
   -1000

Under normal operations, this would need to read through the entire CSV file over
again.  This is somewhat wasteful, and stymies interactive data exploration.


Two Simple Solutions
--------------------

If we know ahead of time that we want both the maximum and minimum, we can
compute them simultaneously.  Dask will share intermediates intelligently,
reading through the dataset only once:

.. code-block:: python

   >>> dd.compute(df.amount.max(), df.amount.min())
   (1000, -1000)

If we know that this column fits in memory then we can also explicitly
compute the column and then continue forward with straight Pandas:

.. code-block:: python

   >>> amount = df.amount.compute()
   >>> amount.max()
   1000
   >>> amount.min()
   -1000

If either of these solutions work for you, great.  Otherwise, continue on for a third approach.


Automatic Opportunistic Caching
-------------------------------

Another approach is to watch *all* intermediate computations, and *guess* which
ones might be valuable to keep for the future.  Dask has an *opportunistic
caching mechanism* that stores intermediate tasks that show the following
characteristics:

1.  Expensive to compute
2.  Cheap to store
3.  Frequently used

We can activate a fixed sized cache as a callback_.

.. _callback: diagnostics.rst

.. code-block:: python

   >>> from dask.cache import Cache
   >>> cache = Cache(2e9)  # Leverage two gigabytes of memory
   >>> cache.register()    # Turn cache on globally

Now the cache will watch every small part of the computation and judge the
value of that part based on the three characteristics listed above (expensive
to compute, cheap to store, and frequently used).

Dask will hold on to 2GB of the
best intermediate results it can find, evicting older results as better results
come in.  If the ``df.amount`` column fits in 2GB then probably all of it will
be stored while we keep working on it.

If we start work on something else,
then the ``df.amount`` column will likely be evicted to make space for other
more timely results:

.. code-block:: python

   >>> df.amount.max().compute()  # slow the first time
   1000
   >>> df.amount.min().compute()  # fast because df.amount is in the cache
   -1000
   >>> df.id.nunique().compute()  # starts to push out df.amount from cache


Cache tasks, not expressions
----------------------------

This caching happens at the low-level scheduling layer, not the high-level
dask.dataframe or dask.array layer.  We don't explicitly cache the column
``df.amount``.  Instead, we cache the hundreds of small pieces of that column
that form the dask graph.  It could be that we end up caching only a fraction
of the column.

This means that the opportunistic caching mechanism described above works for *all* dask
computations, as long as those computations employ a consistent naming scheme
(as all of dask.dataframe, dask.array, and dask.delayed do.)

You can see which tasks are held by the cache by inspecting the following
attributes of the cache object:

.. code-block:: python

   >>> cache.cache.data
   <stored values>
   >>> cache.cache.heap.heap
   <scores of items in cache>
   >>> cache.cache.nbytes
   <number of bytes per item in cache>

The cache object is powered by cachey_, a tiny library for opportunistic
caching.

.. _cachey: https://github.com/blaze/cachey

.. _disclaimer:

Disclaimer
----------

This feature is still experimental, and can cause your computation to fill up RAM.

Restricting your cache to a fixed size like 2GB requires dask to accurately count
the size of each of our objects in memory.  This can be tricky, particularly
for Pythonic objects like lists and tuples, and for DataFrames that contain
object dtypes.

It is entirely possible that the caching mechanism will
*undercount* the size of objects, causing it to use up more memory than
anticipated which can lead to blowing up RAM and crashing your session.

