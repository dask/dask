Shuffling for GroupBy and Join
==============================

.. currentmodule:: dask.dataframe

Operations like ``groupby``, ``join``, and ``set_index`` have special
performance considerations that are different from normal Pandas due to the
parallel, larger-than-memory, and distributed nature of dask.dataframe.

Easy Case
---------

To start off, common groupby operations like
``df.groupby(columns).reduction()`` for known reductions like ``mean, sum, std,
var, count, nunique`` are all quite fast and efficient, even if partitions are
not cleanly divided with known divisions.  This is the common case.

Additionally, if divisions are known then applying an arbitrary function to
groups is efficient when the grouping columns include the index.

Joins are also quite fast when joining a Dask dataframe to a Pandas dataframe
or when joining two Dask dataframes along their index.  No special
considerations need to be made when operating in these common cases.

So if you're doing common groupby and join operations then you can stop reading
this.  Everything will scale nicely.  Fortunately this is true most of the
time.

.. code-block:: python

   >>> df.groupby(columns).known_reduction()             # Fast and common case
   >>> df.groupby(columns_with_index).apply(user_fn)     # Fast and common case
   >>> dask_df.join(pandas_df, on=column)                # Fast and common case
   >>> lhs.join(rhs)                                     # Fast and common case
   >>> lhs.merge(rhs, on=columns_with_index)             # Fast and common case

Difficult Cases
---------------

In some cases, such as when applying an arbitrary function to groups (when not
grouping on index with known divisions), when joining along non-index columns,
or when explicitly setting an unsorted column to be the index, we may need to
trigger a full dataset shuffle

.. code-block:: python

   >>> df.groupby(columns_no_index).apply(user_fn)   # Requires shuffle
   >>> lhs.join(rhs, on=columns_no_index)            # Requires shuffle
   >>> df.set_index(column)                          # Requires shuffle

A shuffle is necessary when we need to re-sort our data along a new index.  For
example if we have banking records that are organized by time and we now want
to organize them by user ID then we'll need to move a lot of data around.  In
Pandas all of this data fit in memory, so this operation was easy.  Now that we
don't assume that all data fits in memory we must be a bit more careful.

Re-sorting the data can be avoided by restricting yourself to the easy cases
mentioned above.

Shuffle Methods
---------------

There are currently two strategies to shuffle data depending on whether you are
on a single machine or on a distributed cluster.

Shuffle on Disk
```````````````

When operating on larger-than-memory data on a single machine we shuffle by
dumping intermediate results to disk.  This is done using the partd_ project
for on-disk shuffles.

.. _partd: https://github.com/dask/partd

Shuffle over the Network
````````````````````````

When operating on a distributed cluster the Dask workers may not have access to
a shared hard drive.  In this case we shuffle data by breaking input partitions
into many pieces based on where they will end up and moving these pieces
throughout the network.  This prolific expansion of intermediate partitions
can stress the task scheduler.  To manage for many-partitioned datasets this we
sometimes shuffle in stages, causing undue copies but reducing the ``n**2``
effect of shuffling to something closer to ``n log(n)`` with ``log(n)`` copies.

Selecting methods
`````````````````

Dask will use on-disk shuffling by default but will switch to task-based
distributed shuffling if the default scheduler is set to use a
``dask.distributed.Client`` such as would be the case if the user sets the
Client as default:

.. code-block:: python

    client = Client('scheduler:8786', set_as_default=True)

Alternatively, if you prefer to avoid defaults, you can specify a ``method=``
keyword argument to ``groupby`` or ``set_index``

.. code-block:: python

    df.set_index(column, method='disk')
    df.set_index(column, method='tasks')


Aggregate
=========

Dask support Pandas' ``aggregate`` syntax to run multiple reductions on the
same groups. Common reductions, such as ``max``, ``sum``, ``mean`` are directly
supported:

.. code-block:: python

    >>> df.groupby(columns).aggregate(['sum', 'mean', 'max', 'min'])

Dask also supports user defined reductions. To ensure proper performance, the
reduction has to be formulated in terms of three independent steps. The
``chunk`` step is applied to each partition independently and reduces the data
within a partition. The ``aggregate`` combines the within partition results.
The optional ``finalize`` step combines the results returned from the
``aggregate`` step and should return a single final column. For Dask to
recognize the reduction, it has to be passed as an instance of
``dask.dataframe.Aggregation``.

For example, ``sum`` could be implemented as

.. code-block:: python

    custom_sum = dd.Aggregation('custom_sum', lambda s: s.sum(), lambda s0: s0.sum())
    df.groupby('g').agg(custom_sum)

The name argument should be different from existing reductions to avoid data
corruption. The arguments to each function are pre-grouped series objects,
similar to ``df.groupby('g')['value']``.

Many reductions can only be implemented with multiple temporaries. To implement
these reductions, the steps should return tuples and expect multiple arguments.
A mean function can be implemented as

.. code-block:: python

    custom_mean = dd.Aggregation(
        'custom_mean',
        lambda s: (s.count(), s.sum()),
        lambda count, sum: (count.sum(), sum.sum()),
        lambda count, sum: sum / count,
    )
    df.groupby('g').agg(custom_mean)
