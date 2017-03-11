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

Joins are also quite fast when joining a Dask dataframe to a Pandas dataframe
or when joining two Dask dataframes along their index.  No special
considerations need to be made when operating in these common cases.

So if you're doing common groupby and join operations then you can stop reading
this.  Everything will scale nicely.  Fortunately this is true most of the
time.

.. code-block:: python

   >>> df.groupby(columns).known_reduction()    # Fast and common case
   >>> dask_df.join(pandas_df, on=column)       # Fast and common case

Difficult Cases
---------------

In some cases, such as when applying an arbitrary function to groups, when
joining along non-index columns, or when explicitly setting an unsorted column
to be the index, we may need to trigger a full dataset shuffle

.. code-block:: python

   >>> df.groupby(columns).apply(arbitrary_user_function)  # Requires shuffle
   >>> lhs.join(rhs, on=column)                            # Requires shuffle
   >>> df.set_index(column)                                # Requires shuffle

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
Client as default using one of the following two options:

.. code-block:: python

    client = Client('scheduler:8786', set_as_default=True)

    or

    dask.set_options(get=client.get)

Alternatively, if you prefer to avoid defaults, you can specify a ``method=``
keyword argument to ``groupby`` or ``set_index``

.. code-block:: python

    df.set_index(column, method='disk')
    df.set_index(column, method='tasks')
