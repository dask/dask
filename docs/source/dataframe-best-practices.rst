.. _dataframe.performance:

Best Practices
==============

It is easy to get started with Dask DataFrame, but using it *well* does require
some experience.  This page contains suggestions for best practices, and
includes solutions to common problems.


Use Pandas
----------

For data that fits into RAM, Pandas can often be faster and easier to use than
Dask DataFrame.  While "Big Data" tools can be exciting, they are almost always
worse than normal data tools while those remain appropriate.


Reduce, and then use Pandas
---------------------------

Similar to above, even if you have a large dataset there may be a point in your
computation where you've reduced things to a more manageable level.  You may
want to switch to Pandas at this point.

.. code-block:: python

   df = dd.read_parquet('my-giant-file.parquet')
   df = df[df.name == 'Alice']              # Select a subsection
   result = df.groupby('id').value.mean()   # Reduce to a smaller size
   result = result.compute()                # Convert to Pandas dataframe
   result...                                # Continue working with Pandas

Pandas Performance Tips Apply to Dask DataFrame
-----------------------------------------------

Usual Pandas performance tips like avoiding apply, using vectorized
operations, using categoricals, etc., all apply equally to Dask DataFrame.  See
`Modern Pandas <https://tomaugspurger.github.io/modern-1-intro>`_ by `Tom
Augspurger <https://github.com/TomAugspurger>`_ for a good read on this topic.

Use the Index
-------------

Dask DataFrame can be optionally sorted along a single index column.  Some
operations against this column can be very fast.  For example, if your dataset
is sorted by time, you can quickly select data for a particular day, perform
time series joins, etc.  You can check if your data is sorted by looking at the
``df.known_divisions`` attribute.  You can set an index column using the
``.set_index(column_name)`` method.  This operation is expensive though, so use
it sparingly (see below):

.. code-block:: python

   df = df.set_index('timestamp')  # set the index to make some operations fast

   df.loc['2001-01-05':'2001-01-12']  # this is very fast if you have an index
   df.merge(df2, left_index=True, right_index=True)  # this is also very fast

For more information, see documentation on :ref:`dataframe partitions <dataframe-design-partitions>`.


Avoid Full-Data Shuffling
-------------------------

Setting an index is an important but expensive operation (see above).  You
should do it infrequently and you should persist afterwards (see below).

Some operations like ``set_index`` and ``merge/join`` are harder to do in a
parallel or distributed setting than if they are in-memory on a single machine.
In particular, *shuffling operations* that rearrange data become much more
communication intensive.  For example, if your data is arranged by customer ID
but now you want to arrange it by time, all of your partitions will have to talk
to each other to exchange shards of data.  This can be an intensive process,
particularly on a cluster.

So, definitely set the index but try do so infrequently.  After you set the
index, you may want to ``persist`` your data if you are on a cluster:

.. code-block:: python

   df = df.set_index('column_name')  # do this infrequently

Additionally, ``set_index`` has a few options that can accelerate it in some
situations.  For example, if you know that your dataset is sorted or you already
know the values by which it is divided, you can provide these to accelerate the
``set_index`` operation.  For more information, see the `set_index docstring
<https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame.set_index>`_.

.. code-block:: python

   df2 = df.set_index(d.timestamp, sorted=True)


Persist Intelligently
---------------------

.. note:: This section is only relevant to users on distributed systems.

Often DataFrame workloads look like the following:

1.  Load data from files
2.  Filter data to a particular subset
3.  Shuffle data to set an intelligent index
4.  Several complex queries on top of this indexed data

It is often ideal to load, filter, and shuffle data once and keep this result in
memory.  Afterwards, each of the several complex queries can be based off of
this in-memory data rather than have to repeat the full load-filter-shuffle
process each time.  To do this, use the `client.persist
<https://distributed.dask.org/en/latest/api.html#distributed.client.Client.persist>`_
method:

.. code-block:: python

   df = dd.read_csv('s3://bucket/path/to/*.csv')
   df = df[df.balance < 0]
   df = client.persist(df)

   df = df.set_index('timestamp')
   df = client.persist(df)

   >>> df.customer_id.nunique().compute()
   18452844

   >>> df.groupby(df.city).size().compute()
   ...

Persist is important because Dask DataFrame is *lazy by default*.  It is a
way of telling the cluster that it should start executing the computations
that you have defined so far, and that it should try to keep those results in
memory.  You will get back a new DataFrame that is semantically equivalent to
your old DataFrame, but now points to running data.  Your old DataFrame still
points to lazy computations:

.. code-block:: python

   # Don't do this
   client.persist(df)  # persist doesn't change the input in-place

   # Do this instead
   df = client.persist(df)  # replace your old lazy DataFrame


Repartition to Reduce Overhead
------------------------------

Your Dask DataFrame is split up into many Pandas DataFrames.  We sometimes call
these "partitions", and often the number of partitions is decided for you. For
example, it might be the number of CSV files from which you are reading. However,
over time, as you reduce or increase the size of your pandas DataFrames by
filtering or joining, it may be wise to reconsider how many partitions you need.
There is a cost to having too many or having too few.

Partitions should fit comfortably in memory (smaller than a gigabyte) but also
not be too many.  Every operation on every partition takes the central
scheduler a few hundred microseconds to process.  If you have a few thousand
tasks this is barely noticeable, but it is nice to reduce the number if
possible.

A common situation is that you load lots of data into reasonably sized
partitions (Dask's defaults make decent choices), but then you filter down your
dataset to only a small fraction of the original.  At this point, it is wise to
regroup your many small partitions into a few larger ones.  You can do this by
using the ``repartition`` method:

.. code-block:: python

   df = dd.read_csv('s3://bucket/path/to/*.csv')
   df = df[df.name == 'Alice']  # only 1/100th of the data
   df = df.repartition(npartitions=df.npartitions // 100)

   df = df.persist()  # if on a distributed system

This helps to reduce overhead and increase the effectiveness of vectorized
Pandas operations.  You should aim for partitions that have around 100MB of
data each.

Additionally, reducing partitions is very helpful just before shuffling, which
creates ``n log(n)`` tasks relative to the number of partitions.  DataFrames
with less than 100 partitions are much easier to shuffle than DataFrames with
tens of thousands.


Joins
-----

Joining two DataFrames can be either very expensive or very cheap depending on
the situation.  It is cheap in the following cases:

1.  Joining a Dask DataFrame with a Pandas DataFrame
2.  Joining a Dask DataFrame with another Dask DataFrame of a single partition
3.  Joining Dask DataFrames along their indexes

Also, it is expensive in the following case:

1.  Joining Dask DataFrames along columns that are not their index

The expensive case requires a shuffle.  This is fine, and Dask DataFrame will
complete the job well, but it will be more expensive than a typical linear-time
operation:

.. code-block:: python

   dd.merge(a, pandas_df)  # fast
   dd.merge(a, b, left_index=True, right_index=True)  # fast
   dd.merge(a, b, left_index=True, right_on='id')  # half-fast, half-slow
   dd.merge(a, b, left_on='id', right_on='id')  # slow

For more information see :doc:`Joins <dataframe-joins>`.


Store Data in Apache Parquet Format
-----------------------------------

HDF5 is a popular choice for Pandas users with high performance needs.  We
encourage Dask DataFrame users to :doc:`store and load data <dataframe-create>`
using Parquet instead.  `Apache Parquet <https://parquet.apache.org/>`_ is a
columnar binary format that is easy to split into multiple files (easier for
parallel loading) and is generally much simpler to deal with than HDF5 (from
the library's perspective).  It is also a common format used by other big data
systems like `Apache Spark <https://spark.apache.org/>`_ and `Apache Impala
<https://impala.apache.org/>`_, and so it is useful to interchange with other
systems:

.. code-block:: python

   df.to_parquet('path/to/my-results/')
   df = dd.read_parquet('path/to/my-results/')

Dask supports reading parquet files with different engine implementations of
the Apache Parquet format for Python:

.. code-block:: python

   df1 = dd.read_parquet('path/to/my-results/', engine='fastparquet')
   df2 = dd.read_parquet('path/to/my-results/', engine='pyarrow')

These libraries can be installed using:

.. code-block:: shell

   conda install fastparquet pyarrow -c conda-forge

`fastparquet <https://github.com/dask/fastparquet/>`_ is a Python-based
implementation that uses the `Numba <https://numba.pydata.org/>`_
Python-to-LLVM compiler. PyArrow is part of the
`Apache Arrow <https://arrow.apache.org/>`_ project and uses the `C++
implementation of Apache Parquet <https://github.com/apache/parquet-cpp>`_.
