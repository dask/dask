Joins
=====

DataFrame joins are a common and expensive computation that benefit from a
variety of optimizations in different situations.  Understanding how your data
is laid out and what you're trying to accomplish can have a large impact on
performance.  This documentation page goes through the various different
options and their performance impacts.

Large to Large Unsorted Joins
-----------------------------

In the worst case scenario you have two large tables with many partitions each
and you want to join them both along a column that may not be sorted.

This can be slow.  In this case Dask DataFrame will need to move all of your
data around so that rows with matching values in the joining columns are in the
same partition.  This large-scale movement can create communication costs, and
can require a large amount of memory.  If enough memory can not be found then
Dask will have to read and write data to disk, which may cause other
performance costs.

These problems are solvable, but will be significantly slower than many other
operations.  They are best avoided if possible.

Large to Small Joins
--------------------

Many join or merge computations combine a large table with one small one.  If
the small table is either a single partition Dask DataFrame or even just a
normal Pandas DataFrame then the computation can proceed in an embarrassingly
parallel way, where each partition of the large DataFrame is joined against the
single small table.  This incurs almost no overhead relative to Pandas joins.

If your smaller table can easily fit in memory, then you might want to ensure
that it is a single partition with the following

.. code-block:: python

    small = small.repartition(npartitions=1)
    result = big.merge(small)

Sorted Joins
------------

The Pandas merge API supports the ``left_index=`` and ``right_index=`` options
to perform joins on the index.  For Dask DataFrames these keyword options hold
special significance if the index has known divisions
(see :ref:`dataframe-design-partitions`).
In this case the DataFrame partitions are aligned along these divisions (which
is generally fast) and then an embarrassingly parallel Pandas join happens
across partition pairs.  This is generally relatively fast.

Sorted or indexed joins are a good solution to the large-large join problem.
If you plan to join against a dataset repeatedly then it may be worthwhile to
set the index ahead of time, and possibly store the data in a format that
maintains that index, like Parquet.

.. code-block:: python

    left = left.set_index('id').persist()

    left.merge(right_one, left_index=True, ...)
    left.merge(right_two, left_index=True, ...)
    ...
