DataFrame
=========

The ``dask.dataframe`` module implements a blocked parallel DataFrame that
mimics a subset of the Pandas DataFrame.  One dask DataFrame is comprised of
several in-memory pandas DataFrames separated along the index.  An operation on
one dask DataFrame triggers many operations on the constituent pandas
DataFrames in a way that is mindful of potential parallelism and memory
constraints.

Dask.dataframe copies the Pandas API
------------------------------------

Dask.dataframe copies a subset of the Pandas API so operations should be
familiar to Pandas users.  There are however some alterations due to the
parallel nature of dask.

.. code-block:: python

   >>> import dask.dataframe as dd
   >>> df = dd.read_csv('2014-*.csv.gz', compression='gzip')
   >>> df.head()
      x  y
   0  1  a
   1  2  b
   2  3  c
   3  4  a
   4  5  b
   5  6  c

   >>> df2 = df[df.y == 'a'].x + 1

As with all dask collections (e.g. Array, Bag, DataFrame) one triggers
computation by calling the ``.compute()`` method.

.. code-block:: python

   >>> df2.compute()
   0    2
   3    5
   Name: x, dtype: int64


Threaded Scheduling
-------------------

By default ``dask.dataframe`` uses the multi-threaded scheduler.
This exposes some parallelism when pandas or the underlying numpy operations
release the GIL.  Generally Pandas is more GIL bound than NumPy, so multi-core
speedups are not as pronounced for ``dask.dataframe`` as they are for
``dask.array``.  This is changing and the Pandas development team is actively
working on releasing the GIL.


What doesn't work?
------------------

Dask.dataframe covers a small but well-used portion of the Pandas API.  This is
for two reasons:

1.  The Pandas API is *huge*
2.  Some operations are genuinely hard to do in parallel

Additionally, some important operations like ``set_index`` work, but are slower
than in Pandas because they may write out to disk.


What definitely works?
----------------------

* Trivially parallelizable operations (fast):
    *  Elementwise operations:  ``df.x + df.y``
    *  Row-wise selections:  ``df[df.x > 0]``
    *  Loc:  ``df.loc[4.0:10.5]``
    *  Common aggregations:  ``df.x.max()``
    *  Is in:  ``df[df.x.isin([1, 2, 3])]``
    *  Datetime/string accessors:  ``df.timestamp.month``
* Cleverly parallelizable operations (also fast):
    *  groupby-aggregate (with common aggregations): ``df.groupby(df.x).y.max()``
    *  value_counts:  ``df.x.value_counts``
    *  Drop duplicates:  ``df.x.drop_duplicates()``
    *  Join on index:  ``dd.merge(df1, df2, left_index=True, right_index=True)``
* Operations requiring a shuffle (slow-ish, unless on index)
    *  Set index:  ``df.set_index(df.x)``
    *  groupby-apply (with anything):  ``df.groupby(df.x).apply(myfunc)``
    *  Join not on the index:  ``pd.merge(df1, df2, on='name')``
* Ingest operations
    *  CSVs: ``pd.read_csv``
    *  Pandas: ``pd.from_pandas``
    *  Anything supporting numpy slicing: ``pd.from_array``
    *  Dask.bag: ``b.to_dataframe(columns=[...])``


Partitions
----------

Internally a dask dataframe is split into many partitions, each partition is
one pandas dataframe.  These dataframes are split vertically along the index.
When our index is sorted and we know the values of the divisions of our
partitions then we can be clever and efficient.

For example, if we have a time-series index then our partitoins might be divided by
month.  All of January will live in one partition while all of February will
live in the next.  In these cases operations like ``loc``, ``groupby``, and
``join/merge`` along the index can be *much* more efficient than would otherwise be possible.  You can view the number of partitions and divisions of your dataframe with the following fields

.. code-block:: python

   >>> df.npartitions
   4
   >>> df.divisions
   ['2015-01-01', '2015-02-01', '2015-03-01', '2015-04-01', '2015-04-31']

Divisions includes the minimum value of every partition's index and the maximum
value of the last partition's index.  In the example above if the user searches
for a specific datetime range then we know which partitions we need to inspect
and which we can drop.

.. code-block:: python

   >>> df.loc['2015-01-20': '2015-02-10']

Often we do not have such information about our partitions.  When reading CSV
files for example we do not know, without extra user input, how the data is
divided.  In this case ``.divisions`` will be all ``None``.

.. code-block:: python

   >>> df.divisions
   [None, None, None, None, None]


See also the API_

.. _API: dataframe-api.html
