DataFrame
=========

The ``dask.dataframe`` modules implements a blocked parallel clone of a subset
of the Pandas DataFrame.  One dask DataFrame is comprised of several in-memory
pandas DataFrames.  An operation on a dask DataFrame triggers possibly very
many operations on the constituent pandas DataFrames in a way that is mindful
of potential parallelism and memory constraints.

Dask.dataframe copies the Pandas API
------------------------------------

Dask.dataframe exactly copies the Pandas API so operations should be familiar.
There are some slight alterations due to the parallel nature of dask.

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

As with all dask collections (e.g. Array, Bag) one triggers computation by
calling the ``.compute()`` method.

.. code-block:: python

   >>> df2.compute()
   0    2
   3    5
   Name: x, dtype: int64


Threading
---------

By default ``dask.dataframe`` uses the multi-threaded scheduler by default.
This exposes some parallelism when pandas or the underlying numpy operations
release the GIL but generally Pandas is more GIL bound than NumPy, so
multi-core speedups are not as pronounced.


What doesn't work?
------------------

Dask.dataframe only covers a small well-used portion of the Pandas API.  This
is for two reasons

1.  The Pandas API is *huge*
2.  Some operations are genuinely hard to do in parallel

Additionally, some important operations like ``set_index`` work, but are now
much slower than in Pandas because they occassionally write out to disk.


What definitely works?
----------------------

* Trivially parallelizable
    *  Elementwise operations:  ``df.x + df.y``
    *  Row-wise selections:  ``df[df.x > 0]``
    *  Loc:  ``df.loc[4.0:10.5]``
    *  Common aggregations:  ``df.x.max()``
    *  Is in:  ``df[df.x.isin([1, 2, 3])]``
    *  Datetime/string accessors:  ``df.timestamp.month``
* Cleverly parallelizable (also fast):
    *  groupby-aggregate (with common aggregations): ``df.groupby(df.x).y.max()``
    *  value_counts:  ``df.x.value_counts``
    *  Drop duplicates:  ``df.x.drop_duplicates()``
    *  Join on index:  ``dd.merge(df1, df2, left_index=True, right_index=True)``
* Requires shuffle (slow-ish, unless on index)
    *  Set index:  ``df.set_index(df.x)``
    *  groupby-apply (with anything):  ``df.groupby(df.x).apply(myfunc)``
    *  Join not on the index:  ``pd.merge(df1, df2, on='name')``
* Ingest
    *  CSVs: ``pd.read_csv``
    *  Pandas: ``pd.from_pandas``
    *  Anything supporting numpy slicing: ``pd.from_array``
    *  Dask.bag: ``b.to_dataframe(columns=[...])``
