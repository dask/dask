DataFrame Quickstart
====================

Dask DataFrame implements a subset of the Pandas application programming
interface (API) to let you work with on datasets larger than memory using
multiple threads.

Installation Testing
--------------------
After :doc:`installing </install>` ``dask.dataframe``, to test that the module
is working run ``python``, and then run a brief set of commands to display a
dataframe of random numbers:

.. code::

   python

.. code:: Python

   import dask.dataframe as dd
   import pandas
   a = pandas.util.testing.makeTimeDataFrame()
   df = dd.from_pandas(a)
   print(df.compute().head())

If dask is installed correctly, this will print out a dataframe similar to the
following:

.. code::

   2000-01-03  0.135802 -2.119094 -0.856968  1.970263
   2000-01-04 -0.600865 -0.063003  0.949473 -1.091354
   2000-01-05  0.124110 -0.575574  0.810458  0.012946
   2000-01-06  0.348173  1.548211 -1.548226 -0.470616
   2000-01-07  1.114026 -0.829408 -0.153096 -0.041810

For more information on how to create Dask DataFrames see:
:doc:`dataframe-create`.

Basics
------
Dask.dataframe supports a small but powerful subset of the Pandas API. 

The two main data structures in Pandas, DataFrame and Series, have
Dask.dataframe counterparts. They both are :doc:`Dask Graphs </spec>` with a
special structure and you can do operations with them, but you must call
``compute()`` to trigger the computation process. 

In many cases the result of ``compute()`` depends on the type of the object
used, Dask DataFrame computes to a Pandas DataFrame, Dask Series computes to
Pandas Series and aggregation operations will return scalar types.  A
consequence is that even tough the data involved in the computations can be
larger than memory, you must ensure that your result will fit in memory.

If you are already familiar with Pandas, using Dask DataFrame is
straightforward. For example, if you want to read a csv file:

.. code::

   x,y
   1,a
   2,b
   3,c
   4,d
   5,e
   6,f

you can use ``read_csv`` from ``dask.dataframe`` to load the data into a Dask
DataFrame. Many other Pandas functions are also supported, including these:

.. code-block:: Python

   import dask.dataframe as dd

   # Read CSV file
   df = dd.read_csv('data.csv')
   df.head()

   # Element-wise operations
   a = df.x + 3*df.x**2
   a.compute()

   # Row-wise selection
   df[df.x > 3].compute()

   # Common aggregations and groupby
   df.x.max().compute()
   df.x.mean().compute()
   df.x.min().compute()
   df.x.std().compute()
   df.x.sum().compute()
   df.x.var().compute()

   b = df.groupby('x').y.max()
   b.compute()

   # Value counts
   c = df.x.value_counts()
   c.compute()

Features such as Datetime accessors and joining dataframes are also inherited
from Pandas. For a more comprehensive list of features please see the
:doc:`dask.dataframe API</dataframe-api>`.
