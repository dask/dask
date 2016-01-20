Dask DataFrame Quickstart
=========================

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
   import numpy as np
   df = dd.from_array(np.random.randn(10,4))
   print(df.compute())

If dask is installed correctly, this will print out a dataframe with 4 columns
and 10 rows similar to:

.. code::

             0         1         2         3
   0  0.866856  1.046474  0.740312 -0.850970
   1  0.860024 -1.425839 -0.990913  0.503083
   2 -0.010060  0.055972  0.687935  0.527395
   3 -0.572874  0.905152 -0.043891 -1.143587
   4  0.534486  0.539572 -0.483121 -0.727399
   5  1.452301 -0.627865  0.600823 -1.572804
   6  0.613424 -0.221860  0.814038 -0.140681
   7 -1.006707  0.094341 -0.624968 -0.662408
   8 -2.128781  0.679268  0.920737  0.146525
   9 -0.227595  1.040548  0.498254  0.787957

For more information on how to create Dask DataFrames see:
:doc:`dataframe-create`.

Basics
------
Dask.dataframe supports a small but powerful subset of the Pandas API. 

A Dask DataFrame is a :doc:`Dask Graph </spec>` with a special structure. You
can do operations with Dask DataFrames but you must call ``compute()`` to
trigger the computation process. The result of calling ``compute`` is not a
Dask DataFrame. For operations that return a dataframe it will be a Pandas
DataFrame, and aggregation operations will return other types.  A consequence
is that even tough the data involved in the computations can be larger than
memory, you must ensure that your result will fit in memory.

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
   rf = df.x + 3*df.x**2
   rf.compute()

   # Row-wise selection
   df[df.x > 3].compute()

   # Common aggregations and groupby
   df.x.max().compute()
   df.x.mean().compute()
   df.x.min().compute()
   df.x.std().compute()
   df.x.sum().compute()
   df.x.var().compute()

   tf = df.groupby('x').y.max()
   tf.compute()

   # Value counts
   cf = df.x.value_counts()
   cf.compute()

Features such as Datetime accessors and joining dataframes are also
inherited from Pandas.
