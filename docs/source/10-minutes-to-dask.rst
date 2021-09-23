10 Minutes to Dask
==================

This is a short overview of what you can do with Dask. It is geared towards new users.
There is much more information contained in the rest of the documentation.

We normally import dask as follows:

.. code-block:: python

   >>> import numpy as np
   >>> import pandas as pd

   >>> import dask.array as da
   >>> import dask.dataframe as dd

Based on the type of data you are working with, you might not need all of these.

Create an Array
---------------

You can make an array from scratch by supplying an existing numpy array and optionally
including information about how the chunks should be structured.

.. code-block:: python

   >>> data = np.arange(100_000).reshape(200, 500)
   >>> a = da.from_array(data, chunks=(100, 100))
   >>> a
   dask.array<array, shape=(200, 500), dtype=int64, chunksize=(100, 100), chunktype=numpy.ndarray>

Now we have a 2D array with the shape (200, 500) composed of 10 chunks where
each chunk has the shape (100, 100). Each chunk represents a piece of the data.

Here are some key properties of an Array:

.. code-block:: python

   # inspect the chunks
   >>> a.chunks
   ((100, 100), (100, 100, 100, 100, 100))

   # access a particular block of data
   >>> a.blocks[1, 3]
   dask.array<blocks, shape=(100, 100), dtype=int64, chunksize=(100, 100), chunktype=numpy.ndarray>


Indexing
--------

Dask arrays look and feel like NumPy arrays. You can slice them in the same ways:

.. code-block:: python

   >>> a[:50, 200]
   dask.array<getitem, shape=(50,), dtype=int64, chunksize=(50,), chunktype=numpy.ndarray>


Computation
-----------

Dask arrays are lazily evaluated. The result from a computation isn't computed until
you ask for it. Instead, a Dask task graph for the computation is produced.

Anytime you have a dask object and you want to get the result. You can call compute:

.. code-block:: python

   >>> a[:50, 200].compute()
   array([  200,   700,  1200,  1700,  2200,  2700,  3200,  3700,  4200,
         4700,  5200,  5700,  6200,  6700,  7200,  7700,  8200,  8700,
         9200,  9700, 10200, 10700, 11200, 11700, 12200, 12700, 13200,
         13700, 14200, 14700, 15200, 15700, 16200, 16700, 17200, 17700,
         18200, 18700, 19200, 19700, 20200, 20700, 21200, 21700, 22200,
         22700, 23200, 23700, 24200, 24700])


Methods
-------

Array matches NumPy. So the methods should look familiar.

.. code-block:: python

   >>> a.mean()
   dask.array<mean_agg-aggregate, shape=(), dtype=float64, chunksize=(), chunktype=numpy.ndarray>

   >>> a.mean().compute()
   49999.5

   >>> np.sin(a)
   dask.array<sin, shape=(200, 500), dtype=float64, chunksize=(100, 100), chunktype=numpy.ndarray>

   >>> np.sin(a).compute()
   array([[ 0.        ,  0.84147098,  0.90929743, ...,  0.58781939,
            0.99834363,  0.49099533],
         [-0.46777181, -0.9964717 , -0.60902011, ..., -0.89796748,
         -0.85547315, -0.02646075],
         [ 0.82687954,  0.9199906 ,  0.16726654, ...,  0.99951642,
            0.51387502, -0.4442207 ],
         ...,
         [-0.99720859, -0.47596473,  0.48287891, ..., -0.76284376,
            0.13191447,  0.90539115],
         [ 0.84645538,  0.00929244, -0.83641393, ...,  0.37178568,
         -0.5802765 , -0.99883514],
         [-0.49906936,  0.45953849,  0.99564877, ...,  0.10563876,
            0.89383946,  0.86024828]])

   >>> a.T
   dask.array<transpose, shape=(500, 200), dtype=int64, chunksize=(100, 100), chunktype=numpy.ndarray>

   >>> a.T.compute()
   array([[    0,   500,  1000, ..., 98500, 99000, 99500],
         [    1,   501,  1001, ..., 98501, 99001, 99501],
         [    2,   502,  1002, ..., 98502, 99002, 99502],
         ...,
         [  497,   997,  1497, ..., 98997, 99497, 99997],
         [  498,   998,  1498, ..., 98998, 99498, 99998],
         [  499,   999,  1499, ..., 98999, 99499, 99999]])

Methods can be chained together just like in NumPy

.. code-block:: python

   >>> b = a.max(axis=1)[::-1] + 10
   >>> b
   dask.array<add, shape=(200,), dtype=int64, chunksize=(100,), chunktype=numpy.ndarray>

   >>> b[:10].compute()
   array([100009,  99509,  99009,  98509,  98009,  97509,  97009,  96509,
         96009,  95509])

Visualize the Task Graph
------------------------

So far we've been setting up computations and calling `compute`. Instead of triggering
computation, we can inspect the task graph to figure out what's going on.

.. code-block:: python

   >>> b.dask
   HighLevelGraph with 6 layers.
   <dask.highlevelgraph.HighLevelGraph object at 0x7fd33a4aa400>
   0. array-ef3148ecc2e8957c6abe629e08306680
   1. amax-b9b637c165d9bf139f7b93458cd68ec3
   2. amax-partial-aaf8028d4a4785f579b8d03ffc1ec615
   3. amax-aggregate-07b2f92aee59691afaf1680569ee4a63
   4. getitem-f9e225a2fd32b3d2f5681070d2c3d767
   5. add-f54f3a929c7efca76a23d6c42cdbbe84

   >>> b.visualize()

.. image:: images/10_minutes_b_graph.png
