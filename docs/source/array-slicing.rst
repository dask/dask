Slicing
=======

Dask array supports most of the NumPy slicing syntax. We'll use this array
for each example:

.. code-block:: python

   >>> x = np.arange(30).reshape(2, 3, 5)
   >>> dx = da.from_array(x, chunks=5)
   >>> x
   array([[[ 0,  1,  2,  3,  4],
           [ 5,  6,  7,  8,  9],
           [10, 11, 12, 13, 14]],
         [[15, 16, 17, 18, 19],
           [20, 21, 22, 23, 24],
           [25, 26, 27, 28, 29]]])
   >>> dx
   dask.array<array, shape=(2, 3, 5), dtype=int64, chunksize=(2, 3, 5)>

In particular it supports the following:

*  Slicing by integers and slices ``dx[0, :5]``

.. code-block:: python

   >>> dx[0, :5].compute()
   array([[ 0,  1,  2,  3,  4],
          [ 5,  6,  7,  8,  9],
          [10, 11, 12, 13, 14]])

*  Slicing by lists/arrays of integers  ``dx[:, :, [1, 2, 4]]``

.. code-block:: python

   >>> dx[:, :, [1, 2, 4]].compute()
   array([[[ 1,  2,  4],
           [ 6,  7,  9],
           [11, 12, 14]],
   
          [[16, 17, 19],
           [21, 22, 24],
           [26, 27, 29]]])

*  Slicing by lists/arrays of booleans ``dx[:, [True, False, True]]``

.. code-block:: python

   >>> dx[:, [True, False, True]].compute()
   array([[[ 0,  1,  2,  3,  4],
           [10, 11, 12, 13, 14]],
   
          [[15, 16, 17, 18, 19],
           [25, 26, 27, 28, 29]]])

*  Slicing one ``dask.array`` with another dask array of booleans ``dx[dx % 2 == 0]`` of the same shape

.. code-block:: python

   >>> dx[dx % 2 == 0].compute()
   array([ 0,  2,  4,  6,  8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28])

* Slicing one ``dask.array`` with another ``dask.array`` of booleans with the same shape on
the first ``n`` axes being masked:

.. code-block:: python

  >>> # dx[0] sums to 105, dx[1] sums to 305, so just dx[1] is True
  >>> dx[dx.sum(1).sum(1) > 200].compute()
  array([[[15, 16, 17, 18, 19],
          [20, 21, 22, 23, 24],
          [25, 26, 27, 28, 29]]])

  >>> dm = dx.sum(2) % 10 == 0
  >>> dm.compute()  # a 2-D mask, matching the shape on the first 2 axes
  array([[ True, False,  True],
         [False,  True, False]], dtype=bool)
  >>> dx[dm].compute()
  array([[ 0,  1,  2,  3,  4],
         [10, 11, 12, 13, 14],
         [20, 21, 22, 23, 24]])

It does not currently support the following:

*  Slicing with lists in multiple axes  ``dx[[1, 2, 3], [3, 2, 1]]``
*  Slicing multiple axes where any indexer is a dask array  ``dx[dx > 0, [3, 2, 1]]``

If you have a use case that requires an unsupported operation, then please `raise an issue`_.

.. _raise an issue: https://github.com/dask/dask

Efficiency
----------

The normal dask schedulers are smart enough to compute only those blocks that
are necessary to achieve the desired slicing.  So large operations may be cheap
if only a small output is desired.

In the example below we create a trillion element Dask array in million element
blocks.  We then operate on the entire array and finally slice out only a
portion of the output.

.. code-block:: python

   >>> Trillion element array of ones, in 1000 by 1000 blocks
   >>> x = da.ones((1000000, 1000000), chunks=(1000, 1000))

   >>> da.exp(x)[:1500, :1500]
   ...

This only needs to compute the top-left four blocks to achieve the result.  We
are still slightly wasteful on those blocks where we need only partial results.
We are also a bit wasteful in that we still need to manipulate the dask-graph
with a million or so tasks in it.  This can cause an interactive overhead of a
second or two.

But generally, slicing works well.
