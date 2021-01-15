Assignment
==========

Dask Array supports most of the NumPy assignment indexing syntax. In
particular, it supports the following:

*  Indexing by integers and slices: ``x[0, :5] = y``
*  Indexing by strictly monotonic lists/arrays of integers: ``x[[1, 2, 4]] = y``
*  Indexing by lists/arrays of booleans: ``x[[False, True, True, False, True]] = y``
*  Indexing one :class:`~dask.array.Array` with an :class:`~dask.array.Array` of bools: ``x[x > 0] = y``
*  Indexing one :class:`~dask.array.Array` with a zero or one-dimensional :class:`~dask.array.Array`
   of ints: ``a[b.argtopk(5)] = y``

However, it does not currently support the following:

*  Indexing with lists in multiple axes: ``x[[1, 2, 3], [3, 2, 1]] = y``
*  Indexing with non-strictly monotonic lists: ``x[[1, 3, 3, 2]] = y``
*  Indexing  one :class:`~dask.array.Array` with a multi-dimensional :class:`~dask.array.Array` of ints

.. _array.assignment.efficiency:

Broadcasting
------------

The normal numpy broadcasting rules apply:

.. code-block:: python

   >>> x = da.zeros((2, 6))
   >>> x[0] = 1
   >>> x[..., 1] = 2.0
   >>> x[:, 2] = [3, 4]
   >>> x[:, 5:2:-2] = [[6, 5]]
   >>> x.compute()
   array([[1., 2., 3., 5., 1., 6.],
          [0., 2., 4., 5., 0., 6.]])
   >>> x[0] = -x[0]
   >>> x.compute()
   array([[-1., -2., -3., -5., -1., -6.],
          [ 0.,  2.,  4.,  5.,  0.,  6.]])

.. _array.assignment.masking::

Masking
-------

Elements may be masked by assigning to the numpy masked value, or to an
array with masked values:

.. _array.slicing.efficiency:

.. code-block:: python

   >>> x = da.ones((2, 6))
   >>> x[0, [1, -2]] = np.ma.masked
   >>> x[1] = np.ma.array([0, 1, 2, 3, 4, 5], mask=[0, 1, 1, 0, 0, 0])
   >>> print(x.compute())
   [[1.0 -- 1.0 1.0 -- 1.0]
    [0.0 -- -- 3.0 4.0 5.0]]
   >>> x[:, 0] = x[:, 1]
   >>> print(x.compute())
   [[1.0 -- 1.0 1.0 -- 1.0]
    [0.0 -- -- 3.0 4.0 5.0]]
   >>> x[:, 0] = x[:, 1]
   >>> print(x.compute())
   [[-- -- 1.0 1.0 -- 1.0]
    [-- -- -- 3.0 4.0 5.0]]
