.. _array.assignment:

Assignment
==========

Dask Array supports most of the NumPy assignment indexing syntax. In
particular, it supports combinations of the following:

* Indexing by integers: ``x[1] = y``
* Indexing by slices: ``x[2::-1] = y``
* Indexing by a list of integers: ``x[[0, 2, 1]] = y``
* Indexing by a 1-d :class:`numpy` array of integers: ``x[np.arange(3)] = y``
* Indexing by a 1-d :class:`~dask.array.Array` of integers: ``x[da.arange(3)] = y``
* Indexing by a list of booleans: ``x[[False, True, True]] = y``
* Indexing by a 1-d :class:`numpy` array of booleans: ``x[np.arange(3) > 0] = y``
* Indexing by a 1-d :class:`~dask.array.Array` of booleans:
  ``x[da.arange(3) > 0] = y``

It also supports:

* Indexing by one broadcastable :class:`~dask.array.Array` of
  booleans: ``x[x > 0] = y``

However, it does not currently support the following:

* Indexing with lists in multiple axes: ``x[[1, 2, 3], [3, 1, 2]] =
  y``
* Indexing with an :class:`~dask.array.Array` of integers that has
  unknown size: ``x[da.where(np.array([1, 2, 3]) < 3)[0]] = y``


.. _array.assignment.broadcasting:

Broadcasting
------------

The normal NumPy broadcasting rules apply:

.. code-block:: python

   >>> x = da.zeros((2, 6))
   >>> x[0] = 1
   >>> x[..., 1] = 2.0
   >>> x[:, 2] = [3, 4]
   >>> x[:, 5:2:-2] = [[6, 5]]
   >>> x.compute()
   array([[1., 2., 3., 5., 1., 6.],
          [0., 2., 4., 5., 0., 6.]])
   >>> x[1] = -x[0]
   >>> x.compute()
   array([[ 1.,  2.,  3.,  5.,  1.,  6.],
          [-1., -2., -3., -5., -1., -6.]])

.. _array.assignment.masking:

Masking
-------

Elements may be masked by assigning to the NumPy masked value, or to an
array with masked values:

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
<<<<<<< HEAD
=======

.. _array.assignment.efficiency:

Efficiency
----------

During computation all blocks are processed as expected, but blocks
which do not overlap the assignment indices are unchanged by the
assignment operation and therefore do not add anything extra to the
computational cost.
>>>>>>> 9dbc5fd0b48db385ea72e4542e092914bfbaecf8
