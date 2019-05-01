Sparse Arrays
=============

By swapping out in-memory NumPy arrays with in-memory sparse arrays, we can
reuse the blocked algorithms of Dask's Array to achieve parallel and distributed
sparse arrays.

The blocked algorithms in Dask Array normally parallelize around in-memory
NumPy arrays.  However, if another in-memory array library supports the NumPy
interface, then it too can take advantage of Dask Array's parallel algorithms.
In particular the `sparse <https://github.com/pydata/sparse/>`_ array library
satisfies a subset of the NumPy API and works well with (and is tested against)
Dask Array.

Example
-------

Say we have a Dask array with mostly zeros:

.. code-block:: python

   x = da.random.random((100000, 100000), chunks=(1000, 1000))
   x[x < 0.95] = 0

We can convert each of these chunks of NumPy arrays into a **sparse.COO** array:

.. code-block:: python

   import sparse
   s = x.map_blocks(sparse.COO)

Now, our array is not composed of many NumPy arrays, but rather of many
sparse arrays.  Semantically, this does not change anything.  Operations that
work will continue to work identically (assuming that the behavior of ``numpy`` 
and ``sparse`` are identical), but performance characteristics and storage costs 
may change significantly:

.. code-block:: python

   >>> s.sum(axis=0)[:100].compute()
   <COO: shape=(100,), dtype=float64, nnz=100>

   >>> _.todense()
   array([ 4803.06859272,  4913.94964525,  4877.13266438,  4860.7470773 ,
           4938.94446802,  4849.51326473,  4858.83977856,  4847.81468485,
           ... ])

Requirements
------------

Any in-memory library that copies the NumPy ndarray interface should work here.
The `sparse <https://github.com/pydata/sparse/>`_ library is a minimal
example.  In particular, an in-memory library should implement at least the
following operations:

1.  Simple slicing with slices, lists, and elements (for slicing, rechunking,
    reshaping, etc)
2.  A ``concatenate`` function matching the interface of ``np.concatenate``.
    This must be registered in ``dask.array.core.concatenate_lookup``
3.  All ufuncs must support the full ufunc interface, including ``dtype=`` and
    ``out=`` parameters (even if they don't function properly)
4.  All reductions must support the full ``axis=`` and ``keepdims=`` keywords
    and behave like NumPy in this respect
5.  The array class should follow the ``__array_priority__`` protocol and be
    prepared to respond to other arrays of lower priority
6.  If ``dot`` support is desired, a ``tensordot`` function matching the
    interface of ``np.tensordot`` should be registered in
    ``dask.array.core.tensordot_lookup``

The implementation of other operations like reshape, transpose, etc.,
should follow standard NumPy conventions regarding shape and dtype.  Not
implementing these is fine; the parallel ``dask.array`` will err at runtime if
these operations are attempted.


Mixed Arrays
------------

Dask's Array supports mixing different kinds of in-memory arrays.  This relies
on the in-memory arrays knowing how to interact with each other when necessary.
When two arrays interact, the functions from the array with the highest
``__array_priority__`` will take precedence (for example, for concatenate,
tensordot, etc.).
