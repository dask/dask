Build Custom Dask.Array Function
================================

As discussed in the :doc:`array design document <array-design>` to create a
dask ``Array`` object we need the following:

1.  A dask graph
2.  A name specifying a set of keys within that graph
3.  A ``chunks`` tuple giving chunk shape information
4.  A NumPy dtype

Often ``dask.array`` functions take other ``Array`` objects as inputs along
with parameters, add tasks to a new dask dictionary, create a new ``chunks``
tuple, and then construct and return a new ``Array`` object.  The hard parts
are invariably creating the right tasks and creating a new ``chunks`` tuple.
Careful review of the :doc:`array design document <array-design>` is suggested.


Example `eye`
-------------

Consider this simple example with the ``eye`` function.

.. code-block:: python

   from dask.base import tokenize

   def eye(n, blocksize):
       chunks = ((blocksize,) * n // blocksize,
                 (blocksize,) * n // blocksize)

       name = 'eye-' + tokenize(n, blocksize)  # unique identifier

       dsk = {(name, i, j): (np.eye, blocksize)
                            if i == j else
                            (np.zeros, (blocksize, blocksize))
                for i in range(n // blocksize)
                for j in range(n // blocksize)}

       dtype = np.eye(0).dtype  # take dtype default from numpy

       return Array(dsk, name, chunks, dtype)

This example is particularly simple because it doesn't take any ``Array``
objects as input.


Example `diag`
--------------

Consider the function ``diag`` that takes a 1d vector and produces a 2d matrix
with the values of the vector along the diagonal.  Consider the case where the
input is a 1d array with chunk sizes ``(2, 3, 4)`` in the first dimension like
this::

    [x_0, x_1], [x_2, x_3, x_4], [x_5, x_6, x_7, x_8]

We need to create a 2d matrix with chunks equal to ``((2, 3, 4), (2, 3, 4))``
where the ith block along the diagonal of the output is the result of calling
``np.diag`` on the ``ith`` block of the input and all other blocks are zero.

.. code-block:: python

   from dask.base import tokenize

   def diag(v):
       """Construct a diagonal array, with ``v`` on the diagonal."""
       assert v.ndim == 1
       chunks = (v.chunks[0], v.chunks[0])  # repeat chunks twice

       name = 'diag-' + tokenize(v)  # unique identifier

       dsk = {(name, i, j): (np.diag, (v.name, i))
                            if i == j else
                            (np.zeros, (v.chunks[0][i], v.chunks[0][j]))
               for i in range(len(v.chunks[0]))
               for j in range(len(v.chunks[0]))}

       dsk.update(v.dask)  # include dask graph of the input

       dtype = v.dtype     # output has the same dtype as the input

       return Array(dsk, name, chunks, dtype)

   >>> x = da.arange(9, chunks=((2, 3, 4),))
   >>> x
   dask.array<arange-1, shape=(9,), chunks=((2, 3, 4)), dtype=int64>

   >>> M = diag(x)
   >>> M
   dask.array<diag-2, shape=(9, 9), chunks=((2, 3, 4), (2, 3, 4)), dtype=int64>

   >>> M.compute()
   array([[0, 0, 0, 0, 0, 0, 0, 0, 0],
          [0, 1, 0, 0, 0, 0, 0, 0, 0],
          [0, 0, 2, 0, 0, 0, 0, 0, 0],
          [0, 0, 0, 3, 0, 0, 0, 0, 0],
          [0, 0, 0, 0, 4, 0, 0, 0, 0],
          [0, 0, 0, 0, 0, 5, 0, 0, 0],
          [0, 0, 0, 0, 0, 0, 6, 0, 0],
          [0, 0, 0, 0, 0, 0, 0, 7, 0],
          [0, 0, 0, 0, 0, 0, 0, 0, 8]])
