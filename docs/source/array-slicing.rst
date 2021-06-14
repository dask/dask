Slicing
=======

Dask Array supports most of the NumPy slicing syntax.  In particular, it
supports the following:

*  Slicing by integers and slices: ``x[0, :5]``
*  Slicing by lists/arrays of integers: ``x[[1, 2, 4]]``
*  Slicing by lists/arrays of booleans: ``x[[False, True, True, False, True]]``
*  Slicing one :class:`~dask.array.Array` with an :class:`~dask.array.Array` of bools: ``x[x > 0]``
*  Slicing one :class:`~dask.array.Array` with a zero or one-dimensional :class:`~dask.array.Array`
   of ints: ``a[b.argtopk(5)]``

However, it does not currently support the following:

*  Slicing with lists in multiple axes: ``x[[1, 2, 3], [3, 2, 1]]``

   This is straightforward to add though.  If you have a use case then raise an
   issue. Also, users interested in this should take a look at
   :attr:`~dask.array.Array.vindex`.

*  Slicing one :class:`~dask.array.Array` with a multi-dimensional :class:`~dask.array.Array` of ints

.. _array.slicing.efficiency:

Efficiency
----------

The normal Dask schedulers are smart enough to compute only those blocks that
are necessary to achieve the desired slicing.  Hence, large operations may be cheap
if only a small output is desired.

In the example below, we create a Dask array with a trillion elements with million
element sized blocks.  We then operate on the entire array and finally slice out
only a portion of the output:

.. code-block:: python

   >>> # Trillion element array of ones, in 1000 by 1000 blocks
   >>> x = da.ones((1000000, 1000000), chunks=(1000, 1000))

   >>> da.exp(x)[:1500, :1500]
   ...

This only needs to compute the top-left four blocks to achieve the result.  We
are slightly wasteful on those blocks where we need only partial results.  Moreover,
we are also a bit wasteful in that we still need to manipulate the Dask graph
with a million or so tasks in it.  This can cause an interactive overhead of a
second or two.

Slicing with concrete indexers (a list of integers, say) has a couple of possible
failure modes that are worth mentioning. First, when you're indexing a chunked
axis, Dask will typically "match" the chunking on the output.

.. code-block:: python

   # Array of ones, chunked along axis 0
   >>> a = da.ones((4, 10000, 10000), chunks=(1, -1, -1))

If we slice that with a *sorted* sequence of integers, Dask will return one chunk
per input chunk (notice the output `chunksize` is 1, since the indices ``0``
and ``1`` are in separate chunks in the input).

   >>> a[[0, 1], :, :]          #doctest: +SKIP
   dask.array<getitem, shape=(2, 10000, 10000), dtype=float64, chunksize=(1, 10000, 10000), chunktype=numpy.ndarray>

But what about repeated indices? Dask continues to return one chunk per input chunk,
but if you have many repetitions from the same input chunk, your output chunk could
be much larger.

.. code-block:: python

   >>> a[[0] * 15, :, :]
   PerformanceWarning: Slicing is producing a large chunk. To accept the large
   chunk and silence this warning, set the option
       >>> with dask.config.set({'array.slicing.split_large_chunks': False}):
       ...     array[indexer]

   To avoid creating the large chunks, set the option
       >>> with dask.config.set({'array.slicing.split_large_chunks': True}):
       ...     array[indexer]
   dask.array<getitem, shape=(15, 10000, 10000), dtype=float64, chunksize=(15, 10000, 10000), chunktype=numpy.ndarray>

Previously we had a chunksize of ``1`` along the first dimension since we selected
just one element from each input chunk. But now we've selected 15 elements
from the first chunk, producing a large output chunk.

Dask warns when indexing like this produces a chunk that's 5x larger
than the ``array.chunk-size`` config option. You have two options to deal with
that warning:

1. Set ``dask.config.set({"array.slicing.split_large_chunks": False})`` to
   allow the large chunk and silence the warning.
2. Set ``dask.config.set({"array.slicing.split_large_chunks": True})`` to
   avoid creating the large chunk in the first place.

The right choice will depend on your downstream operations. See :ref:`array.chunks`
for more on choosing chunk sizes.
