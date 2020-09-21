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

But generally, slicing works well.
