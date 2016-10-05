Slicing
=======

Dask array supports most of the NumPy slicing syntax.  In particular it
supports the following:

*  Slicing by integers and slices ``x[0, :5]``
*  Slicing by lists/arrays of integers  ``x[[1, 2, 4]]``
*  Slicing by lists/arrays of booleans ``x[[False, True, True, False, True]]``

It does not currently support the following:

*  Slicing one ``dask.array`` with another ``x[x > 0]``
*  Slicing with lists in multiple axes  ``x[[1, 2, 3], [3, 2, 1]]``

Both of these are straightforward to add though.  If you have a use case then
raise an issue.

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
