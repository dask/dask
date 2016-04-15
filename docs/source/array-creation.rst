Create Dask Arrays
==================

We store and manipulate large arrays in a wide variety of ways.  There are some
standards like HDF5 and NetCDF but just as often people use custom storage
solutions.  This page talks about how to build dask graphs to interact with
your array.

In principle we need functions that return NumPy arrays.  These functions and
their arrangement can be as simple or as complex as the situation dictates.


Simple case - Format Supports NumPy Slicing
-------------------------------------------

Many formats have Python projects that expose storage using NumPy slicing syntax.
For example the HDF5 file format has the ``h5py`` project, which provides a
``Dataset`` object into which we can slice in NumPy fashion

.. code-block:: Python

   >>> import h5py
   >>> f = h5py.File('myfile.hdf5') # HDF5 file
   >>> d = f['/data/path']          # Pointer on on-disk array
   >>> d.shape                      # d can be very large
   (1000000, 1000000)

   >>> x = d[:5, :5]                # We slice to get numpy arrays

It is common for Python wrappers of on-disk array formats to present a NumPy
slicing syntax.  The full dataset looks like a NumPy array with ``.shape`` and
``.dtype`` attributes even though the data hasn't yet been loaded in and still
lives on disk.  Slicing in to this array-like object fetches the appropriate
data from disk and returns that region as an in-memory NumPy array.

For this common case ``dask.array`` presents the convenience function
``da.from_array``

.. code-block:: Python

   >>> import dask.array as da
   >>> x = da.from_array(d, chunks=(1000, 1000))


Concatenation and Stacking
--------------------------

Often we store data in several different locations and want to stitch them
together.  For this case please see docs on
:doc:`concatenation and stacking <stack>`.


Complex case
------------

If your format does not provide a convenient slicing solution you will need to
dive down one layer to interact with dask dictionaries.  Your goal is to create
a dictionary with tasks that create NumPy arrays, see docs on
:doc:`array design <array-design>` before continuing with this subsection.

To construct a dask array manually you need a dict with tasks that form numpy
arrays

.. code-block:: Python

   dsk = {('x', 0): (f, ...),
          ('x', 1), (f, ...),
          ('x', 2): (f, ...)}

And a chunks tuple that defines the shapes of your blocks along each
dimension

.. code-block:: Python

   chunks = [(1000, 1000, 1000)]

For the tasks ``(f, ...)`` your choice of function ``f`` and arguments ``...``
is up to you.  You have the full freedom of the Python language here as long as
your function, when run with those arguments, produces the appropriate NumPy
array.


Chunks
------

We always specify a ``chunks`` argument to tell dask.array how to break up the
underlying array into chunks.  This strongly impacts performance.  We can
specify ``chunks`` in one of three ways

*  a blocksize like ``1000``
*  a blockshape like ``(1000, 1000)``
*  explicit sizes of all blocks along all dimensions,
   like ``((1000, 1000, 500), (400, 400))``

Your chunks input will be normalized and stored in the third and most explicit
form.

A good choice of ``chunks`` follows the following rules:

1.  A chunk should be small enough to fit comfortably in memory.  We'll
    have many chunks in memory at once.
2.  A chunk must be large enough so that computations on that chunk take
    significantly longer than the 1ms overhead per task that dask scheduling
    incurs.  A task should take longer than 100ms.
3.  Chunks should align with the computation that you want to do.  For example
    if you plan to frequently slice along a particular dimension then it's more
    efficient if your chunks are aligned so that you have to touch fewer
    chunks.  If you want to add two arrays then its convenient if those arrays
    have matching chunks patterns.


Slicing
-------

Dask.array supports most of the NumPy slicing syntax.  In particular it
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
~~~~~~~~~~

The normal dask schedulers are smart enough to compute only those blocks that
are necessary to achieve the desired slicing.  So large operations may be cheap
if only a small output is desired.

In the example below we create a trillion element dask array in million element
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


Stack and Concatenate
---------------------

Often we have many arrays stored on disk that we want to stack together and
think of as one large array.  This is common with geospatial data in which we
might have many HDF5/NetCDF files on disk, one for every day, but we want to do
operations that span multiple days.

To solve this problem we use the functions ``da.stack`` and ``da.concatenate``.

Stack
~~~~~

We stack many existing dask Arrays into a new array, creating a new dimension
as we go.

.. code-block:: python

   >>> import dask.array as da
   >>> data = [from_array(np.ones((4, 4)), chunks=(2, 2))
   ...          for i in range(3)]  # A small stack of dask arrays

   >>> x = da.stack(data, axis=0)
   >>> x.shape
   (3, 4, 4)

   >>> da.stack(data, axis=1).shape
   (4, 3, 4)

   >>> da.stack(data, axis=-1).shape
   (4, 4, 3)

This creates a new dimension with length equal to the number of slices

Concatenate
~~~~~~~~~~~

We concatenate existing arrays into a new array, extending them along an
existing dimension

.. code-block:: python

   >>> import dask.array as da
   >>> import numpy as np

   >>> data = [from_array(np.ones((4, 4)), chunks=(2, 2))
   ...          for i in range(3)]  # small stack of dask arrays

   >>> x = da.concatenate(data, axis=0)
   >>> x.shape
   (12, 4)

   >>> da.concatenate(data, axis=1).shape
   (4, 12)
