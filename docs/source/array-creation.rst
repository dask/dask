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


dask.array.random
-----------------

``dask.array`` copies the ``numpy.random`` module for all univariate
distributions.  The interface to each function is identical except for the
addition of a new ``chunks=`` keyword argument.

.. code-block:: python

   import numpy as np
   x = np.random.normal(10, 0.1, size=(10, 10))

   import dask.array as da
   x = da.random.normal(10, 0.1, size=(10, 10), chunks=(5, 5))


Overlapping Blocks with Ghost Cells
-----------------------------------

Some array operations require communication of borders between neighboring
blocks.  Example operations include the following:

*  Convolve a filter across an image
*  Sliding sum/mean/max, ...
*  Search for image motifs like a Gaussian blob that might span the border of a
   block
*  Evaluate a partial derivative
*  Play the game of Life_

Dask.array supports these operations by creating a new dask.array where each
block is slightly expanded by the borders of its neighbors.  This costs an
excess copy and the communication of many small chunks but allows localized
functions to evaluate in an embarrassing manner.  We call this process
*ghosting*.

Ghosting
~~~~~~~~

Consider two neighboring blocks in a dask array.

.. image:: images/unghosted-neighbors.png
   :width: 30%
   :alt: un-ghosted neighbors

We extend each block by trading thin nearby slices between arrays

.. image:: images/ghosted-neighbors.png
   :width: 30%
   :alt: ghosted neighbors

We do this in all directions, including also diagonal interactions with the
ghost function:

.. image:: images/ghosted-blocks.png
   :width: 40%
   :alt: ghosted blocks

.. code-block:: python

   >>> import dask.array as da
   >>> import numpy as np

   >>> x = np.arange(64).reshape((8, 8))
   >>> d = da.from_array(x, chunks=(4, 4))
   >>> d.chunks
   ((4, 4), (4, 4))

   >>> g = da.ghost.ghost(d, depth={0: 2, 1: 1},
   ...                       boundary={0: 100, 1: 'reflect'})
   >>> g.chunks
   ((8, 8), (6, 6))

   >>> np.array(g)
   array([[100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
          [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
          [  0,   0,   1,   2,   3,   4,   3,   4,   5,   6,   7,   7],
          [  8,   8,   9,  10,  11,  12,  11,  12,  13,  14,  15,  15],
          [ 16,  16,  17,  18,  19,  20,  19,  20,  21,  22,  23,  23],
          [ 24,  24,  25,  26,  27,  28,  27,  28,  29,  30,  31,  31],
          [ 32,  32,  33,  34,  35,  36,  35,  36,  37,  38,  39,  39],
          [ 40,  40,  41,  42,  43,  44,  43,  44,  45,  46,  47,  47],
          [ 16,  16,  17,  18,  19,  20,  19,  20,  21,  22,  23,  23],
          [ 24,  24,  25,  26,  27,  28,  27,  28,  29,  30,  31,  31],
          [ 32,  32,  33,  34,  35,  36,  35,  36,  37,  38,  39,  39],
          [ 40,  40,  41,  42,  43,  44,  43,  44,  45,  46,  47,  47],
          [ 48,  48,  49,  50,  51,  52,  51,  52,  53,  54,  55,  55],
          [ 56,  56,  57,  58,  59,  60,  59,  60,  61,  62,  63,  63],
          [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
          [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100]])


Boundaries
~~~~~~~~~~

While ghosting you can specify how to handle the boundaries.  Current policies
include the following:

*  ``periodic`` - wrap borders around to the other side
*  ``reflect`` - reflect each border outwards
*  ``any-constant`` - pad the border with this value

So an example boundary kind argument might look like the following

.. code-block:: python

   {0: 'periodic',
    1: 'reflect',
    2: np.nan}

Alternatively you can use functions like ``da.fromfunction`` and
``da.concatenate`` to pad arbitrarily.


Map a function across blocks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ghosting goes hand-in-hand with mapping a function across blocks.  This
function can now use the additional information copied over from the neighbors
that is not stored locally in each block

.. code-block:: python

   >>> from scipy.ndimage.filters import gaussian_filter
   >>> def func(block):
   ...    return gaussian_filter(block, sigma=1)

   >>> filt = g.map_blocks(func)

While in this case we used a SciPy function above this could have been any
arbitrary function.  This is a good interaction point with Numba_.

If your function does not preserve the shape of the block then you will need to
provide a ``chunks`` keyword argument.  If your block sizes are regular  then
this can be a blockshape, such as ``(1000, 1000)`` or if your blocks are irregular
then this must be a full chunks tuple, for example ``((1000, 700, 1000), (200, 300))``.

.. code-block:: python

   >>> g.map_blocks(myfunc, chunks=(5, 5))

If your function needs to know the location of the block on which it operates
you can give your function a keyword argument ``block_id``

.. code-block:: python

   def func(block, block_id=None):
       ...

This extra keyword argument will be given a tuple that provides the block
location like ``(0, 0)`` for the upper right block or ``(0, 1)`` for the block
just to the right of that block.


Trim Excess
~~~~~~~~~~~

After mapping a blocked function you may want to trim off the borders from each
block by the same amount by which they were expanded.  The function
``trim_internal`` is useful here and takes the same ``depth`` argument
given to ``ghost``.

.. code-block:: python

   >>> x.chunks
   ((10, 10, 10, 10), (10, 10, 10, 10))

   >>> da.ghost.trim_internal(x, {0: 2, 1: 1})
   ((6, 6, 6, 6), (8, 8, 8, 8))


*Note: at the moment ``trim`` cuts indiscriminately from the boundaries as
well.  If you don't specify a boundary kind then this may not be desired.*


Full Workflow
~~~~~~~~~~~~~

And so a pretty typical ghosting workflow includes ``ghost``, ``map_blocks``,
and ``trim_internal``

.. code-block:: python

   >>> x = ...
   >>> g = da.ghost.ghost(x, depth={0: 2, 1: 2},
   ...                       boundary={0: 'periodic', 1: 'periodic'})
   >>> g2 = g.map_blocks(myfunc)
   >>> result = da.ghost.trim_internal(g2, {0: 2, 1: 2})


.. _Life: http://en.wikipedia.org/wiki/Conway%27s_Game_of_Life
.. _Numba: http://numba.pydata.org/
