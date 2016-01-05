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


Example
```````

As an example we might load a grid of pickle files known to contain 1000
by 1000 NumPy arrays

.. code-block:: Python

   def load(fn):
       with open(fn) as f:
           result = pickle.load(f)
        return result

   dsk = {('x', 0, 0): (load, 'block-0-0.pkl'),
          ('x', 0, 1): (load, 'block-0-1.pkl'),
          ('x', 0, 2): (load, 'block-0-2.pkl'),
          ('x', 1, 0): (load, 'block-1-0.pkl'),
          ('x', 1, 1): (load, 'block-1-1.pkl'),
          ('x', 1, 2): (load, 'block-1-2.pkl')}

    chunks = ((1000, 1000), (1000, 1000, 1000))

    x = da.Array(dsk, 'x', chunks)
