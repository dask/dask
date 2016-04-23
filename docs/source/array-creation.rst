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

Many storage formats have Python projects that expose storage using NumPy
slicing syntax.  These include HDF5, NetCDF, BColz, Zarr, GRIB, etc..  For
example the ``HDF5`` file format has the ``h5py`` Python project, which
provides a ``Dataset`` object into which we can slice in NumPy fashion.

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
together.

.. code-block:: Python

   >>> filenames = sorted(glob('2015-*-*.hdf5')
   >>> dsets = [h5py.File(fn)['/data'] for fn in filenames]
   >>> arrays = [da.from_array(dset, chunks=(1000, 1000)) for dset in dsets]
   >>> x = da.concatenate(arrays, axis=0)  # Concatenate arrays along first axis

For more information see :doc:`concatenation and stacking <array-stack>` docs.

Using ``dask.delayed``
----------------------

You can create a plan to arrange many numpy arrays into a grid with normal for
loops using :doc:`dask.delayed<delayed-overview>` and then convert these
into a dask array later.  See :doc:`documentation on using dask.delayed with
collections<delayed-collections>`.

Raw dask graphs
---------------

If your format does not provide a convenient slicing solution you can dive down
one layer to interact with dask dictionaries.  Your goal is to create a
dictionary with tasks that create NumPy arrays, see docs on :doc:`array design
<array-design>` before continuing with this subsection.

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
~~~~~~

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
~~~~~~~

As an example, we might load a grid of pickle files known to contain 1000 by
1000 NumPy arrays.

.. code-block:: python

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


Store Dask Arrays
=================

In Memory
---------

If you have a small amount of data, you can call ``np.array`` on your dask
array to turn in to a normal NumPy array:

.. code-block:: Python

   >>> x = da.arange(6, chunks=3)
   >>> y = x**2
   >>> np.array(y)
   array([0, 1, 4, 9, 16, 25])


HDF5
----

Use the ``to_hdf5`` function to store data into HDF5 using ``h5py``:

.. code-block:: Python

   >>> da.to_hdf5('myfile.hdf5', '/y', y)  # doctest: +SKIP

Store several arrays in one computation with the function
``da.to_hdf5`` by passing in a dict:

.. code-block:: Python

   >>> da.to_hdf5('myfile.hdf5', {'/x': x, '/y': y})  # doctest: +SKIP

Other On-Disk Storage
---------------------

Alternatively, you can store dask arrays in any object that supports numpy-style
slice assignment like ``h5py.Dataset``, or ``bcolz.carray``:

.. code-block:: Python

   >>> import bcolz  # doctest: +SKIP
   >>> out = bcolz.zeros(shape=y.shape, rootdir='myfile.bcolz')  # doctest: +SKIP
   >>> da.store(y, out)  # doctest: +SKIP

You can store several arrays in one computation by passing lists of sources and
destinations:

   >>> da.store([array1, array2], [output1, outpu2])  # doctest: +SKIP


On-Disk Storage
---------------

In the example above we used ``h5py``, but ``dask.array`` works equally well
with ``pytables``, ``bcolz``, or any library that provides an array object from
which we can slice out numpy arrays:

.. code-block:: Python

   >>> x = dataset[1000:2000, :2000]  # pull out numpy array from on-disk object

This API has become a standard in the numeric Python ecosystem.  Dask works
with any object that supports this operation and the equivalent assignment
syntax:

.. code-block:: Python

   >>> dataset[1000:2000, :2000] = x  # Store numpy array in on-disk object
