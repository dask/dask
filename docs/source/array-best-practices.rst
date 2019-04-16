Best Practices
==============

It is easy to get started with Dask arrays, but using them *well* does require
some experience.  This page contains suggestions for best practices, and
includes solutions to common problems.

Use NumPy
---------

If your data fits comfortably in RAM and you are not performance bound, then
using NumPy might be the right choice.  Dask adds another layer of complexity
which may get in the way.


Select a good chunk size
------------------------

A common performance problem among Dask Array users is that they have chosen a
chunk size that is either too small (leading to lots of overhead) or poorly
aligned with their data (leading to inefficient reading).

While optimal sizes and shapes are highly problem specific, it is rare to see
chunk sizes below 100 MB in size.  If you are dealing with float64 data then
this is around ``(4000, 4000)`` in size.


Orient your chunks
------------------

When reading data you should align your chunks with your storage format.
Most array storage formats store data in chunks themselves.  If your Dask array
chunks aren't multiples of these chunk shapes then you will have to read the
same data repeatedly, which can be very expensive.  Note though that often
storage formats choose chunk sizes that are much smaller than is ideal for
Dask.  In these cases you should choose a Dask chunk size that aligns with the
storage chunk size and that every Dask chunk dimension is a multiple of the
storage chunk dimension.

So for example if we have an HDF file that has chunks of size ``(128, 64)``, we
might choose a chunk shape of ``(1280, 6400)``.

.. code-block:: python

   >>> import h5py
   >>> storage = h5py.File('myfile.hdf5')['x']
   >>> storage.chunks
   (128, 64)

   >>> import dask.array as da
   >>> x = da.from_array(storage, chunks=(1280, 6400))

Note that if you provide ``chunks='auto'`` then Dask Array will look for a
``.chunks`` attribute and use that to provide a good chunking.


Avoid Oversubscription
----------------------

By default Dask will run as many concurrent tasks as you have logical cores.
It assumes that each task will consume about one core.  However, many
array-computing libraries are themselves multi-threaded, which can cause
contention and low performance.  In particular the BLAS/LAPACK libraries that
back most of NumPy's linear algebra routines are often multi-threaded, and need
to be told to use only one thread explicitly.  You can do this with the
following environment variables (using bash ``export`` command below, but this
may vary depending on your operating system).

.. code-block::

   export OMP_NUM_THREADS=1
   export MKL_NUM_THREADS=1

You need to run this before you start your Python process for it to take effect.


Consider Xarray
---------------

The `Xarray <http://xarray.pydata.org/en/stable/>`_ package wraps around Dask Array, but adds a variety of convenience
functions.

1.  Manage multiple arrays together as a consistent dataset
2.  Read from a stack of HDF or NetCDF files at once
3.  Consistent API regardless of whether or not you're backed by NumPy or Pandas

Xarray was originally designed for atmospheric and oceanographic science, but
is general purpose enough to be of use to other disciplines.  It also has a
thriving user community that is good at supporting each other.
