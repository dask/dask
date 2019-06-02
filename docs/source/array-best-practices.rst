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

If you are just looking for speedups rather than scalability then you may want
to consider a project like `Numba <https://numba.pydata.org>`_


Select a good chunk size
------------------------

A common performance problem among Dask Array users is that they have chosen a
chunk size that is either too small (leading to lots of overhead) or poorly
aligned with their data (leading to inefficient reading).

While optimal sizes and shapes are highly problem specific, it is rare to see
chunk sizes below 100 MB in size.  If you are dealing with float64 data then
this is around ``(4000, 4000)`` in size for a 2D array or ``(100, 400, 400)``
for a 3D array.

You want to choose a chunk size that is large in order to reduce the number of
chunks that Dask has to think about (which affects overhead) but also small
enough so that many of them can fit in memory at once.  Dask will often have as
many chunks in memory as twice the number of active threads.


Orient your chunks
------------------

When reading data you should align your chunks with your storage format.
Most array storage formats store data in chunks themselves.  If your Dask array
chunks aren't multiples of these chunk shapes then you will have to read the
same data repeatedly, which can be expensive.  Note though that often
storage formats choose chunk sizes that are much smaller than is ideal for
Dask, closer to 1MB than 100MB.  In these cases you should choose a Dask chunk
size that aligns with the storage chunk size and that every Dask chunk
dimension is a multiple of the storage chunk dimension.

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


Avoid Oversubscribing Threads
-----------------------------

By default Dask will run as many concurrent tasks as you have logical cores.
It assumes that each task will consume about one core.  However, many
array-computing libraries are themselves multi-threaded, which can cause
contention and low performance.  In particular the BLAS/LAPACK libraries that
back most of NumPy's linear algebra routines are often multi-threaded, and need
to be told to use only one thread explicitly.  You can do this with the
following environment variables (using bash ``export`` command below, but this
may vary depending on your operating system).

.. code-block:: bash

   export OMP_NUM_THREADS=1
   export MKL_NUM_THREADS=1
   export OPENBLAS_NUM_THREADS=1

You need to run this before you start your Python process for it to take effect.


Consider Xarray
---------------

The `Xarray <http://xarray.pydata.org/en/stable/>`_ package wraps around Dask
Array, and so offers the same scalability, but also adds convenience when
dealing with complex datasets.  In particular Xarray can help with the
following:

1.  Manage multiple arrays together as a consistent dataset
2.  Read from a stack of HDF or NetCDF files at once
3.  Switch between Dask Array and NumPy with a consistent API

Xarray is used in wide range of fields, including physics, astronomy, geoscience,
microscopy, bioinformatics, engineering, finance, and deep learning.
Xarray also has a thriving user community that is good at providing support.


Build your own Operations
-------------------------

Often we want to perform computations for which there is no exact function
in Dask Array.  In these cases we may be able to use some of the more generic
functions to build our own.  These include:

.. currentmodule:: dask.array

.. autosummary::
   blockwise
   map_blocks
   map_overlap
   reduction

These functions may help you to apply a function that you write for NumPy
functions onto larger Dask arrays.
