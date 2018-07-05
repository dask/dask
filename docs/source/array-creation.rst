Create Dask Arrays
==================

You can load or store dask arrays from a variety of common sources like HDF5,
and NetCDF, `Zarr <http://zarr.readthedocs.io/en/stable/>`_, or any format that
supports Numpy-style slicing.

.. currentmodule:: dask.array

.. autosummary::
   from_array
   from_delayed
   from_npy_stack
   from_zarr
   stack
   concatenate

NumPy Slicing
-------------

.. autosummary::
   from_array

Many storage formats have Python projects that expose storage using NumPy
slicing syntax.  These include HDF5, NetCDF, BColz, Zarr, GRIB, etc..  For
example we can load a Dask array from an HDF5 file using `h5py <http://www.h5py.org/>`_:

.. code-block:: Python

   >>> import h5py
   >>> f = h5py.File('myfile.hdf5') # HDF5 file
   >>> d = f['/data/path']          # Pointer on on-disk array
   >>> d.shape                      # d can be very large
   (1000000, 1000000)

   >>> x = d[:5, :5]                # We slice to get numpy arrays

Given an object like ``d`` above that has ``dtype`` and ``shape`` properties
and that supports Numpy style slicing we can construct a lazy Dask array.

.. code-block:: Python

   >>> import dask.array as da
   >>> x = da.from_array(d, chunks=(1000, 1000))

This process is entirely lazy.  Neither creating the h5py object nor wrapping
it with ``da.from_array`` have loaded any data.


Concatenation and Stacking
--------------------------

.. autosummary::
   stack
   concatenate

Often we store data in several different locations and want to stitch them together.

.. code-block:: Python

    dask_arrays = []
    for fn in filenames:
        f = h5py.File(fn)
        d = f['/data']
        array = da.from_array(d, chunks=(1000, 1000))
        dask_arrays.append(array)

    x = da.concatenate(dask_arrays, axis=0)  # concatenate arrays along first axis

For more information see :doc:`concatenation and stacking <array-stack>` docs.


Using ``dask.delayed``
----------------------

.. autosummary::
   from_delayed
   stack
   concatenate

Sometimes Numpy-style data resides in formats that do not support numpy-style
slicing.  We can still construct Dask arrays around this data if we have a
Python function that can generate pieces of the full array if we use
:doc:`dask.delayed <delayed>`.  Dask delayed lets us delay a single function
call that would create a numpy array.  We can then wrap this delayed object
with ``da.from_delayed``, providing a dtype and shape to produce a
single-chunked Dask array.  We can then use ``stack`` or ``concatenate`` from
before to construct a larger lazy array.


As an example, consider loading a stack of images using ``skimage.io.imread``:

.. code-block:: python

    import skimage.io
    import dask.array as da
    import dask

    imread = dask.delayed(skimage.io.imread, pure=True)  # Lazy version of imread

    filenames = sorted(glob.glob('*.jpg'))

    lazy_images = [imread(path) for path in filenames]     # Lazily evaluate imread on each path
    sample = lazy_images[0].compute() # load the first image (assume rest are same shape/dtype)

    arrays = [da.from_delayed(lazy_image,           # Construct a small Dask array
                              dtype=sample.dtype,   # for every lazy value
                              shape=sample.shape)
              for lazy_image in lazy_images]

    stack = da.stack(arrays, axis=0)                # Stack all small Dask arrays into one

See :doc:`documentation on using dask.delayed with collections<delayed-collections>`.


From Dask.dataframe
-------------------

You can create dask arrays from dask dataframes using the ``.values`` attribute
or the ``.to_records()`` method.

.. code-block:: python

   >>> x = df.values
   >>> x = df.to_records()

However these arrays do not have known chunk sizes (dask.dataframe does not
track the number of rows in each partition) and so some operations like slicing
will not operate correctly.

If you have a function that converts a Pandas dataframe into a Numpy array
then calling ``map_partitions`` with that function on a Dask dataframe will
produce a Dask array.

.. code-block:: python

   >>> x = df.map_partitions(np.asarray)


Interactions with NumPy arrays
------------------------------

Dask.array operations will automatically convert NumPy arrays into single-chunk
dask arrays

.. code-block:: python

   >>> x = da.sum(np.ones(5))
   >>> x.compute()
   5

When NumPy and Dask arrays interact the result will be a Dask array.  Automatic
rechunking rules will generally slice the NumPy array into the appropriate Dask
chunk shape

.. code-block:: python

   >>> x = da.ones(10, chunks=(5,))
   >>> y = np.ones(10)
   >>> z = x + y
   >>> z
   dask.array<add, shape=(10,), dtype=float64, chunksize=(5,)>

These interactions work not just for NumPy arrays, but for any object that has
shape and dtype attributes and implements NumPy slicing syntax.


Chunks
------

See :doc:`documentation on Array Chunks <array-chunks>` for more information.


Store Dask Arrays
=================

.. autosummary::
   store
   to_hdf5
   to_npy_stack
   to_zarr
   compute

In Memory
---------

.. autosummary::
   compute

If you have a small amount of data, you can call ``np.array`` or ``.compute()``
on your Dask array to turn in to a normal NumPy array:

.. code-block:: Python

   >>> x = da.arange(6, chunks=3)
   >>> y = x**2
   >>> np.array(y)
   array([0, 1, 4, 9, 16, 25])

   >>> y.compute()
   array([0, 1, 4, 9, 16, 25])


Numpy style slicing
-------------------

.. autosummary::
   store

You can store dask arrays in any object that supports numpy-style slice
assignment like ``h5py.Dataset``:

.. code-block:: Python

   >>> import h5py
   >>> f = h5py.File('myfile.hdf5')
   >>> d = f.require_dataset('/data', shape=x.shape, dtype=x.dtype)
   >>> da.store(x, d)

You can store several arrays in one computation by passing lists of sources and
destinations:

.. code-block:: Python

   >>> da.store([array1, array2], [output1, output2])  # doctest: +SKIP

HDF5
----

.. autosummary::
   to_hdf5

HDF5 is sufficiently common that there is a special function, ``to_hdf5`` to
store data into HDF5 files using ``h5py``:

.. code-block:: Python

   >>> da.to_hdf5('myfile.hdf5', '/y', y)  # doctest: +SKIP

Store several arrays in one computation with the function
``da.to_hdf5`` by passing in a dict:

.. code-block:: Python

   >>> da.to_hdf5('myfile.hdf5', {'/x': x, '/y': y})  # doctest: +SKIP


Zarr
----

The `Zarr <https://zarr.readthedocs.io>`_ format is a chunk-wise binary array
storage file format, with a good selection of encoding and compression options.
Due to each chunk being stored in a separate file, it is ideal for parallel
access in both reading and writing (for the latter, if the dask array
chunks are alligned with the target). Furthermore, storage in
:doc:`remote data services <remote-data-services>` such as S3 and GCS is
supported.

For example, to save data to a local zarr dataset:

.. code-block:: Python

   >>> arr.to_zarr('output.zarr')

or to save to a particular bucket on S3:

.. code-block:: Python

   >>> arr.to_zarr('s3://mybucket/output.zarr', storage_option={'key': 'mykey',
                   'secret': 'mysecret'})

or your own custom zarr Array:

.. code-block:: Python

   >>> z = zarr.create((10,), dtype=float, store=zarr.ZipStore("output.zarr"))
   >>> arr.to_zarr(z)

To retrieve those data, you would do ``da.read_zarr`` with exactly the same arguments. The
chunking of the resultant dask.Array is defined by how the files were saved, unless
otherwise specified.


Plugins
=======

We can run arbitrary user-defined functions on dask.arrays whenever they are
constructed. This allows us to build a variety of custom behaviors that improve
debugging, user warning, etc..  You can register a list of functions to run on
all dask.arrays to the global ``array_plugins=`` value:

.. code-block:: python

   >>> def f(x):
   ...     print(x.nbytes)

   >>> with dask.config.set(array_plugins=[f]):
   ...     x = da.ones((10, 1), chunks=(5, 1))
   ...     y = x.dot(x.T)
   80
   80
   800
   800

If the plugin function returns None then the input Dask.array will be returned
without change.  If the plugin function returns something else then that value
will be the result of the constructor.

Examples
--------

Automatically compute
~~~~~~~~~~~~~~~~~~~~~

We may wish to turn some Dask.array code into normal NumPy code.  This is
useful for example to track down errors immediately that would otherwise be
hidden by Dask's lazy semantics.

.. code-block:: python

   >>> with dask.config.set(array_plugins=[lambda x: x.compute()]):
   ...     x = da.arange(5, chunks=2)

   >>> x  # this was automatically converted into a numpy array
   array([0, 1, 2, 3, 4])

Warn on large chunks
~~~~~~~~~~~~~~~~~~~~

We may wish to warn users if they are creating chunks that are too large

.. code-block:: python

   def warn_on_large_chunks(x):
       shapes = list(itertools.product(*x.chunks))
       nbytes = [x.dtype.itemsize * np.prod(shape) for shape in shapes]
       if any(nb > 1e9 for nb in nbytes):
           warnings.warn("Array contains very large chunks")

   with dask.config.set(array_plugins=[warn_on_large_chunks]):
       ...

Combine
~~~~~~~

You can also combine these plugins into a list.  They will run one after the
other, chaining results through them.

.. code-block:: python

   with dask.config.set(array_plugins=[warn_on_large_chunks, lambda x: x.compute()]):
       ...
