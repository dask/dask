Arrays
======

We compute on very large arrays by breaking them up into many small arrays.
We implement operations on the large array as many small operations on the
small arrays.  We coordinate these operations through ``dask`` graphs.

Scope
-----

The ``dask.array`` library supports the following interface from ``numpy``.

*  Arithmetic and scalar mathematics, ``+, *, exp, log, ...``
*  Reductions along axes, ``sum(), mean(), std(), sum(axis=0), ...``
*  Tensor contractions / dot products / matrix multiply, ``tensordot``
*  Axis reordering / transpose, ``transpose``
*  Slicing, ``x[:100, 500:100:-2]``
*  Fancy indexing along single axes with lists or numpy arrays, ``x[:, [10, 1, 5]]``
*  The array protocol ``__array__``

These operations should match the NumPy interface precisely.


Construct
---------

We can construct dask array objects from other array objects that support
numpy-style slicing.  Here we wrap a dask array around an HDF5 dataset,
chunking that dataset into blocks of size ``(1000, 1000)``.

.. code-block:: Python

   >>> import h5py
   >>> f = h5py.File('myfile.hdf5')
   >>> dset = f['/data/path']

   >>> import dask.array as da
   >>> x = da.from_array(dset, blockshape=(1000, 1000))

Often we have many such datasets.  We can use the ``stack`` or ``concatenate``
functions to bind many dask arrays into one.

.. code-block:: Python

   >>> dsets = [h5py.File(fn)['/data'] for fn in sorted(glob('myfiles.*.hdf5')]
   >>> arrays = [da.from_array(dset, blockshape=(1000, 1000))
                   for dset in dsets]

   >>> x = da.stack(arrays, axis=0)  # Stack along a new first axis


Interact
--------

Dask copies the NumPy API for an important subset of operations, including
arithmetic operators, ufuncs, slicing, dot products, and reductions.

.. code-block:: Python

   >>> y = log(x + 1)[:5].sum(axis=1)

Alternatively Dask can use Blaze for this same interface.

.. code-block:: Python

   >>> from blaze import Data, log, compute
   >>> d = Data(x)
   >>> y = log(d + 1)[:5].sum(axis=1)

   >>> result = compute(y)

This provides a smoother interactive experience, dtype tracking, numba
acceleration, etc. but does require an extra step.


Store
-----

If your data is small you can call ``np.array`` on your dask array to turn it
in to a normal NumPy array.

If your data is large then you can store your dask array in any object that
supports numpy-style item assignment like an ``h5py.Dataset``.

.. code-block:: Python

   >>> import h5py  # doctest: +SKIP
   >>> f = h5py.File('myfile.hdf5')

   >>> dset = f.create_dataset('/data', shape=y.shape,
   ...                                  chunks=y.blockshape,
   ...                                  dtype='f8')

   >>> y.store(dset)

Alternatively, if you're comfortable using Blaze and ``into`` then you can jump
directly from the blaze expression to storage, leaving it to handle dataset
creation.

.. code-block:: Python

   >>> from blaze import Data, log, into
   >>> d = Data(x)
   >>> y = log(d + 1)[:5].sum(axis=1)
   >>> into('myfile.hdf5::/data', y)
