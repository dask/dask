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
*  Tensor contractions / dot products / matrix multiple, ``tensordot``
*  Axis reordering / transpose, ``transpose``
*  Slicing, ``x[:100, 500:100:-2]``
*  Fancy indexing along single axes with lists or numpy arrays, ``x[:, [10, 1,
  5]]``
*  The array protocol ``__array__``

To the best of our knowledge these operations match the NumPy API precisely.


Construction
------------

We often construct dask array objects from another array-like object that
supports numpy-style slicing.  Here we show an example wrapping a dask array
around an HDF5 dataset

.. code-block:: Python

   >>> import h5py
   >>> f = h5py.File('myfile.hdf5')
   >>> dset = f['/data/path']

   >>> import dask.array as da
   >>> a = da.Array.from_arraylike(dset, blockshape=(1000, 1000))

Often we have many such datasets.  We can use the ``stack`` or ``concatenate``
functions to bind many arrays into one.

.. code-block:: Python

   >>> dsets = [h5py.File(fn)['/data'] for fn in sorted(glob('myfiles.*.hdf5')]
   >>> arrays = [da.Array.from_arraylike(dset, blockshape=(1000, 1000))
                   for dset in dsets]
   >>> a = da.stack(arrays, axis=0)  # Stack along a new first axis


Interaction
-----------

Dask relies on Blaze for usability

.. code-block:: Python

   >>> from blaze import Data
   >>> a = Data(a)
   >>> (a + 1)[:5].sum(axis=1)
   [....]


Store results
-------------

If your data is small you can call ``np.array`` on your dask array to turn it
in to a normal NumPy array.

If your data is large then you can store your dask array in any object that
supports numpy-style item assignment like ``h5py.Dataset``.

.. code-block:: Python

   >>> import h5py  # doctest: +SKIP
   >>> f = h5py.File('myfile.hdf5')

   >>> dset = f.create_dataset('/data', shape=x.shape,
   ...                                  chunks=x.blockshape,
   ...                                  dtype='f8')

   >>> x.store(dset)
