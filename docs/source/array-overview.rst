Overview
========

Dask Array implements a subset of the NumPy ndarray interface using blocked
algorithms, cutting up the large array into many small arrays. This lets us
compute on arrays larger than memory using all of our cores. We coordinate these
blocked algorithms using dask graphs.

For a live tutorial in your browser,
visit our Binder_.

.. _Binder: http://mybinder.org/repo/dask/dask-examples/dask-array-basics.ipynb

Scope
-----

The ``dask.array`` library supports the following interface from ``numpy``:

*  Arithmetic and scalar mathematics, ``+, *, exp, log, ...``
*  Reductions along axes, ``sum(), mean(), std(), sum(axis=0), ...``
*  Tensor contractions / dot products / matrix multiply, ``tensordot``
*  Axis reordering / transpose, ``transpose``
*  Slicing, ``x[:100, 500:100:-2]``
*  Fancy indexing along single axes with lists or numpy arrays, ``x[:, [10, 1, 5]]``
*  The array protocol ``__array__``

NOTE: These operations must match the NumPy interface exactly.

Construct
---------

We can construct dask array objects from other array objects that support
numpy-style slicing.  In this example, we wrap a dask array around an HDF5 dataset,
chunking that dataset into blocks of size ``(1000, 1000)``:

.. code-block:: Python

   >>> import h5py
   >>> f = h5py.File('myfile.hdf5')
   >>> dset = f['/data/path']

   >>> import dask.array as da
   >>> x = da.from_array(dset, chunks=(1000, 1000))

Often we have many such datasets.  We can use the ``stack`` or ``concatenate``
functions to bind many dask arrays into one:

.. code-block:: Python

   >>> dsets = [h5py.File(fn)['/data'] for fn in sorted(glob('myfiles.*.hdf5')]
   >>> arrays = [da.from_array(dset, chunks=(1000, 1000))
                   for dset in dsets]

   >>> x = da.stack(arrays, axis=0)  # Stack along a new first axis

Interact
--------

Dask copies the NumPy API for an important subset of operations, including
arithmetic operators, ufuncs, slicing, dot products, and reductions:

.. code-block:: Python

   >>> y = log(x + 1)[:5].sum(axis=1)

Store
-----

In Memory
~~~~~~~~~

If you have a small amount of data, you can call ``np.array`` on your dask array to turn it
in to a normal NumPy array:

.. code-block:: Python

   >>> x = da.arange(6, chunks=3)
   >>> y = x**2
   >>> np.array(y)
   array([0, 1, 4, 9, 16, 25])


HDF5
~~~~

Use the ``to_hdf5`` function to store data into HDF5 using ``h5py``:

.. code-block:: Python

   >>> da.to_hdf5('myfile.hdf5', '/y', y)  # doctest: +SKIP

Store several arrays in one computation with the function
``da.to_hdf5`` by passing in a dict:

.. code-block:: Python

   >>> da.to_hdf5('myfile.hdf5', {'/x': x, '/y': y})  # doctest: +SKIP

Other On-Disk Storage
~~~~~~~~~~~~~~~~~~~~~

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

This API has become a standard in Scientific Python.  Dask works with any
object that supports this operation and the equivalent assignment syntax:

.. code-block:: Python

   >>> dataset[1000:2000, :2000] = x  # Store numpy array in on-disk object


Limitations
-----------

Dask.array does not implement the entire numpy interface.  Users expecting this
will be disappointed.  Notably, dask.array has the following limitations:

1.  Dask does not implement all of ``np.linalg``.  This has been done by a
    number of excellent BLAS/LAPACK implementations, and is the focus of
    numerous ongoing academic research projects.
2.  Dask.array does not support any operation where the resulting shape
    depends on the values of the array.  In order to form the dask graph we
    must be able to infer the shape of the array before actually executing the
    operation.  This precludes operations like indexing one dask array with
    another or operations like ``np.where``.
3.  Dask.array does not attempt operations like ``sort`` which are notoriously
    difficult to do in parallel, and are of somewhat diminished value on very
    large data (you rarely actually need a full sort).
    Often we include parallel-friendly alternatives like ``topk``.
4.  Dask development is driven by immediate need, and so many lesser used
    functions have not been implemented. Community contributions are encouraged.

